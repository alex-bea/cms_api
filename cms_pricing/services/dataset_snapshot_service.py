"""Service for selecting and registering dataset snapshots.

Part of Quick Win #1: Dataset Snapshots Table. Extended to support snapshot
metadata reuse for MPFS implementation (snapshot reuse + targeted fetcher
pattern).
"""

from dataclasses import dataclass
import json
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import structlog

from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.utils.snapshot_fallback import (
    collect_search_roots,
    discover_latest_release,
    filename_prefix,
    replace_release_in_path,
    resolve_repo_path,
)
from cms_pricing.utils.snapshot_fallback import (
    collect_search_roots,
    discover_latest_release,
    filename_prefix,
    replace_release_in_path,
)

logger = structlog.get_logger()

SNAPSHOT_ALIAS_MAP: Dict[str, List[str]] = {
    "rvu_items": ["rvu_items", "pprrvu"],
    "gpci_indices": ["gpci_indices", "gpci"],
    "anescf": ["anescf"],
    "localitycounty": ["localitycounty", "locality"],
    "oppscap": ["oppscap"],
    "mpfs_payment_curated": ["mpfs_payment_curated", "mpfs_payment"],
    "mpfs_rvu": ["mpfs_rvu", "pprrvu"],
    "mpfs_gpci": ["mpfs_gpci", "gpci"],
}


@dataclass
class SnapshotMetadata:
    """Lightweight metadata record returned by DatasetSnapshotService."""

    dataset_id: str
    release_id: str
    digest: str
    effective_from: date
    effective_to: Optional[date]
    manifest_url: Optional[str] = None
    path: Optional[str] = None


class SnapshotRegistrationError(Exception):
    """Raised when snapshot registration fails."""


class SnapshotAlreadyExistsError(SnapshotRegistrationError):
    """Raised when attempting to register a duplicate snapshot without override."""

    def __init__(self, dataset_id: str, release_id: str):
        super().__init__(f"Snapshot already exists for {dataset_id}:{release_id}")
        self.dataset_id = dataset_id
        self.release_id = release_id


class DatasetSnapshotService:
    """Service for selecting dataset snapshots by valuation date"""

    def __init__(self, db: Optional[Session] = None):
        if db is not None:
            self.db = db
            self._managed_session = False
        else:
            from cms_pricing.database import SessionLocal  # lazy import to avoid circular import

            self.db = SessionLocal()
            self._managed_session = True
        self._search_roots = collect_search_roots()

    # ------------------------------------------------------------------
    # Snapshot selection
    # ------------------------------------------------------------------
    
    def select_snapshot(
        self,
        dataset_id: str,
        valuation_date: Optional[date] = None,
        release_id: Optional[str] = None
    ) -> Optional[DatasetSnapshot]:
        """
        Select active snapshot for dataset at valuation date.
        
        Args:
            dataset_id: Dataset identifier (e.g., 'MPFS', 'OPPS')
            valuation_date: Date for which to select snapshot (defaults to today)
            release_id: Optional specific release_id to select
            
        Returns:
            DatasetSnapshot if found, None otherwise
        """
        if valuation_date is None:
            valuation_date = date.today()
        
        query = self.db.query(DatasetSnapshot).filter(
            DatasetSnapshot.dataset_id == dataset_id
        )
        
        # If specific release_id requested, use it directly
        if release_id:
            query = query.filter(DatasetSnapshot.release_id == release_id)
        else:
            # Otherwise, select active snapshot at valuation date
            query = query.filter(
                and_(
                    DatasetSnapshot.effective_from <= valuation_date,
                    or_(
                        DatasetSnapshot.effective_to.is_(None),
                        DatasetSnapshot.effective_to >= valuation_date
                    )
                )
            )
        
        # Order by effective_from desc, then created_at desc to get latest
        snapshot = query.order_by(
            DatasetSnapshot.effective_from.desc(),
            DatasetSnapshot.created_at.desc()
        ).first()
        
        if snapshot:
            logger.debug(
                "Selected snapshot",
                dataset_id=dataset_id,
                release_id=snapshot.release_id,
                effective_from=snapshot.effective_from,
                valuation_date=valuation_date
        )
        
        return snapshot

    def get_latest_snapshot(
        self,
        dataset_id: str,
        valuation_date: Optional[date] = None,
        release_id: Optional[str] = None
    ) -> Optional[SnapshotMetadata]:
        """
        Return latest snapshot metadata for dataset.

        Args:
            dataset_id: Dataset identifier (e.g., 'rvu_items')
            valuation_date: Optional valuation date (defaults to today)
            release_id: Optional specific release identifier

        Returns:
            SnapshotMetadata or None if no snapshot available.
        """
        snapshot = self.select_snapshot(
            dataset_id=dataset_id,
            valuation_date=valuation_date,
            release_id=release_id
        )

        if not snapshot:
            return None

        resolved_path = self._resolve_curated_path(snapshot)
        return SnapshotMetadata(
            dataset_id=snapshot.dataset_id,
            release_id=snapshot.release_id,
            digest=snapshot.digest,
            effective_from=snapshot.effective_from,
            effective_to=snapshot.effective_to,
            manifest_url=snapshot.manifest_url,
            path=resolved_path
        )
    
    def get_snapshot_by_release(
        self,
        dataset_id: str,
        release_id: str
    ) -> Optional[DatasetSnapshot]:
        """Get snapshot by exact dataset_id and release_id (uses composite PK)"""
        return self.db.get(DatasetSnapshot, (dataset_id, release_id))
    
    def list_snapshots(
        self,
        dataset_id: Optional[str] = None,
        limit: int = 50
    ) -> List[DatasetSnapshot]:
        """List snapshots, optionally filtered by dataset_id"""
        query = self.db.query(DatasetSnapshot)
        
        if dataset_id:
            query = query.filter(DatasetSnapshot.dataset_id == dataset_id)
        
        return query.order_by(
            DatasetSnapshot.dataset_id,
            DatasetSnapshot.effective_from.desc()
        ).limit(limit).all()
    
    def register_snapshot(
        self,
        dataset_id: str,
        release_id: str,
        digest: str,
        effective_from: date,
        effective_to: Optional[date] = None,
        manifest_url: Optional[str] = None,
        curated_path: Optional[str] = None,
        *,
        allow_overwrite: bool = False,
        autocommit: bool = True,
    ) -> DatasetSnapshot:
        """
        Register a new dataset snapshot.
        
        Args:
            dataset_id: Dataset identifier
            release_id: Release identifier (must match fee schedule tables)
            digest: SHA256 digest of dataset content
            effective_from: Date when snapshot becomes effective
            effective_to: Optional expiration date
            manifest_url: Optional URL to dataset manifest (used for provenance)
            curated_path: Optional local path to curated dataset output. When
                provided it will be stored in manifest_url if manifest_url is
                not supplied.
            
        Returns:
            Created DatasetSnapshot
        """
        stored_manifest = manifest_url or curated_path

        # Check if already exists (efficient PK lookup)
        existing = self.db.get(DatasetSnapshot, (dataset_id, release_id))
        if existing:
            if not allow_overwrite:
                raise SnapshotAlreadyExistsError(dataset_id, release_id)
            existing.digest = digest
            existing.effective_from = effective_from
            existing.effective_to = effective_to
            existing.manifest_url = stored_manifest
            if autocommit:
                self.db.commit()
                self.db.refresh(existing)
            else:
                self.db.flush()
            return existing

        snapshot = DatasetSnapshot(
            dataset_id=dataset_id,
            release_id=release_id,
            digest=digest,
            effective_from=effective_from,
            effective_to=effective_to,
            manifest_url=stored_manifest
        )

        self.db.add(snapshot)
        if autocommit:
            self.db.commit()
            self.db.refresh(snapshot)
        else:
            self.db.flush()
        
        logger.info(
            "Registered dataset snapshot",
            dataset_id=dataset_id,
            release_id=release_id,
            effective_from=effective_from
        )
        
        return snapshot

    def close(self) -> None:
        """Close managed DB session if this service created its own."""
        if getattr(self, "_managed_session", False) and self.db:
            try:
                self.db.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_curated_path(self, snapshot: DatasetSnapshot) -> Optional[str]:
        """
        Resolve best-guess curated dataset path for snapshot.

        Priority:
            1. If manifest_url points to a local manifest.json, attempt to
               resolve the specific parquet path from the manifest contents.
            2. If manifest_url looks like a local path (non-JSON), return it.
            3. Default mapping based on dataset_id (directory); the caller may
               select the exact parquet file from the directory.

        Returns:
            String path or None if unable to resolve.
        """
        def candidate_names() -> List[str]:
            aliases = SNAPSHOT_ALIAS_MAP.get(snapshot.dataset_id, [snapshot.dataset_id])
            seen: List[str] = []
            for entry in aliases:
                if entry not in seen:
                    seen.append(entry)
            return seen

        def normalize_candidate(
            path_value: Optional[str],
            base_dir: Optional[Path] = None,
            allow_missing: bool = False,
        ) -> Optional[Path]:
            if not isinstance(path_value, str) or not path_value.strip():
                return None
            candidate_path = Path(path_value.strip())
            if (
                base_dir
                and not candidate_path.is_absolute()
                and not self._is_repo_relative_root(candidate_path)
            ):
                candidate_path = base_dir / candidate_path
            candidate_path = self._normalize_ingestion_path(candidate_path)
            candidate_path = self._dedupe_repeated_prefix(candidate_path)
            if self._repo_path_exists(candidate_path, snapshot.dataset_id):
                return candidate_path
            if allow_missing:
                return candidate_path
            return None

        def resolve_manifest_entry(value: Optional[str], base: Optional[Path]) -> Optional[str]:
            resolved = normalize_candidate(value, base)
            if resolved:
                return str(resolved)
            fallback = normalize_candidate(value, base, allow_missing=True)
            if fallback:
                logger.debug(
                    "manifest_candidate_missing",
                    dataset_id=snapshot.dataset_id,
                    release_id=snapshot.release_id,
                    candidate=str(fallback),
                )
                return str(fallback)
            return None

        manifest_url = snapshot.manifest_url or ""
        if manifest_url:
            mpath = Path(manifest_url)
            mpath = self._normalize_ingestion_path(mpath)
            mpath = self._dedupe_repeated_prefix(mpath)
            fs_manifest = self._filesystem_path(mpath, snapshot.dataset_id)
            if fs_manifest:
                if fs_manifest.is_file():
                    if mpath.suffix.lower() == ".json":
                        try:
                            data = json.loads(fs_manifest.read_text())
                            base_dir = fs_manifest.parent

                            # Try common shapes first
                            ds = data.get("datasets")
                            keys = candidate_names()
                            if isinstance(ds, dict):
                                for key in keys:
                                    entry = ds.get(key)
                                    if isinstance(entry, dict):
                                        parquet_path = entry.get("parquet_path") or entry.get("path")
                                        resolved = resolve_manifest_entry(parquet_path, base_dir)
                                        if resolved:
                                            return resolved
                                for key in keys:
                                    entry = ds.get(key)
                                    if isinstance(entry, str):
                                        resolved = resolve_manifest_entry(entry, base_dir)
                                        if resolved:
                                            return resolved
                            elif isinstance(ds, list):
                                for entry in ds:
                                    if isinstance(entry, dict):
                                        entry_name = entry.get("name") or entry.get("dataset_id") or entry.get("dataset")
                                        if entry_name and entry_name not in keys:
                                            continue
                                        parquet_path = entry.get("parquet_path") or entry.get("path")
                                        resolved = resolve_manifest_entry(parquet_path, base_dir)
                                        if resolved:
                                            return resolved
                                    elif isinstance(entry, str):
                                        resolved = resolve_manifest_entry(entry, base_dir)
                                        if resolved:
                                            return resolved

                            ct = data.get("curated_tables")
                            if isinstance(ct, dict):
                                for alias in candidate_names():
                                    value = ct.get(alias)
                                    if isinstance(value, str):
                                        resolved = resolve_manifest_entry(value, base_dir)
                                        if resolved:
                                            return resolved
                        except Exception as err:
                            logger.debug("manifest_resolution_failed", error=str(err), manifest=str(mpath))
                    else:
                        return self._stringify_path(mpath)
                else:
                    return self._stringify_path(mpath)
            else:
                if mpath.suffix.lower() != ".json":
                    logger.debug(
                        "manifest_path_missing",
                        dataset_id=snapshot.dataset_id,
                        release_id=snapshot.release_id,
                        candidate=str(mpath),
                    )
                    fallback = self._fallback_to_latest_drop(snapshot, mpath)
                    if fallback:
                        return fallback
                    return self._stringify_path(mpath)

        dataset_root = self._default_curated_root(snapshot.dataset_id)
        if not dataset_root:
            return None

        release_dir = dataset_root / snapshot.release_id
        release_dir = self._normalize_ingestion_path(release_dir)
        fallback = self._fallback_to_latest_drop(snapshot, release_dir)
        if fallback:
            return fallback
        return str(release_dir)

    @staticmethod
    def _normalize_ingestion_path(path: Path) -> Path:
        """Rewrite known absolute ingestion prefixes to repository-relative paths."""
        if not isinstance(path, Path):
            path = Path(path)

        prefix_mappings = (
            (Path("/var/data/ingestion"), Path("data/ingestion")),
            (Path("/app/data/ingestion"), Path("data/ingestion")),
            (Path("/var/data/curated"), Path("data/curated")),
            (Path("/app/data/curated"), Path("data/curated")),
        )

        for prefix, replacement in prefix_mappings:
            try:
                relative = path.resolve(strict=False).relative_to(prefix)
                path = replacement / relative
                break
            except ValueError:
                continue

        posix_path = path.as_posix()
        if posix_path.startswith("./"):
            posix_path = posix_path[2:]

        normalized = Path(posix_path)
        normalized = DatasetSnapshotService._dedupe_repeated_prefix(normalized)
        return normalized

    @staticmethod
    def _dedupe_repeated_prefix(path: Path) -> Path:
        """Collapse repeated data/ingestion or data/curated prefixes."""
        if not isinstance(path, Path):
            path = Path(path)

        parts = list(path.parts)
        if not parts:
            return path

        prefixes = (
            ("data", "ingestion"),
            ("data", "curated"),
        )

        for prefix in prefixes:
            prefix_len = len(prefix)
            if tuple(parts[:prefix_len]) != prefix:
                continue

            dataset_segment: Tuple[str, ...] = ()
            if len(parts) > prefix_len:
                dataset_segment = (parts[prefix_len],)

            cleaned: List[str] = list(parts[: prefix_len + len(dataset_segment)])
            idx = len(cleaned)

            while idx < len(parts):
                if tuple(parts[idx : idx + prefix_len]) == prefix:
                    idx += prefix_len
                    if dataset_segment and tuple(parts[idx : idx + len(dataset_segment)]) == dataset_segment:
                        idx += len(dataset_segment)
                    continue
                cleaned.append(parts[idx])
                idx += 1

            return Path(*cleaned)

        return Path(*parts)

    def _stringify_path(self, path: Path) -> str:
        """Normalize a filesystem path and return it as a repo-relative string."""
        normalized = self._normalize_ingestion_path(path)
        return normalized.as_posix()

    @staticmethod
    def _is_repo_relative_root(path: Path) -> bool:
        """Return True when the provided path begins at the repository root."""
        if not isinstance(path, Path):
            path = Path(path)

        if not path.parts:
            return False
        return path.parts[0] == "data"

    @staticmethod
    def _default_curated_root(dataset_id: str) -> Optional[Path]:
        """Provide default curated directory for known datasets."""
        default_roots = {
            "rvu_items": Path("data/curated/rvu"),
            "gpci_indices": Path("data/curated/rvu"),
            "mpfs_payment_curated": Path("data/curated/mpfs"),
            "mpfs_rvu": Path("data/curated/mpfs"),
            "mpfs_gpci": Path("data/curated/mpfs"),
            "mpfs_cf_vintage": Path("data/curated/mpfs"),
        }

        if dataset_id in default_roots:
            return default_roots[dataset_id]

        # Fallback to generic dataset-id directory
        return Path("data/curated") / dataset_id

    def _filesystem_path(self, path: Path, dataset_id: Optional[str]) -> Optional[Path]:
        """Resolve repo-relative path to actual filesystem location."""
        if path.is_absolute():
            return path if path.exists() else None

        cwd_candidate = Path.cwd() / path
        if cwd_candidate.exists():
            return cwd_candidate

        hint = self._dataset_hint(path, dataset_id)
        return resolve_repo_path(path, self._search_roots, dataset_hint=hint)

    @staticmethod
    def _dataset_hint(path: Path, dataset_id: Optional[str]) -> Optional[str]:
        for candidate in ("cms_rvu", dataset_id):
            if candidate and candidate in path.parts:
                return candidate
        return None

    def _repo_path_exists(self, path: Path, dataset_id: Optional[str]) -> bool:
        return self._filesystem_path(path, dataset_id) is not None

    def _fallback_to_latest_drop(self, snapshot: DatasetSnapshot, missing_path: Path) -> Optional[str]:
        """Use the most recent curated drop when the requested release is absent."""
        prefix = filename_prefix(missing_path, snapshot.dataset_id)
        dataset_hint = None
        for hint in ("cms_rvu", snapshot.dataset_id):
            if hint and hint in missing_path.parts:
                dataset_hint = hint
                break

        latest = discover_latest_release(prefix, self._search_roots, dataset_hint=dataset_hint)
        if not latest:
            return None

        fallback_path = replace_release_in_path(missing_path, latest.release, new_filename_prefix=prefix)
        if not self._filesystem_path(fallback_path, snapshot.dataset_id):
            return None
        fallback_str = self._stringify_path(fallback_path)
        logger.warning(
            "snapshot_fallback_to_latest_drop",
            dataset_id=snapshot.dataset_id,
            release_id=snapshot.release_id,
            requested=str(missing_path),
            fallback=fallback_str,
            latest_release=latest.release,
        )
        return fallback_str
