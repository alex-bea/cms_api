"""Service for selecting and registering dataset snapshots.

Part of Quick Win #1: Dataset Snapshots Table. Extended to support snapshot
metadata reuse for MPFS implementation (snapshot reuse + targeted fetcher
pattern).
"""

from dataclasses import dataclass
import json
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import structlog

from cms_pricing.models.dataset_snapshots import DatasetSnapshot

logger = structlog.get_logger()


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
        curated_path: Optional[str] = None
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
            logger.warning(
                "Snapshot already exists, updating",
                dataset_id=dataset_id,
                release_id=release_id
            )
            existing.digest = digest
            existing.effective_from = effective_from
            existing.effective_to = effective_to
            existing.manifest_url = stored_manifest
            self.db.commit()
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
        self.db.commit()
        self.db.refresh(snapshot)
        
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
        manifest_url = snapshot.manifest_url or ""
        if manifest_url.startswith("data/") or manifest_url.startswith("./data/"):
            # If this is a manifest file, try to resolve a concrete parquet path from it
            if manifest_url.endswith(".json"):
                try:
                    mpath = Path(manifest_url)
                    if mpath.exists():
                        data = json.loads(mpath.read_text())
                        # Try common shapes first
                        # 1) datasets: { <key>: { parquet_path: "..." } }
                        ds = data.get("datasets")
                        if isinstance(ds, dict):
                            # Try by dataset_id, then known aliases
                            candidate_keys = [snapshot.dataset_id]
                            alias_map = {
                                "rvu_items": "pprrvu",
                                "gpci_indices": "gpci",
                                "anescf": "anescf",
                                "localitycounty": "localitycounty",
                                "oppscap": "oppscap",
                            }
                            alias = alias_map.get(snapshot.dataset_id)
                            if alias:
                                candidate_keys.append(alias)
                            for key in candidate_keys:
                                entry = ds.get(key)
                                if isinstance(entry, dict):
                                    parquet_path = entry.get("parquet_path") or entry.get("path")
                                    if isinstance(parquet_path, str) and Path(parquet_path).exists():
                                        return parquet_path
                        # 2) curated_tables: { <alias>: "...parquet" }
                        ct = data.get("curated_tables")
                        if isinstance(ct, dict):
                            # Map dataset_id to alias
                            alias_map = {
                                "rvu_items": "pprrvu",
                                "gpci_indices": "gpci",
                                "anescf": "anescf",
                                "localitycounty": "localitycounty",
                                "oppscap": "oppscap",
                            }
                            alias = alias_map.get(snapshot.dataset_id)
                            if alias and isinstance(ct.get(alias), str):
                                parquet_path = ct[alias]
                                if Path(parquet_path).exists():
                                    return parquet_path
                    # If manifest exists but we couldn't resolve a parquet file, fall through
                except Exception as err:
                    logger.debug("manifest_resolution_failed", error=str(err), manifest=manifest_url)
            else:
                # Return the local path directly (directory or file)
                return manifest_url

        dataset_root = self._default_curated_root(snapshot.dataset_id)
        if not dataset_root:
            return None

        release_dir = dataset_root / snapshot.release_id
        return str(release_dir)

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
