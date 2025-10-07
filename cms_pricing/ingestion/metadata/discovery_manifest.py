"""Shared helpers for discovery manifest creation, persistence, and comparison."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


def _isoformat(value: Any) -> Optional[str]:
    """Convert supported temporal values to ISO strings."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    # Allow strings to pass through untouched.
    if isinstance(value, str):
        return value

    raise TypeError(f"Unsupported datetime value: {value!r}")


def _drop_nulls(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys with None values for cleaner manifests."""

    return {key: value for key, value in data.items() if value is not None}


_DEFAULT_CONTENT_TYPES = {
    "zip": "application/zip",
    "csv": "text/csv",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "txt": "text/plain",
    "pdf": "application/pdf",
}


_REQUIRED_MANIFEST_FIELDS = ("source", "source_url", "discovered_at", "files")
_REQUIRED_FILE_FIELDS = ("url", "filename", "content_type")


@dataclass
class DiscoveryFileEntry:
    """A single file entry within a discovery manifest."""

    url: str
    filename: str
    content_type: str
    size_bytes: Optional[int] = None
    sha256: Optional[str] = None
    last_modified: Optional[str] = None
    year: Optional[int] = None
    quarter: Optional[str] = None
    file_type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(
        cls,
        obj: Union[Dict[str, Any], Any],
        *,
        default_content_type: Optional[str] = None,
    ) -> "DiscoveryFileEntry":
        """Create an entry from a scraper object or dictionary."""

        if isinstance(obj, dict):
            def getter(key: str, default: Any = None) -> Any:
                return obj.get(key, default)
        else:
            def getter(key: str, default: Any = None) -> Any:
                return getattr(obj, key, default)

        file_type = getter("file_type", None)
        content_type = getter("content_type", None) or default_content_type

        if content_type is None and file_type:
            content_type = _DEFAULT_CONTENT_TYPES.get(str(file_type).lower())

        url = getter("url")
        filename = getter("filename") or Path(url).name

        if content_type is None:
            ext = Path(filename).suffix.lower().lstrip(".")
            if ext:
                content_type = _DEFAULT_CONTENT_TYPES.get(ext)

        content_type = content_type or "application/octet-stream"

        metadata = getter("metadata", {}) or {}

        entry = cls(
            url=url,
            filename=filename,
            content_type=content_type,
            size_bytes=getter("size_bytes", None),
            sha256=getter("checksum", None) or getter("sha256", None),
            last_modified=_isoformat(getter("last_modified", None)),
            year=getter("year", None),
            quarter=getter("quarter", None),
            file_type=file_type,
            metadata=dict(metadata),
        )

        return entry

    def signature(self) -> Tuple[Any, ...]:
        """Return a tuple suitable for equality checks between manifests."""

        return (
            self.url,
            self.filename,
            self.sha256,
            self.size_bytes,
            self.last_modified,
            self.file_type,
        )

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["metadata"] = dict(self.metadata)
        return _drop_nulls(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DiscoveryFileEntry":
        return cls(
            url=data["url"],
            filename=data["filename"],
            content_type=data["content_type"],
            size_bytes=data.get("size_bytes"),
            sha256=data.get("sha256"),
            last_modified=data.get("last_modified"),
            year=data.get("year"),
            quarter=data.get("quarter"),
            file_type=data.get("file_type"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class DiscoveryManifest:
    """Canonical representation of a discovery manifest."""

    source: str
    source_url: str
    discovered_from: str
    discovered_at: str
    files: List[DiscoveryFileEntry]
    metadata: Dict[str, Any] = field(default_factory=dict)
    license: Optional[Dict[str, Any]] = None
    notes_url: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    latest_only: Optional[bool] = None
    extras: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        source: str,
        source_url: str,
        discovered_from: str,
        files: Iterable[Union[DiscoveryFileEntry, Dict[str, Any], Any]],
        metadata: Optional[Dict[str, Any]] = None,
        license_info: Optional[Dict[str, Any]] = None,
        notes_url: Optional[str] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        latest_only: Optional[bool] = None,
        extras: Optional[Dict[str, Any]] = None,
        default_content_type: Optional[str] = None,
        discovered_at: Optional[str] = None,
    ) -> "DiscoveryManifest":
        entries: List[DiscoveryFileEntry] = []

        for item in files:
            if isinstance(item, DiscoveryFileEntry):
                entries.append(item)
            else:
                entries.append(
                    DiscoveryFileEntry.from_obj(
                        item, default_content_type=default_content_type
                    )
                )

        return cls(
            source=source,
            source_url=source_url,
            discovered_from=discovered_from,
            discovered_at=discovered_at or datetime.now(timezone.utc).isoformat(),
            files=entries,
            metadata=dict(metadata or {}),
            license=license_info,
            notes_url=notes_url,
            start_year=start_year,
            end_year=end_year,
            latest_only=latest_only,
            extras=dict(extras or {}),
        )

    def validate(self) -> List[str]:
        errors: List[str] = []

        for field in _REQUIRED_MANIFEST_FIELDS:
            if getattr(self, field, None) in (None, ""):
                errors.append(f"Manifest missing required field '{field}'")

        for idx, entry in enumerate(self.files):
            for field in _REQUIRED_FILE_FIELDS:
                if getattr(entry, field, None) in (None, ""):
                    errors.append(
                        f"Manifest file {idx} missing required field '{field}'"
                    )

        return errors

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "source": self.source,
            "source_url": self.source_url,
            "discovered_from": self.discovered_from,
            "discovered_at": self.discovered_at,
            "files": [entry.to_dict() for entry in self.files],
            "metadata": dict(self.metadata),
            "extras": dict(self.extras),
        }

        optional_fields = {
            "license": self.license,
            "notes_url": self.notes_url,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "latest_only": self.latest_only,
        }

        data.update(_drop_nulls(optional_fields))
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DiscoveryManifest":
        files = [DiscoveryFileEntry.from_dict(item) for item in data.get("files", [])]
        return cls(
            source=data["source"],
            source_url=data["source_url"],
            discovered_from=data.get("discovered_from", data["source_url"]),
            discovered_at=data.get("discovered_at", datetime.now(timezone.utc).isoformat()),
            files=files,
            metadata=dict(data.get("metadata", {})),
            license=data.get("license"),
            notes_url=data.get("notes_url"),
            start_year=data.get("start_year"),
            end_year=data.get("end_year"),
            latest_only=data.get("latest_only"),
            extras=dict(data.get("extras", {})),
        )

    def digest(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")
        return f"sha256:{hashlib.sha256(payload).hexdigest()[:16]}"

    def files_signature(self) -> Tuple[Tuple[Any, ...], ...]:
        return tuple(sorted(entry.signature() for entry in self.files))

    def has_same_files(self, other: Optional["DiscoveryManifest"]) -> bool:
        if other is None:
            return False
        return self.files_signature() == other.files_signature()


class DiscoveryManifestStore:
    """Persist discovery manifests in a structured, timestamped manner."""

    def __init__(self, manifest_dir: Union[str, Path], prefix: str):
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix

    def _manifest_path(self, timestamp: Optional[datetime] = None) -> Path:
        ts = (timestamp or datetime.now(timezone.utc)).strftime("%Y%m%d_%H%M%S")
        return self.manifest_dir / f"{self.prefix}_{ts}.jsonl"

    def save(self, manifest: DiscoveryManifest) -> Path:
        path = self._manifest_path()
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(manifest.to_dict()))
            f.write("\n")
        return path

    def load_latest(self) -> Optional[DiscoveryManifest]:
        try:
            candidates = list(self.manifest_dir.glob(f"{self.prefix}_*.jsonl"))
            candidates.extend(self.manifest_dir.glob(f"{self.prefix}_*.json"))
            manifest_files = sorted(
                candidates,
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except FileNotFoundError:
            manifest_files = []

        if not manifest_files:
            return None

        with open(manifest_files[0], "r", encoding="utf-8") as f:
            content = f.read().strip()
            data = json.loads(content) if content else {}
        return DiscoveryManifest.from_dict(data)

    def latest_path(self) -> Optional[Path]:
        candidates = list(self.manifest_dir.glob(f"{self.prefix}_*.jsonl"))
        candidates.extend(self.manifest_dir.glob(f"{self.prefix}_*.json"))
        files = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None
