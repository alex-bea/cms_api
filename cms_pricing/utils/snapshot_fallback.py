"""Helpers for locating parquet drops and falling back to the latest available release."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


DEFAULT_SEARCH_ROOTS = (
    Path.cwd(),
    Path("/var/data/ingestion"),
    Path("/var/data/curated"),
)


@dataclass(frozen=True)
class ReleaseCandidate:
    release: str
    path: Path
    date: datetime


def collect_search_roots(explicit_roots: Optional[Sequence[Path]] = None) -> List[Path]:
    """Collect filesystem roots for snapshot resolution."""
    roots: List[Path] = []

    def _add(root: Path) -> None:
        root = Path(root)
        if root and root not in roots:
            roots.append(root)

    env_roots = os.getenv("SNAPSHOT_SEARCH_ROOTS")
    if env_roots:
        for chunk in env_roots.split(os.pathsep):
            chunk = chunk.strip()
            if chunk:
                _add(Path(chunk))

    for default_root in DEFAULT_SEARCH_ROOTS:
        _add(default_root)

    if explicit_roots:
        for root in explicit_roots:
            _add(root)

    return roots


def discover_latest_release(
    file_prefix: str,
    search_roots: Sequence[Path],
    dataset_hint: Optional[str] = None,
) -> Optional[ReleaseCandidate]:
    """Return the latest parquet release available across search roots."""
    pattern = f"{file_prefix}_*.parquet"
    best: Optional[ReleaseCandidate] = None

    for root in search_roots:
        if not root.exists():
            continue
        try:
            iterator = root.rglob(pattern)
        except Exception:
            continue

        for candidate in iterator:
            if dataset_hint and dataset_hint not in candidate.parts:
                continue
            release = _release_from_path(candidate)
            if not release:
                continue
            release_dt = _parse_release_date(release)
            if not release_dt:
                continue
            rc = ReleaseCandidate(release=release, path=candidate, date=release_dt)
            if not best or rc.date > best.date:
                best = rc

    return best


def _release_from_path(path: Path) -> Optional[str]:
    """Return release directory name inferred from `<release>/data/<file>` structure."""
    try:
        return path.parent.parent.name
    except IndexError:
        return None


def _parse_release_date(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def replace_release_in_path(path: Path, new_release: str, new_filename_prefix: Optional[str] = None) -> Path:
    """Replace the release segment and filename portion of a repo-relative path."""
    parts = list(path.parts)
    if len(parts) < 3:
        return path

    release_idx = len(parts) - 3  # release sits before `data/<file>`
    if release_idx < 0:
        return path

    parts[release_idx] = new_release
    filename = parts[-1]
    name_root, ext = os.path.splitext(filename)
    prefix = new_filename_prefix or name_root.split("_")[0]
    parts[-1] = f"{prefix}_{new_release}{ext or '.parquet'}"
    return Path(*parts)


def filename_prefix(path: Path, dataset_id: Optional[str] = None) -> str:
    """Infer the parquet filename prefix (e.g., `pprrvu`)"""
    stem = path.stem
    return stem.split("_")[0] if stem else (dataset_id or "dataset")

