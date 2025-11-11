"""Utility helpers for atomic filesystem writes used by publish workflows."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Any

CHUNK_SIZE = 1024 * 1024  # 1MB chunks for fsync + hashing


def _fsync_path(path: Path) -> None:
    """Flush file contents to disk if the path exists."""
    if not path.exists():
        return
    with path.open("rb") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    """Flush directory metadata to disk if the path exists."""
    if not path.exists():
        return
    fd = os.open(str(path), os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _temporary_path(target: Path) -> Path:
    """Create a temporary path in the target directory."""
    tmp_dir = target.parent / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=tmp_dir, suffix=target.suffix or ".tmp")
    os.close(fd)
    return Path(name)


def atomic_write(target: Path, writer: Callable[[Path], Any]) -> None:
    """Write a file via a temporary path and atomically replace the target."""
    temp_path = _temporary_path(target)
    try:
        writer(temp_path)
        _fsync_path(temp_path)
        os.replace(temp_path, target)
        _fsync_directory(target.parent)
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        raise


def atomic_write_json(target: Path, payload: Any, *, indent: int = 2) -> None:
    """Atomically persist JSON payloads to disk."""
    def _write(path: Path) -> None:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=indent)
            handle.flush()
            os.fsync(handle.fileno())

    atomic_write(target, _write)


def compute_sha256(path: Path) -> str:
    """Compute SHA256 digest for a file on disk."""
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
