from __future__ import annotations

from pathlib import Path
from typing import Iterator

PRDS_DIR = Path("prds")
MASTER_DOC_NAME = "DOC-master-catalog-prd-v1.0.md"
MASTER_DOC_PATH = PRDS_DIR / MASTER_DOC_NAME


def ensure_master_catalog() -> Path:
    """Return the master catalog path, ensuring it exists."""
    if not MASTER_DOC_PATH.exists():
        raise FileNotFoundError(f"Master catalog not found at {MASTER_DOC_PATH}")
    return MASTER_DOC_PATH


def read_master_catalog() -> str:
    return ensure_master_catalog().read_text(encoding="utf-8")


def iter_prd_paths(pattern: str = "*.md") -> Iterator[Path]:
    yield from PRDS_DIR.glob(pattern)


def get_prd_names(pattern: str = "*.md") -> set[str]:
    return {path.name for path in iter_prd_paths(pattern)}


def read_prd_text(name: str) -> str:
    return (PRDS_DIR / name).read_text(encoding="utf-8")


def read_path_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def classify_doc(name: str) -> str:
    return name.split("-", 1)[0] if "-" in name else name

