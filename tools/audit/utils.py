"""Shared utilities for PRD audit scripts."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, Set

PRDS_DIR = Path("prds")
MASTER_DOC = PRDS_DIR / "DOC-master-catalog_prd_v1.0.md"

_HEADER_PATTERN = re.compile(r"^\*\*(?P<field>[^:]+):\*\*\s*(?P<value>.+?)\s*$")
_CODE_REF_PATTERN = re.compile(r"`([A-Z]{3}-[a-z0-9\-]+_prd_v[0-9]+\.[0-9]+\.md)`")


def iter_prd_files() -> Iterable[Path]:
    """Yield all PRD markdown files sorted by name."""
    return sorted(PRDS_DIR.glob("*.md"))


def load_text(path: Path) -> str:
    """Read a file as UTF-8 text."""
    return path.read_text(encoding="utf-8")


def parse_header_fields(path: Path) -> Dict[str, str]:
    """Parse the governance header block from the top of a PRD.

    Returns a dictionary of header fields (Status, Owners, etc.).
    Raises ValueError if mandatory fields are missing or malformed.
    """
    lines = load_text(path).splitlines()[:20]
    fields: Dict[str, str] = {}
    for line in lines:
        match = _HEADER_PATTERN.match(line.strip())
        if match:
            field = match.group("field").strip()
            value = match.group("value").strip()
            fields[field] = value
    return fields


def extract_code_refs(text: str) -> Set[str]:
    """Return the set of backtick-wrapped PRD references inside markdown text."""
    return set(_CODE_REF_PATTERN.findall(text))


def ensure_master_exists() -> None:
    """Raise FileNotFoundError if the master catalog is missing."""
    if not MASTER_DOC.exists():
        raise FileNotFoundError(f"Master catalog not found at {MASTER_DOC}")


def extract_doc_codes(text: str) -> Set[str]:
    """Return all PRD identifiers referenced in the supplied text."""
    return extract_code_refs(text)
