#!/usr/bin/env python3
"""Cross-check PRD headers against the master catalog.

Warnings/errors when:
- A doc listed in the master catalog has no corresponding file.
- A PRD's status (from header) mismatches the catalog entry.
- Master entries lack concrete "Last Reviewed" dates (still 'YYYY-MM-DD').

Exit code 0 when everything aligns; otherwise prints discrepancies and exits 1.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple

PRDS_DIR = Path("prds")
MASTER_DOC = PRDS_DIR / "DOC-master-catalog_prd_v1.0.md"
HEADER_PATTERN = re.compile(r"^\*\*(?P<field>[^:]+):\*\*\s*(?P<value>.+?)\s*$")
STATUS_PATTERN = re.compile(r"(Adopted|Draft|Deprecated)[^\s]*", re.IGNORECASE)

@dataclass
class DocHeader:
    status: str


def parse_header(path: Path) -> DocHeader:
    lines = path.read_text(encoding="utf-8").splitlines()[:20]
    status_line = next((line for line in lines if "**Status:**" in line), None)
    if not status_line:
        raise ValueError(f"Missing Status field in {path.name}")

    match = HEADER_PATTERN.match(status_line.strip())
    if not match:
        raise ValueError(f"Malformed Status line in {path.name}: {status_line}")

    return DocHeader(status=match.group("value").strip())


def extract_table_entries(text: str) -> Dict[str, str]:
    tables: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("| `STD") and not line.startswith("| `REF") and not line.startswith("| `PRD") and not line.startswith("| `RUN"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 3:
            continue
        name = cells[0].strip("` ")
        status = cells[1]
        tables[name] = status
    return tables


def main() -> int:
    if not MASTER_DOC.exists():
        print(f"Master catalog not found at {MASTER_DOC}", file=sys.stderr)
        return 1

    master_text = MASTER_DOC.read_text(encoding="utf-8")
    catalog_status = extract_table_entries(master_text)
    actual_docs = {p.name for p in PRDS_DIR.glob("*.md")}

    issues = False

    missing_files = sorted(name for name in catalog_status if name not in actual_docs)
    if missing_files:
        issues = True
        print("[ERROR] Catalog lists docs that are missing on disk:")
        for name in missing_files:
            print(f"  - {name}")

    placeholder_dates = [line for line in master_text.splitlines() if "YYYY-MM-DD" in line]
    if placeholder_dates:
        issues = True
        print("[WARN] Catalog still contains placeholder last-reviewed dates (YYYY-MM-DD). Please update:")
        for line in placeholder_dates:
            print(f"  {line.strip()}")

    for name, status in catalog_status.items():
        if name not in actual_docs:
            continue
        try:
            header = parse_header(PRDS_DIR / name)
        except ValueError as exc:
            issues = True
            print(f"[ERROR] {exc}")
            continue

        header_status_norm = STATUS_PATTERN.search(header.status or "")
        catalog_status_norm = STATUS_PATTERN.search(status or "")

        if not header_status_norm:
            issues = True
            print(f"[ERROR] Could not parse header status for {name}: '{header.status}'")
            continue
        if not catalog_status_norm:
            issues = True
            print(f"[ERROR] Could not parse catalog status for {name}: '{status}'")
            continue

        if header_status_norm.group(0).lower() != catalog_status_norm.group(0).lower():
            issues = True
            print(f"[ERROR] Status mismatch for {name}: header='{header.status}' vs catalog='{status}'")

    if issues:
        return 1

    print("Metadata audit passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
