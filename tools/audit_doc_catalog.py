#!/usr/bin/env python3
"""Consistency audit for documentation catalog.

Usage
-----
Run locally:

    python tools/audit_doc_catalog.py

CI / automation:
- This script is wired into a scheduled GitHub Actions workflow (see
  `.github/workflows/doc-catalog-audit.yml`) that runs weekly. The workflow
  fails if any inconsistencies are detected.

Checks performed
----------------
1. Every markdown file in `prds/` appears in the master catalog.
2. The master catalog does not reference docs that do not exist.
3. Every doc (except the master itself) links back to the master catalog.

Exit code 0 = all good; otherwise prints issues and exits 1.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PRDS_DIR = Path("prds")
MASTER_DOC = PRDS_DIR / "DOC-master-catalog_prd_v1.0.md"
MASTER_LINK = "DOC-master-catalog_prd_v1.0.md"
EXEMPT_FROM_MASTER_LINK = {MASTER_DOC.name}

DOC_PATTERN = re.compile(r"`([A-Z]{3}-[a-z0-9\-]+_prd_v[0-9]+\.[0-9]+\.md)`")


def discover_docs() -> set[str]:
    return {p.name for p in PRDS_DIR.glob("*.md")}


def extract_master_entries(text: str) -> set[str]:
    return set(DOC_PATTERN.findall(text))


def docs_missing_master_link(docs: set[str]) -> list[str]:
    missing = []
    for name in sorted(docs):
        if name in EXEMPT_FROM_MASTER_LINK:
            continue
        if MASTER_LINK not in (PRDS_DIR / name).read_text(encoding="utf-8"):
            missing.append(name)
    return missing


def main() -> int:
    if not MASTER_DOC.exists():
        print(f"Master catalog not found at {MASTER_DOC}", file=sys.stderr)
        return 1

    actual_docs = discover_docs()
    master_text = MASTER_DOC.read_text(encoding="utf-8")
    catalog_docs = extract_master_entries(master_text)

    missing_from_catalog = sorted(actual_docs - catalog_docs)
    stale_in_catalog = sorted(catalog_docs - actual_docs)
    no_master_link = docs_missing_master_link(actual_docs)

    issues = False

    if missing_from_catalog:
        issues = True
        print("[ERROR] Docs missing from master catalog:")
        for name in missing_from_catalog:
            print(f"  - {name}")

    if stale_in_catalog:
        issues = True
        print("[ERROR] Master catalog references non-existent docs:")
        for name in stale_in_catalog:
            print(f"  - {name}")

    if no_master_link:
        issues = True
        print("[ERROR] Docs missing reference to master catalog:")
        for name in no_master_link:
            print(f"  - {name}")

    if not issues:
        print("Documentation catalog audit passed.")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
