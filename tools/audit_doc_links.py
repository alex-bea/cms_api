#!/usr/bin/env python3
"""Validate that cross-references inside PRDs point to real files and required links exist."""

from __future__ import annotations

import re
import sys
from pathlib import Path

PRDS_DIR = Path("prds")
MASTER_NAME = "DOC-master-catalog_prd_v1.0.md"
CODE_REF_PATTERN = re.compile(r"`([A-Z]{3}-[a-z0-9\-]+_prd_v[0-9]+\.[0-9]+\.md)`")

MANDATORY_REFS = {
    "STD": {MASTER_NAME},
    "REF": {MASTER_NAME},
    "PRD": {MASTER_NAME},
    "RUN": {MASTER_NAME},
    "DOC": set(),
}


def classify(name: str) -> str:
    return name.split("-", 1)[0]


def main() -> int:
    docs = {p.name for p in PRDS_DIR.glob("*.md")}
    issues = False

    for path in sorted(PRDS_DIR.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        refs = set(CODE_REF_PATTERN.findall(text))
        missing = sorted(ref for ref in refs if ref not in docs)
        if missing:
            issues = True
            print(f"[ERROR] {path.name} references missing docs:")
            for ref in missing:
                print(f"  - {ref}")

        category = classify(path.name)
        required = MANDATORY_REFS.get(category, set())
        for req in required:
            if req not in text and path.name != MASTER_NAME:
                issues = True
                print(f"[ERROR] {path.name} missing required reference to {req}")

    if issues:
        return 1

    print("Link audit passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
