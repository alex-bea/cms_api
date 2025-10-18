#!/usr/bin/env python3
"""
Audit governance document sizes to keep modular docs within agreed budgets.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Tuple

PRDS_DIR = Path("prds")

BUDGETS: Dict[str, Tuple[int, str]] = {
    "STD": (800, "STD core policy documents should stay concise (≤{limit} lines)."),
    "STD_IMPL": (900, "Companion implementation guides should stay under {limit} lines."),
    "REF": (900, "Reference guides must remain digestible for AI context."),
    "RUN": (800, "Runbooks should stay ≤{limit} lines for quick operational consumption."),
    "APPENDIX": (400, "Appendix/reference tables should remain lightweight."),
}


def classify(path: Path) -> str:
    name = path.name
    if name.startswith("STD-"):
        if "-impl-" in name:
            return "STD_IMPL"
        return "STD"
    if name.startswith("RUN-"):
        return "RUN"
    if name.startswith("REF-"):
        if "appendix" in name:
            return "APPENDIX"
        return "REF"
    return "OTHER"


def main() -> int:
    failures = []

    for path in sorted(PRDS_DIR.glob("*.md")):
        category = classify(path)
        if category not in BUDGETS:
            continue
        name = path.name
        if "-parser-" not in name:
            continue
        if "ARCHIVED" in name.upper():
            continue
        limit, message = BUDGETS[category]
        lines = sum(1 for _ in path.open("r", encoding="utf-8"))
        if lines > limit:
            failures.append((path, category, lines, limit, message.format(limit=limit)))

    if failures:
        print("❌ Document length audit failed:\n")
        for path, category, lines, limit, advice in failures:
            print(f"- {path} ({category}): {lines} lines (limit {limit})")
            print(f"  {advice}")
        return 1

    print("✅ All governance documents within configured size budgets.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
