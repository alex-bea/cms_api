#!/usr/bin/env python3
"""
Generate a machine-readable report confirming content parity between the archived
STD parser contracts document and the new modularized surfaces.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from tools import prd_modularizer as modularizer

SOURCE_PATH = Path("prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md")
COMPARISON_TARGETS: Dict[str, Path] = {
    "policy": Path("prds/STD-parser-contracts-prd-v2.0.md"),
    "implementation": Path("prds/STD-parser-contracts-impl-v2.0.md"),
    "routing_ref": Path("prds/REF-parser-routing-detection-v1.0.md"),
    "quality_ref": Path("prds/REF-parser-quality-guardrails-v1.0.md"),
    "runbook": Path("prds/RUN-parser-qa-runbook-prd-v1.0.md"),
    "appendix": Path("prds/REF-parser-reference-appendix-v1.0.md"),
}
REPORT_PATH = Path("tools/reports/content_parity.json")


def ensure_source_exists() -> None:
    if not SOURCE_PATH.exists():
        raise SystemExit(f"Source document not found: {SOURCE_PATH}")


def collect_sections() -> Tuple[str, list]:
    text = SOURCE_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()
    sections = modularizer.parse_sections(lines)
    if not sections:
        raise SystemExit(f"No sections detected in {SOURCE_PATH}")
    return text, sections


def build_comparisons() -> Dict[str, Tuple[Path, str]]:
    comparisons: Dict[str, Tuple[Path, str]] = {}
    for category, path in COMPARISON_TARGETS.items():
        if not path.exists():
            print(f"⚠️  Warning: comparison target missing ({category}) → {path}")
            continue
        comparisons[category] = (path, path.read_text(encoding="utf-8"))
    return comparisons


def section_snapshot(section: modularizer.Section) -> Dict[str, int | str]:
    return {
        "title": section.title,
        "anchor": section.anchor,
        "line_start": section.start_line,
        "line_end": section.end_line,
    }


def build_report(sections, comparison_results) -> Dict[str, object]:
    categories: Dict[str, object] = {}
    for category, data in comparison_results.items():
        target_path = data.get("compare_path")
        found = data.get("found", [])
        missing = data.get("missing", [])
        total = data.get("total", 0)

        categories[category] = {
            "target": str(target_path) if target_path else None,
            "matched_count": len(found),
            "total_sections": total,
            "missing_sections": [section_snapshot(sec) for sec in missing],
            "matched_sections": [section_snapshot(sec) for sec in found],
        }

    report = {
        "source_file": str(SOURCE_PATH),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_sections": len(sections),
        "categories": categories,
    }
    return report


def main() -> None:
    ensure_source_exists()
    _, sections = collect_sections()
    comparisons = build_comparisons()
    _, _, comparison_results = modularizer.build_plan(
        SOURCE_PATH, sections, threshold=1200, comparisons=comparisons
    )
    report = build_report(sections, comparison_results)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"✅ Content parity report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
