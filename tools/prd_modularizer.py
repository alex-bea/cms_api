#!/usr/bin/env python3
"""
Generate modularization guidance for large PRD/STD documents.

This helper analyzes a markdown source file, groups sections into
governance-aligned buckets (policy, implementation, routing, quality,
runbook, appendix), and outputs a restructuring plan that follows
STD-doc-governance-prd-v1.0 expectations.

Usage:
    python tools/prd_modularizer.py <path-to-markdown> [--output plan.md]
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import textwrap
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclasses.dataclass
class Section:
    level: int
    title: str
    anchor: str
    start_line: int
    end_line: int
    line_count: int
    category: str = "uncategorized"


TARGET_INFO: Dict[str, Dict[str, str]] = {
    "policy": {
        "doc_type": "STD core policy",
        "description": "High-stability policy, scope, contracts, versioning, risks.",
        "recommended_name": "{prefix}-{slug}-prd-v{major_increment}.0.md",
    },
    "implementation": {
        "doc_type": "STD companion implementation guide",
        "description": "Detailed how-to content, templates, anti-patterns for engineers.",
        "recommended_name": "{prefix}-{slug}-impl-v{major_increment}.0.md",
    },
    "routing_ref": {
        "doc_type": "REF routing/reference architecture",
        "description": "Router/layout detection strategies, flowcharts, anti-patterns.",
        "recommended_name": "REF-{slug}-routing-v1.0.md",
    },
    "quality_ref": {
        "doc_type": "REF quality guardrails",
        "description": "Validation tiers, error taxonomy, metrics, observability patterns.",
        "recommended_name": "REF-{slug}-quality-guardrails-v1.0.md",
    },
    "runbook": {
        "doc_type": "RUN QA/operations runbook",
        "description": "SLAs, checklists, golden workflows, production procedures.",
        "recommended_name": "RUN-{slug}-qa-runbook-prd-v1.0.md",
    },
    "appendix": {
        "doc_type": "REF appendix",
        "description": "Static reference tables, examples, compatibility matrices.",
        "recommended_name": "REF-{slug}-appendix-v1.0.md",
    },
    "uncategorized": {
        "doc_type": "Review needed",
        "description": "Sections that did not match any rule; inspect manually.",
        "recommended_name": "TBD",
    },
}


METADATA_DEFAULTS: Dict[str, Dict[str, object]] = {
    "policy": {
        "doc_type": "STD",
        "normative": True,
        "requires": [
            "STD-doc-governance-prd-v1.0.md#3-metadata-requirements",
            "STD-doc-governance-prd-v1.0.md#6-cross-referencing-guidelines",
        ],
    },
    "implementation": {
        "doc_type": "STD",
        "normative": False,
        "requires": ["STD-parser-contracts-prd-v2.0.md#6-contracts"],
    },
    "routing_ref": {
        "doc_type": "REF",
        "normative": False,
        "requires": ["STD-parser-contracts-prd-v2.0.md#62-router-contract"],
    },
    "quality_ref": {
        "doc_type": "REF",
        "normative": False,
        "requires": ["STD-parser-contracts-prd-v2.0.md#8-validation-requirements"],
    },
    "runbook": {
        "doc_type": "RUN",
        "normative": False,
        "requires": ["STD-parser-contracts-prd-v2.0.md#5-scope-requirements"],
    },
    "appendix": {
        "doc_type": "REF",
        "normative": False,
        "requires": ["STD-parser-contracts-prd-v2.0.md#6-contracts"],
    },
}

BASELINE_OVERRIDES: Dict[str, Dict[str, str]] = {
    # Specific overrides for known modular doc sets (slug -> category -> filename)
    "parser-contracts-prd": {
        "policy": "STD-parser-contracts-prd-v2.0.md",
        "implementation": "STD-parser-contracts-impl-v2.0.md",
        "routing_ref": "REF-parser-routing-detection-v1.0.md",
        "quality_ref": "REF-parser-quality-guardrails-v1.0.md",
        "runbook": "RUN-parser-qa-runbook-prd-v1.0.md",
        "appendix": "REF-parser-reference-appendix-v1.0.md",
    },
}


CATEGORY_PATTERNS: List[Tuple[str, Iterable[re.Pattern]]] = [
    (
        "policy",
        [
            re.compile(p, re.I)
            for p in [
                r"\bsummary\b",
                r"\bgoal",
                r"\bnon-goal",
                r"\busers\b",
                r"\bkey decisions\b",
                r"\bscope\b",
                r"\bcontracts?\b",
                r"\bcompatibility\b",
                r"\bversioning\b",
                r"\brisk",
                r"\bmitigation",
                r"\broadmap\b",
                r"\bsecurity\b",
                r"\bacceptance\b",
                r"\bglossary\b",
            ]
        ],
    ),
    (
        "implementation",
        [
            re.compile(p, re.I)
            for p in [
                r"processing requirement",
                r"\binputs\b",
                r"\boutputs\b",
                r"implementation template",
                r"\bdefensive\b",
                r"type handling",
                r"alias map",
                r"\banti-pattern\b",
                r"validation phase",
                r"canonicalize",
            ]
        ],
    ),
    (
        "routing_ref",
        [
            re.compile(p, re.I)
            for p in [
                r"\brouter\b",
                r"layout registry",
                r"format detection",
                r"\brouting\b",
                r"\bzip\b",
                r"layout-schema alignment",
                r"\bregistry\b",
            ]
        ],
    ),
    (
        "quality_ref",
        [
            re.compile(p, re.I)
            for p in [
                r"\bvalidation\b",
                r"\bvalidation tiers\b",
                r"\bguardrail",
                r"\bquality\b",
                r"\berror\b",
                r"\bexception\b",
                r"\bmetric",
                r"\bobservability\b",
                r"\blogging\b",
                r"\bquarantine\b",
            ]
        ],
    ),
    (
        "runbook",
        [
            re.compile(p, re.I)
            for p in [
                r"\bsla\b",
                r"\bconstraints\b",
                r"\bchecklist\b",
                r"\bpre[- ]implementation\b",
                r"\bqa\b",
                r"\bgolden\b",
                r"variance analysis",
                r"\bacceptance criteria\b",
                r"\boperational\b",
                r"\brunbook\b",
                r"\bworkflow\b",
            ]
        ],
    ),
    (
        "appendix",
        [
            re.compile(p, re.I)
            for p in [
                r"\bappendix\b",
                r"\breference\b",
                r"\bexamples\b",
                r"\bcompatibility\b",
                r"\bsource\b",
            ]
        ],
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate modularization guidance for a PRD/STD markdown document."
    )
    parser.add_argument("path", type=Path, help="Path to the markdown document.")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Optional path to save the generated plan (markdown).",
    )
    parser.add_argument(
        "--compare",
        action="append",
        metavar="CATEGORY=PATH",
        help=(
            "Compare a generated bucket against an existing document to ensure sections were migrated. "
            "Example: --compare policy=prds/STD-parser-contracts-prd-v2.0.md "
            "--compare implementation=prds/STD-parser-contracts-impl-v2.0.md"
        ),
    )
    parser.add_argument(
        "--export",
        action="append",
        metavar="CATEGORY=PATH",
        help=(
            "Copy raw sections for a category into the specified file. "
            "Example: --export routing_ref=prds/REF-parser-routing-detection-v1.0.md"
        ),
    )
    parser.add_argument(
        "--export-mode",
        choices=["append", "overwrite"],
        default="append",
        help=(
            "When exporting sections, append to existing file content (default) or overwrite it."
        ),
    )
    parser.add_argument(
        "--comparison-report",
        type=Path,
        help="Write detailed comparison results to this path.",
    )
    parser.add_argument(
        "--auto-compare",
        action="store_true",
        help="Automatically compare against existing modular documents if detected.",
    )
    parser.add_argument(
        "--suggestions-output",
        type=Path,
        help="Optional path to write human-readable suggestions based on comparison gaps.",
    )
    parser.add_argument(
        "--compare-root",
        type=Path,
        default=Path("prds"),
        help="Directory to search for existing modular documents when auto-comparing (default: prds/).",
    )
    parser.add_argument(
        "--min-lines",
        type=int,
        default=1200,
        help="Line count threshold to flag a document as long (default: 1200).",
    )
    return parser.parse_args()


def slugify(title: str) -> str:
    value = re.sub(r"[^a-z0-9\s-]", "", title.lower())
    value = re.sub(r"\s+", "-", value.strip())
    return value


def parse_sections(lines: List[str]) -> List[Section]:
    sections: List[Section] = []
    for idx, line in enumerate(lines):
        if not line.startswith("#"):
            continue
        hashes = len(line) - len(line.lstrip("#"))
        if hashes == 0:
            continue
        title = line.strip("#").strip()
        anchor = slugify(title)
        section = Section(
            level=hashes,
            title=title,
            anchor=anchor,
            start_line=idx + 1,
            end_line=len(lines),
            line_count=0,
        )
        sections.append(section)

    for i, section in enumerate(sections):
        if i + 1 < len(sections):
            next_section = sections[i + 1]
            section.end_line = next_section.start_line - 1
        else:
            section.end_line = len(lines)
        section.line_count = section.end_line - section.start_line + 1

    return sections


def categorize_section(section: Section) -> str:
    title_lower = section.title.lower()
    for category, patterns in CATEGORY_PATTERNS:
        if any(pattern.search(title_lower) for pattern in patterns):
            return category
    # Heuristic: top-level sections without keywords often belong to policy.
    if section.level <= 2 and re.match(r"^##?\s*[0-9]+\.", f"{'#' * section.level} {section.title}"):
        return "policy"
    return "uncategorized"


def determine_prefix_slug(path: Path) -> Tuple[str, str, str, int]:
    """
    Extract prefix (STD/PRD/etc), slug, suffix, and major version from filename.
    """
    name = path.name.replace(".md", "")
    parts = name.split("-")
    if len(parts) < 3:
        return "DOC", name, "", 1

    prefix = parts[0]
    version_idx = None
    for i in range(len(parts) - 1, 0, -1):
        if re.match(r"^v[0-9]+(\.[0-9]+)*$", parts[i], re.IGNORECASE):
            version_idx = i
            break

    if version_idx is None:
        version_part = parts[-1]
        slug_parts = parts[1:-1]
    else:
        version_part = parts[version_idx]
        slug_parts = parts[1:version_idx]

    suffix_parts = []
    if version_idx is not None and version_idx + 1 < len(parts):
        suffix_parts = parts[version_idx + 1 :]

    suffix_token = "-".join(suffix_parts)

    if slug_parts and slug_parts[-1] in {"prd", "impl"} and suffix_token:
        # Keep trailing markers for slug only if no suffix present
        pass

    slug = "-".join(slug_parts)
    if not slug:
        slug = "-".join(parts[1:-1]) or path.stem
    suffix = suffix_token
    major_version = 1
    match = re.search(r"v([0-9]+)", version_part)
    if match:
        major_version = int(match.group(1))

    if not slug:
        slug = "-".join(parts[1:-1]) or path.stem

    return prefix, slug, version_part, major_version


def build_plan(
    path: Path,
    sections: List[Section],
    threshold: int,
    comparisons: Optional[Dict[str, Tuple[Path, str]]] = None,
) -> Tuple[str, Dict[str, List[Section]], Dict[str, Dict[str, object]]]:
    total_lines = sum(section.line_count for section in sections)
    prefix, slug, version_part, major_version = determine_prefix_slug(path)
    recommended_major = max(major_version + 1, major_version) if version_part.startswith("v") else major_version

    categorized: Dict[str, List[Section]] = {key: [] for key in TARGET_INFO}
    for section in sections:
        category = categorize_section(section)
        section.category = category
        categorized.setdefault(category, []).append(section)

    doc_is_long = total_lines >= threshold

    output_lines: List[str] = []
    output_lines.append(f"# Modularization Guidance for `{path}`")
    output_lines.append("")
    output_lines.append(f"- Total lines (approx): **{total_lines}**")
    output_lines.append(f"- Section count: **{len(sections)}**")
    if doc_is_long:
        output_lines.append(f"- Status: Document exceeds {threshold} line threshold → modularization recommended.")
    else:
        output_lines.append(f"- Status: Document below {threshold} lines (modularization optional).")
    output_lines.append("")
    output_lines.append("## Suggested Document Breakdown")
    output_lines.append("")

    comparison_results: Dict[str, Dict[str, object]] = {}

    for category, info in TARGET_INFO.items():
        assigned = categorized.get(category, [])
        if not assigned:
            continue
        recommended_name = info["recommended_name"].format(
            prefix=prefix,
            slug=slug,
            major_increment=recommended_major,
        )
        output_lines.append(f"### {info['doc_type']}")
        output_lines.append(f"- Proposed filename: `{recommended_name}`")
        output_lines.append(f"- Purpose: {info['description']}")
        total_category_lines = sum(sec.line_count for sec in assigned)
        output_lines.append(f"- Sections captured ({total_category_lines} lines):")
        for sec in assigned:
            output_lines.append(
                f"  - {sec.title} (lines {sec.start_line}-{sec.end_line}, anchor `#{sec.anchor}`)"
            )
        comparison_entry = None
        if comparisons and category in comparisons:
            comparison_entry = comparisons[category]
        if category == "policy":
            output_lines.append(
                "  - Governance reminder: add companion references per §1.5 of STD-doc-governance."
            )
        if category == "implementation":
            output_lines.append(
                "  - Include `Companion to: <core STD>` in header and keep change log in sync."
            )
        if category == "runbook":
            output_lines.append(
                "  - Ensure SLA tables and checklists align with RUN naming rules (prefix + `-prd`)."
            )
        if comparison_entry:
            compare_path, compare_text = comparison_entry
            found, missing = evaluate_section_coverage(assigned, compare_text)
            comparison_results[category] = {
                "compare_path": compare_path,
                "found": found,
                "missing": missing,
                "total": len(assigned),
            }
            output_lines.append(
                f"- Comparison vs `{compare_path}`: {len(found)}/{len(assigned)} section titles matched."
            )
            if missing:
                output_lines.append("  - Missing titles (review to ensure content migrated):")
                for sec in missing:
                    output_lines.append(
                        f"    - {sec.title} (original lines {sec.start_line}-{sec.end_line})"
                    )
            output_lines.append("")
        output_lines.append("")

    uncategorized = [sec for sec in sections if sec.category == "uncategorized"]
    if uncategorized:
        output_lines.append("## Manual Review Needed")
        output_lines.append(
            "The following sections did not match known governance buckets; decide where they belong:"
        )
        for sec in uncategorized:
            output_lines.append(
                f"- {sec.title} (lines {sec.start_line}-{sec.end_line})"
            )
        output_lines.append("")

    output_lines.append("## Execution Checklist")
    output_lines.extend(
        textwrap.dedent(
            """
            - [ ] Seed each new document with the governance header (`Status`, `Owners`, `Consumers`, `Change control`).
            - [ ] Add `## Change Log` with initial entry (`vX.Y.Z – Initial adoption / Modularization`).
            - [ ] Preserve or update anchors referenced by other documents (consider creating stub sections during transition).
            - [ ] Update `DOC-master-catalog-prd-v*.md` with new entries and companion relationships.
            - [ ] Run `tools/audit_doc_links.py`, `tools/audit_cross_references.py`, and `tools/audit_doc_catalog.py`.
            - [ ] Archive the original document with a `Deprecated` status once migration completes.
            """
        ).strip().splitlines()
    )

    return "\n".join(output_lines) + "\n", categorized, comparison_results


def build_metadata_block(category: str, source_path: Path) -> str:
    defaults = METADATA_DEFAULTS.get(
        category,
        {"doc_type": "DOC", "normative": False, "requires": []},
    )
    doc_type = defaults.get("doc_type", "DOC")
    normative_flag = "true" if defaults.get("normative", False) else "false"
    requires = list(defaults.get("requires", []))
    source_ref = source_path.name
    if category != "policy" and source_ref not in requires:
        requires.append(source_ref)
    requires = list(dict.fromkeys(requires))

    lines = [f"doc_type: {doc_type}", f"normative: {normative_flag}"]
    if requires:
        lines.append("requires:")
        for ref in requires:
            lines.append(f"  - {ref}")
    else:
        lines.append("requires: []")
    return "\n".join(lines)


def build_provenance_comment(source_path: Path, section: Section) -> str:
    return (
        f"<!-- Source: {source_path} §{section.title} "
        f"(lines {section.start_line}-{section.end_line}, anchor #{section.anchor}) -->"
    )


def build_source_sections_block(
    source_name: str, entries: List[Tuple[str, str, int, int]]
) -> str:
    if not entries:
        return ""
    lines = [f"## Source Sections ({source_name})", ""]
    for title, anchor, start, end in entries:
        lines.append(
            f"- §{title} (anchor `#{anchor}`, lines {start}-{end})"
        )
    return "\n".join(lines).rstrip()


def remove_provenance_block(text: str, source_name: str) -> str:
    pattern = re.compile(
        rf"\n*## Source Sections \({re.escape(source_name)}\)(?:.|\n)*$", re.MULTILINE
    )
    return re.sub(pattern, "", text)


def perform_exports(
    exports: Dict[str, Path],
    categorized: Dict[str, List[Section]],
    all_lines: List[str],
    mode: str,
    source_path: Path,
) -> None:
    source_name = source_path.name
    for category, target_path in exports.items():
        sections = categorized.get(category)
        if not sections:
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        existing = ""
        if mode != "overwrite" and target_path.exists():
            existing = target_path.read_text(encoding="utf-8")
            existing = remove_provenance_block(existing, source_name).rstrip()
        has_metadata = existing.lstrip().startswith("doc_type:") if existing else False
        chunks: List[str] = []
        entries: List[Tuple[str, str, int, int]] = []
        for sec in sections:
            start_idx = max(sec.start_line - 1, 0)
            end_idx = max(sec.end_line, start_idx)
            original = "\n".join(all_lines[start_idx:end_idx]).rstrip()
            if not original:
                continue
            comment = build_provenance_comment(source_path, sec)
            chunk = f"{comment}\n{original}"
            chunks.append(chunk)
            entries.append((sec.title, sec.anchor, sec.start_line, sec.end_line))
        if not chunks:
            continue
        body = "\n\n".join(chunks).rstrip()
        combined = existing.rstrip()
        if mode == "overwrite":
            combined = ""
        if not has_metadata:
            metadata_block = build_metadata_block(category, source_path)
            if combined:
                combined = metadata_block + "\n\n" + combined.lstrip()
            else:
                combined = metadata_block
        if combined:
            if not combined.endswith("\n"):
                combined += "\n"
            combined += "\n"
        combined += body
        source_block = build_source_sections_block(source_name, entries)
        if source_block:
            if combined and not combined.endswith("\n"):
                combined += "\n"
            if combined and not combined.endswith("\n\n"):
                combined += "\n"
            combined += source_block
        if combined and not combined.endswith("\n"):
            combined += "\n"
        target_path.write_text(combined, encoding="utf-8")


def evaluate_section_coverage(
    sections: List[Section], comparison_text: str
) -> Tuple[List[Section], List[Section]]:
    text_lower = comparison_text.lower()
    found: List[Section] = []
    missing: List[Section] = []
    for sec in sections:
        if section_present(sec, text_lower):
            found.append(sec)
        else:
            missing.append(sec)
    return found, missing


def section_present(section: Section, comparison_text_lower: str) -> bool:
    title_lower = section.title.lower()
    if title_lower and title_lower in comparison_text_lower:
        return True
    anchor = section.anchor.lower()
    if anchor and anchor in comparison_text_lower:
        return True
    tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", title_lower)
        if token and len(token) > 3
    ]
    if tokens:
        matches = sum(1 for token in tokens if token in comparison_text_lower)
        if matches >= max(1, len(tokens) // 2):
            return True
    return False


def render_comparison_report(
    source: Path, results: Dict[str, Dict[str, object]]
) -> str:
    if not results:
        return (
            f"# Comparison Results for `{source}`\n\n"
            "No comparison targets were provided.\n"
        )
    lines: List[str] = [
        f"# Comparison Results for `{source}`",
        "",
    ]
    for category, data in results.items():
        compare_path = data.get("compare_path")
        found = data.get("found", [])
        missing = data.get("missing", [])
        total = data.get("total", 0)
        lines.append(
            f"## {TARGET_INFO.get(category, {}).get('doc_type', category.title())}"
        )
        if compare_path:
            lines.append(f"- Target file: `{compare_path}`")
        lines.append(f"- Matched sections: {len(found)} / {total}")
        if missing:
            lines.append("- Missing section titles:")
            for sec in missing:
                lines.append(
                    f"  - §{sec.title} (anchor `#{sec.anchor}`, lines {sec.start_line}-{sec.end_line})"
                )
        else:
            lines.append("- All section titles matched.")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def compute_recommended_filename(
    category: str,
    prefix: str,
    slug: str,
    major_increment: int,
) -> Optional[str]:
    info = TARGET_INFO.get(category)
    if not info:
        return None
    template = info.get("recommended_name")
    if not template:
        return None
    return template.format(
        prefix=prefix,
        slug=slug,
        major_increment=major_increment,
    )


def discover_auto_comparisons(
    prefix: str,
    slug: str,
    major_increment: int,
    compare_root: Path,
) -> Dict[str, Path]:
    auto_map: Dict[str, Path] = {}
    override = BASELINE_OVERRIDES.get(slug, {})
    for category in TARGET_INFO.keys():
        if category == "uncategorized":
            continue
        candidate: Optional[Path] = None
        if category in override:
            override_path = compare_root / override[category]
            if override_path.exists():
                candidate = override_path
        if candidate is None:
            filename = compute_recommended_filename(
                category, prefix, slug, major_increment
            )
            if filename:
                suggested = compare_root / filename
                if suggested.exists():
                    candidate = suggested
        if candidate and candidate.exists():
            auto_map[category] = candidate
    return auto_map


def render_suggestions(
    comparison_results: Dict[str, Dict[str, object]]
) -> str:
    lines: List[str] = []
    for category, data in comparison_results.items():
        missing = data.get("missing", [])
        if not missing:
            continue
        doc_title = TARGET_INFO.get(category, {}).get("doc_type", category.title())
        compare_path = data.get("compare_path")
        lines.append(f"### {doc_title}")
        if compare_path:
            lines.append(f"- Baseline: `{compare_path}`")
        lines.append("- Missing sections to review:")
        for sec in missing:
            lines.append(
                f"  - §{sec.title} (lines {sec.start_line}-{sec.end_line})"
            )
        lines.append("")
    if not lines:
        return "All compared documents already contain the assigned sections.\n"
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    path = args.path
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    prefix, slug, version_part, major_version = determine_prefix_slug(path)
    if version_part.startswith("v"):
        recommended_major = max(major_version + 1, major_version)
    else:
        recommended_major = major_version
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    sections = parse_sections(lines)
    if not sections:
        raise SystemExit("No sections found; ensure the document uses markdown headings.")
    comparisons: Dict[str, Tuple[Path, str]] = {}
    exports: Dict[str, Path] = {}
    if args.compare:
        for entry in args.compare:
            if "=" not in entry:
                raise SystemExit(f"Invalid --compare argument: '{entry}'. Use CATEGORY=PATH.")
            category, value = entry.split("=", 1)
            category = category.strip().lower()
            if category not in TARGET_INFO:
                raise SystemExit(
                    f"Unknown category '{category}'. Valid options: {', '.join(TARGET_INFO.keys())}"
                )
            compare_path = Path(value.strip())
            if not compare_path.exists():
                raise SystemExit(f"Comparison file not found: {compare_path}")
            comparisons[category] = (compare_path, compare_path.read_text(encoding="utf-8"))
    if args.export:
        for entry in args.export:
            if "=" not in entry:
                raise SystemExit(f"Invalid --export argument: '{entry}'. Use CATEGORY=PATH.")
            category, value = entry.split("=", 1)
            category = category.strip().lower()
            if category not in TARGET_INFO:
                raise SystemExit(
                    f"Unknown category '{category}'. Valid options: {', '.join(TARGET_INFO.keys())}"
                )
            export_path = Path(value.strip())
            exports[category] = export_path
    plan_md, categorized, comparison_results = build_plan(
        path, sections, args.min_lines, comparisons if comparisons else None
    )

    if args.auto_compare:
        auto_candidates = discover_auto_comparisons(
            prefix=prefix,
            slug=slug,
            major_increment=recommended_major,
            compare_root=args.compare_root,
        )
        added = False
        for category, candidate_path in auto_candidates.items():
            if category in comparisons:
                continue
            if category not in categorized or not categorized[category]:
                continue
            if candidate_path.resolve() == path.resolve():
                continue
            comparisons[category] = (
                candidate_path,
                candidate_path.read_text(encoding="utf-8"),
            )
            added = True
        if added:
            plan_md, categorized, comparison_results = build_plan(
                path, sections, args.min_lines, comparisons
            )

    if exports:
        perform_exports(exports, categorized, lines, args.export_mode, path)
    if args.comparison_report:
        report_text = render_comparison_report(path, comparison_results)
        args.comparison_report.parent.mkdir(parents=True, exist_ok=True)
        args.comparison_report.write_text(report_text, encoding="utf-8")
    suggestions_text: Optional[str] = None
    if comparison_results:
        suggestions_text = render_suggestions(comparison_results)
        if args.suggestions_output:
            args.suggestions_output.parent.mkdir(parents=True, exist_ok=True)
            args.suggestions_output.write_text(suggestions_text, encoding="utf-8")
        elif args.auto_compare and suggestions_text.strip() and not suggestions_text.startswith("All compared"):
            print("\n## Comparison Suggestions\n")
            print(suggestions_text)
    if args.output:
        args.output.write_text(plan_md, encoding="utf-8")
    else:
        print(plan_md)


if __name__ == "__main__":
    main()
