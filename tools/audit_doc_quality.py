#!/usr/bin/env python3
"""Audit documentation quality for Phase 2 standards."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple, Dict

# Thresholds
CODE_BLOCK_MAX_LINES = 120
VERBOSE_LIST_THRESHOLD = 6
PHASE2_HEADINGS = {
    "Schema Contracts & Validation": {"table_required": True},
}

PHASE2_TRACEABILITY: Dict[str, List[str]] = {
    "3.2.1 SchemaService Pattern": ["services/schema_service.py"],
    "3.2.2 Database Loading Pattern": ["datasets/rvu_loaders.py", "stages/publish.py"],
    "3.2.3 Adapter Extraction Pattern": ["datasets/rvu_adapter.py", "stages/normalize.py"],
    "3.3 Validation Rules & Business Rules": ["datasets/rvu_spec.py", "services/validation_service.py"],
    "6.2 Step 1: Create Ingestor Class": ["rvu_ingestor.py"],
}

CALLOUT_KEYWORDS = ["ENABLE_ENRICHMENT", "schema drift"]

# Patterns
CODE_BLOCK_PATTERN = re.compile(r"```[a-zA-Z0-9_]*\n[\s\S]*?```", re.MULTILINE)
LIST_PATTERN = re.compile(r"(^\s*[-*].*$\n(?:^\s{2,}[-*].*$\n)*)", re.MULTILINE)
HEADING_PATTERN = re.compile(r"^(#+)\s+(.*)$", re.MULTILINE)
SECTION_REF_PATTERN = re.compile(r"§(\d+(?:\.\d+)*)")


class Issue:
    def __init__(self, severity: str, message: str, location: str):
        self.severity = severity
        self.message = message
        self.location = location

    def __str__(self):
        return f"[{self.severity}] {self.location}: {self.message}"


def find_duplicate_blocks(text: str) -> List[Issue]:
    issues = []
    chunks = [c.strip() for c in text.split("\n\n") if len(c.strip()) > 200]
    seen = {}
    for chunk in chunks:
        key = chunk[:120]
        seen.setdefault(key, 0)
        seen[key] += 1
    for key, count in seen.items():
        if count > 1:
            issues.append(Issue("WARN", "Duplicate prose block detected", key))
    return issues


def find_oversized_code_blocks(text: str) -> List[Issue]:
    issues = []
    for match in CODE_BLOCK_PATTERN.finditer(text):
        block = match.group(0)
        lines = block.count("\n")
        if lines > CODE_BLOCK_MAX_LINES:
            issues.append(Issue("WARN", f"Code block is {lines} lines (limit {CODE_BLOCK_MAX_LINES})", "Code block"))
    return issues


def find_verbose_lists(text: str, code_spans: List[Tuple[int, int]]) -> List[Issue]:
    issues = []
    for match in LIST_PATTERN.finditer(text):
        block = match.group(0)
        bullet_count = len(re.findall(r"^\s*[-*]", block, re.MULTILINE))
        if bullet_count >= VERBOSE_LIST_THRESHOLD:
            issues.append(Issue("NOTE", f"List has {bullet_count} items; consider tightening", "Bullet list"))
    return issues


def ensure_phase2_table(text: str) -> List[Issue]:
    issues = []
    for heading, config in PHASE2_HEADINGS.items():
        match = re.search(rf"##+\s+{re.escape(heading)}", text)
        if match:
            section = text[match.end():]
            snippet = section[:500]
            if config.get("table_required") and "| Pattern" not in snippet:
                issues.append(Issue("WARN", f"Phase 2 heading '{heading}' missing quick-start table", heading))
    return issues


def ensure_traceability(text: str) -> List[Issue]:
    issues = []
    if "docs/release_notes/phase2_refactor.md" not in text:
        issues.append(Issue("NOTE", "Phase 2 section missing release notes reference", "Traceability"))
    headings = {match.group(2).strip(): match.start() for match in HEADING_PATTERN.finditer(text)}
    for heading, refs in PHASE2_TRACEABILITY.items():
        for title, pos in headings.items():
            if title.startswith(heading):
                snippet = text[pos:text.find("\n###", pos) if text.find("\n###", pos) != -1 else len(text)]
                if not all(ref in snippet for ref in refs):
                    issues.append(Issue("NOTE", f"Section '{title}' missing reference(s): {', '.join(refs)}", title))
    return issues


def find_callout_opportunities(text: str, code_spans: List[Tuple[int, int]]) -> List[Issue]:
    issues = []
    reported = set()
    for keyword in CALLOUT_KEYWORDS:
        for match in re.finditer(keyword, text, re.IGNORECASE):
            index = match.start()
            if any(start <= index <= end for start, end in code_spans):
                continue
            line_no = text.count("\n", 0, index) + 1
            if line_no in reported:
                continue
            line_start = text.rfind("\n", 0, index) + 1
            line = text[line_start:text.find("\n", index)]
            if not line.lstrip().startswith(">"):
                issues.append(Issue("NOTE", f"Consider callout for keyword '{keyword}'", f"Line {line_no}"))
                reported.add(line_no)
    return issues


def gather_code_spans(text: str) -> List[Tuple[int, int]]:
    spans = []
    for match in CODE_BLOCK_PATTERN.finditer(text):
        spans.append((match.start(), match.end()))
    return spans


def audit_file(path: Path) -> List[Issue]:
    text = path.read_text(encoding="utf-8")
    issues: List[Issue] = []
    issues.extend(find_duplicate_blocks(text))
    issues.extend(find_oversized_code_blocks(text))
    code_spans = gather_code_spans(text)
    issues.extend(find_verbose_lists(text, code_spans))
    issues.extend(ensure_phase2_table(text))
    issues.extend(ensure_traceability(text))
    issues.extend(find_callout_opportunities(text, code_spans))
    return issues


def main(paths: List[Path]) -> int:
    failures = []
    for path in paths:
        issues = audit_file(path)
        if issues:
            print(f"\n📄 {path}")
            for issue in issues:
                print(f"  {issue}")
            failures.extend(issue for issue in issues if issue.severity == "WARN")
    if failures:
        print("\n❌ Documentation quality audit found warnings.")
        return 1
    print("✅ Documentation quality audit passed.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit documentation quality")
    parser.add_argument("paths", nargs="*", type=Path, default=[Path("prds/STD-data-architecture-impl-v1.0.md")])
    args = parser.parse_args()
    exit(main(args.paths))
