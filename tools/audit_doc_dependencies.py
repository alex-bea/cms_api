#!/usr/bin/env python3
"""Audit master catalog dependency graph vs. tables."""

from __future__ import annotations

import re
import sys
from typing import Dict, Set

from tools.shared.logging_utils import (
    AuditIssue,
    count_by_severity,
    emit_issues,
    exit_code_from_issues,
    get_logger,
)
from tools.shared.prd_helpers import MASTER_DOC_NAME, read_master_catalog

MERMAID_BLOCK = re.compile(r"```mermaid\n(.*?)\n```", re.DOTALL)
NODE_PATTERN = re.compile(r"\[(.*?)\]")

CATEGORY_HEADERS = {
    "Standards": "`STD-",
    "Reference": "`REF-",
    "Products": "`PRD-",
    "Runbooks": "`RUN-",
}


def parse_tables(text: str) -> Dict[str, Set[str]]:
    sections: Dict[str, Set[str]] = {k: set() for k in CATEGORY_HEADERS}
    current = None
    for line in text.splitlines():
        if line.startswith("## "):
            heading = line.strip("# ").strip()
            current = None
            heading_lc = heading.lower()
            for section in CATEGORY_HEADERS:
                key = section.lower()
                alt = key[:-1] if key.endswith('s') else key
                if key in heading_lc or alt in heading_lc:
                    current = section
                    break
            continue
        if current and line.startswith("|") and "`" in line:
            cells = [cell.strip() for cell in line.strip().split("|")]
            if len(cells) >= 2 and cells[1].startswith("`"):
                name = cells[1].strip("` ")
                base = name.split("-prd-", 1)[0]
                sections[current].add(base)
    return sections


def parse_graph_nodes(text: str) -> Dict[str, Set[str]]:
    sections = {k: set() for k in CATEGORY_HEADERS}
    block = MERMAID_BLOCK.search(text)
    if not block:
        raise ValueError("Mermaid dependency graph not found.")
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("%%"):
            continue
        if line.startswith("subgraph"):
            current = None
            for section in CATEGORY_HEADERS:
                if section in line:
                    current = section
                    break
            continue
        match = NODE_PATTERN.search(line)
        if match and current:
            name = match.group(1)
            name = name.split("-prd", 1)[0]
            sections[current].add(name)
    for section in sections:
        sections[section] = {n.strip('`') for n in sections[section]}
        sections[section] = {n.replace('`', '').strip() for n in sections[section]}
    return sections


def main() -> int:
    logger = get_logger("audit.doc_dependencies")
    try:
        text = read_master_catalog()
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    table_docs = parse_tables(text)
    graph_docs = parse_graph_nodes(text)

    issues: list[AuditIssue] = []
    for section in CATEGORY_HEADERS:
        stale = graph_docs.get(section, set()) - table_docs[section]
        if stale:
            for name in sorted(stale):
                issues.append(
                    AuditIssue(
                        "error",
                        f"Dependency graph lists extra {section} node not in tables",
                        doc=name,
                    )
                )

    if not issues:
        logger.info("Dependency graph audit passed.")
        return 0

    emit_issues(logger, issues)
    counts = count_by_severity(issues)
    logger.error(
        "Dependency graph audit failed (%s errors).",
        counts.get("error", 0),
    )
    return exit_code_from_issues(issues)


if __name__ == '__main__':
    sys.exit(main())
