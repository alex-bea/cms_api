#!/usr/bin/env python3
"""Audit master catalog dependency graph vs. tables."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, Set

MASTER = Path("prds/DOC-master-catalog_prd_v1.0.md")
TABLE_PATTERN = re.compile(r"^\| `([A-Z]{3}-[a-z0-9\-]+)_prd_v[0-9]+\.[0-9]+\.md`")
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
                base = name.split("_prd_", 1)[0]
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
            name = name.split("_prd", 1)[0]
            sections[current].add(name)
    for section in sections:
        sections[section] = {n.strip('`') for n in sections[section]}
        sections[section] = {n.replace('`', '').strip() for n in sections[section]}
    return sections


def main() -> int:
    if not MASTER.exists():
        print("Master catalog not found.", file=sys.stderr)
        return 1

    text = MASTER.read_text(encoding="utf-8")
    table_docs = parse_tables(text)
    graph_docs = parse_graph_nodes(text)

    issues = False
    for section in CATEGORY_HEADERS:
        stale = graph_docs.get(section, set()) - table_docs[section]
        if stale:
            issues = True
            print(f"[ERROR] Dependency graph lists extra {section} nodes not in tables:")
            for name in sorted(stale):
                print(f"  - {name}")

    if issues:
        return 1

    print("Dependency graph audit passed.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
