#!/usr/bin/env python3
"""Comprehensive consistency audit for documentation catalog and PRD references.

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
4. All PRD references use correct hyphenated format (-prd-v).
5. README.md references point to existing PRD files.
6. YAML files (pre-commit, workflows) use correct PRD naming patterns.
7. No old underscore format (_prd_v) references remain.

Exit code 0 = all good; otherwise prints issues and exits 1.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Set, List, Tuple

PRDS_DIR = Path("prds")
MASTER_DOC = PRDS_DIR / "DOC-master-catalog-prd-v1.0.md"
MASTER_LINK = "DOC-master-catalog-prd-v1.0.md"
EXEMPT_FROM_MASTER_LINK = {MASTER_DOC.name}

DOC_PATTERN = re.compile(r"`([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)`")
OLD_PRD_PATTERN = re.compile(r"_prd_v")
CORRECT_PRD_PATTERN = re.compile(r"-prd-v")
PRD_REFERENCE_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)")


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


def check_old_prd_references() -> List[Tuple[str, str]]:
    """Check for old underscore format PRD references."""
    violations = []
    
    # Check PRD files
    for prd_file in PRDS_DIR.glob("*.md"):
        content = prd_file.read_text(encoding="utf-8")
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(prd_file), "Contains old _prd_v format"))
    
    # Check README.md
    readme_file = Path("README.md")
    if readme_file.exists():
        content = readme_file.read_text(encoding="utf-8")
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(readme_file), "Contains old _prd_v format"))
    
    # Check YAML files (excluding backup files and tools directory)
    for yaml_file in Path(".").glob("*.yml"):
        if "tools/" in str(yaml_file) or yaml_file.name.endswith(".bak"):
            continue
        try:
            content = yaml_file.read_text(encoding="utf-8")
            if OLD_PRD_PATTERN.search(content):
                violations.append((str(yaml_file), "Contains old _prd_v format"))
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    
    for yaml_file in Path(".").glob("*.yaml"):
        if "tools/" in str(yaml_file) or yaml_file.name.endswith(".bak"):
            continue
        try:
            content = yaml_file.read_text(encoding="utf-8")
            if OLD_PRD_PATTERN.search(content):
                violations.append((str(yaml_file), "Contains old _prd_v format"))
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    
    # Check GitHub workflows
    workflows_dir = Path(".github/workflows")
    if workflows_dir.exists():
        for workflow_file in workflows_dir.glob("*.yml"):
            try:
                content = workflow_file.read_text(encoding="utf-8")
                if OLD_PRD_PATTERN.search(content):
                    violations.append((str(workflow_file), "Contains old _prd_v format"))
            except (UnicodeDecodeError, FileNotFoundError):
                continue
        
        for workflow_file in workflows_dir.glob("*.yaml"):
            try:
                content = workflow_file.read_text(encoding="utf-8")
                if OLD_PRD_PATTERN.search(content):
                    violations.append((str(workflow_file), "Contains old _prd_v format"))
            except (UnicodeDecodeError, FileNotFoundError):
                continue
    
    return violations


def check_readme_prd_references() -> List[Tuple[str, str]]:
    """Check README.md PRD references point to existing files."""
    violations = []
    
    readme_file = Path("README.md")
    if not readme_file.exists():
        return violations
    
    content = readme_file.read_text(encoding="utf-8")
    prd_refs = PRD_REFERENCE_PATTERN.findall(content)
    
    for ref in prd_refs:
        ref_path = PRDS_DIR / ref
        if not ref_path.exists():
            violations.append((str(readme_file), f"References non-existent PRD: {ref}"))
    
    return violations


def check_yaml_prd_patterns() -> List[Tuple[str, str]]:
    """Check YAML files use correct PRD naming patterns."""
    violations = []
    
    # Check .pre-commit-config.yaml specifically
    precommit_file = Path(".pre-commit-config.yaml")
    if precommit_file.exists():
        content = precommit_file.read_text(encoding="utf-8")
        # Check if regex pattern uses old format
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(precommit_file), "Pre-commit regex uses old _prd_v format"))
        # Check if regex pattern uses correct format
        if not CORRECT_PRD_PATTERN.search(content):
            violations.append((str(precommit_file), "Pre-commit regex missing -prd-v pattern"))
    
    return violations


def main() -> int:
    if not MASTER_DOC.exists():
        print(f"Master catalog not found at {MASTER_DOC}", file=sys.stderr)
        return 1

    # Original catalog checks
    actual_docs = discover_docs()
    master_text = MASTER_DOC.read_text(encoding="utf-8")
    catalog_docs = extract_master_entries(master_text)

    missing_from_catalog = sorted(actual_docs - catalog_docs)
    stale_in_catalog = sorted(catalog_docs - actual_docs)
    no_master_link = docs_missing_master_link(actual_docs)

    # New comprehensive checks
    old_prd_violations = check_old_prd_references()
    readme_violations = check_readme_prd_references()
    yaml_violations = check_yaml_prd_patterns()

    issues = False

    # Report original issues
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

    # Report new issues
    if old_prd_violations:
        issues = True
        print("[ERROR] Files containing old PRD format (_prd_v):")
        for file_path, description in old_prd_violations:
            print(f"  - {file_path}: {description}")

    if readme_violations:
        issues = True
        print("[ERROR] README.md PRD reference issues:")
        for file_path, description in readme_violations:
            print(f"  - {file_path}: {description}")

    if yaml_violations:
        issues = True
        print("[ERROR] YAML file PRD pattern issues:")
        for file_path, description in yaml_violations:
            print(f"  - {file_path}: {description}")

    if not issues:
        print("Documentation catalog audit passed.")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
