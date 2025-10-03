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
from typing import List, Tuple

from tools.shared.logging_utils import (
    AuditIssue,
    count_by_severity,
    emit_issues,
    exit_code_from_issues,
    get_logger,
)
from tools.shared.prd_helpers import (
    MASTER_DOC_NAME,
    PRDS_DIR,
    get_prd_names,
    read_master_catalog,
    read_path_text,
    read_prd_text,
)

MASTER_LINK = MASTER_DOC_NAME
EXEMPT_FROM_MASTER_LINK = {MASTER_DOC_NAME}

DOC_PATTERN = re.compile(r"`([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)`")
OLD_PRD_PATTERN = re.compile(r"_prd_v")
CORRECT_PRD_PATTERN = re.compile(r"-prd-v")
PRD_REFERENCE_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)")


def extract_master_entries(text: str) -> set[str]:
    return set(DOC_PATTERN.findall(text))


def docs_missing_master_link(docs: set[str]) -> list[str]:
    missing = []
    for name in sorted(docs):
        if name in EXEMPT_FROM_MASTER_LINK:
            continue
        if MASTER_LINK not in read_prd_text(name):
            missing.append(name)
    return missing


def check_old_prd_references() -> List[Tuple[str, str]]:
    """Check for old underscore format PRD references."""
    violations = []
    
    # Check PRD files
    for prd_file in PRDS_DIR.glob("*.md"):
        content = read_path_text(prd_file)
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(prd_file), "Contains old _prd_v format"))
    
    # Check README.md
    readme_file = Path("README.md")
    if readme_file.exists():
        content = read_path_text(readme_file)
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(readme_file), "Contains old _prd_v format"))
    
    # Check YAML files (excluding backup files and tools directory)
    for yaml_file in Path(".").glob("*.yml"):
        if "tools/" in str(yaml_file) or yaml_file.name.endswith(".bak"):
            continue
        try:
            content = read_path_text(yaml_file)
            if OLD_PRD_PATTERN.search(content):
                violations.append((str(yaml_file), "Contains old _prd_v format"))
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    
    for yaml_file in Path(".").glob("*.yaml"):
        if "tools/" in str(yaml_file) or yaml_file.name.endswith(".bak"):
            continue
        try:
            content = read_path_text(yaml_file)
            if OLD_PRD_PATTERN.search(content):
                violations.append((str(yaml_file), "Contains old _prd_v format"))
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    
    # Check GitHub workflows
    workflows_dir = Path(".github/workflows")
    if workflows_dir.exists():
        for workflow_file in workflows_dir.glob("*.yml"):
            try:
                content = read_path_text(workflow_file)
                if OLD_PRD_PATTERN.search(content):
                    violations.append((str(workflow_file), "Contains old _prd_v format"))
            except (UnicodeDecodeError, FileNotFoundError):
                continue
        
        for workflow_file in workflows_dir.glob("*.yaml"):
            try:
                content = read_path_text(workflow_file)
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
    
    content = read_path_text(readme_file)
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
        content = read_path_text(precommit_file)
        # Check if regex pattern uses old format
        if OLD_PRD_PATTERN.search(content):
            violations.append((str(precommit_file), "Pre-commit regex uses old _prd_v format"))
        # Check if regex pattern uses correct format
        if not CORRECT_PRD_PATTERN.search(content):
            violations.append((str(precommit_file), "Pre-commit regex missing -prd-v pattern"))
    
    return violations


def main() -> int:
    logger = get_logger("audit.doc_catalog")
    issues: List[AuditIssue] = []

    try:
        master_text = read_master_catalog()
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    actual_docs = get_prd_names()
    catalog_docs = extract_master_entries(master_text)

    for name in sorted(actual_docs - catalog_docs):
        issues.append(AuditIssue("error", "Missing from master catalog", doc=name))

    for name in sorted(catalog_docs - actual_docs):
        issues.append(AuditIssue("error", "Catalog entry has no matching file", doc=name))

    for name in docs_missing_master_link(actual_docs):
        issues.append(AuditIssue("error", "Missing backlink to master catalog", doc=name))

    for file_path, description in check_old_prd_references():
        issues.append(AuditIssue("error", description, doc=file_path))

    for file_path, description in check_readme_prd_references():
        issues.append(AuditIssue("error", description, doc=file_path))

    for file_path, description in check_yaml_prd_patterns():
        issues.append(AuditIssue("error", description, doc=file_path))

    if not issues:
        logger.info("Documentation catalog audit passed.")
        return 0

    emit_issues(logger, issues)
    counts = count_by_severity(issues)
    logger.error(
        "Documentation catalog audit failed (%s errors).",
        counts.get("error", 0),
    )
    return exit_code_from_issues(issues)


if __name__ == "__main__":
    sys.exit(main())
