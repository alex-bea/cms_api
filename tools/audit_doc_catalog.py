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

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

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
    classify_doc,
    get_prd_names,
    read_master_catalog,
    read_path_text,
    read_prd_text,
)

MASTER_LINK = MASTER_DOC_NAME
EXEMPT_FROM_MASTER_LINK = {MASTER_DOC_NAME}

DOC_PATTERN = re.compile(r"`([A-Z]{3,4}-[a-z0-9\-]+(?:-(?:prd|impl))?(?:-v[0-9]+\.[0-9]+(?:-[A-Z]+)?)?\.md)`")
OLD_PRD_PATTERN = re.compile(r"_prd_v")
CORRECT_PRD_PATTERN = re.compile(r"-prd-v")
PRD_REFERENCE_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+(?:-[A-Z]+)?)?\.md)")
IMPL_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+)-impl-v[0-9]+\.[0-9]+\.md")
PRD_SLUG_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+)-prd-v[0-9]+\.[0-9]+(?:-[A-Z]+)?\.md")

REQUIRED_REFERENCES = {
    "PRD-mpfs-prd-v1.0.md": ["REF-cms-pricing-source-map-prd-v1.0.md"],
    "PRD-rvu-gpci-prd-v0.1.md": ["REF-cms-pricing-source-map-prd-v1.0.md"],
    "PRD-opps-prd-v1.0.md": ["REF-cms-pricing-source-map-prd-v1.0.md"],
    "PRD-geography-locality-mapping-prd-v1.0.md": ["REF-geography-source-map-prd-v1.0.md"],
}


@dataclass(frozen=True)
class CatalogSection:
    heading: str
    stub_builder: Callable[[str], str]


CATALOG_SECTIONS: Dict[str, CatalogSection] = {
    "STD": CatalogSection(
        heading="## 1. Architectural Standards (`STD-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Status TBD | Owner TBD | TBD | Auto-added placeholder (pending metadata) |"
        ),
    ),
    "REF": CatalogSection(
        heading="## 2. Reference Architectures (`REF-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Status TBD | Owner TBD | Auto-added placeholder (pending metadata) |"
        ),
    ),
    "PRD": CatalogSection(
        heading="## 3. Product & Dataset PRDs (`PRD-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Status TBD | Owner TBD | TBD | TBD |"
        ),
    ),
    "RUN": CatalogSection(
        heading="## 4. Operational Runbooks (`RUN-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Status TBD | Owner TBD | Scope TBD |"
        ),
    ),
    "DOC": CatalogSection(
        heading="## 5. Documentation & Meta (`DOC-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Status TBD | Owner TBD | Purpose TBD |"
        ),
    ),
    "SRC": CatalogSection(
        heading="## 6. Source Descriptors (`SRC-*`)",
        stub_builder=lambda doc: (
            f"| `{doc}` | Source TBD | Owner TBD | Status TBD | Parser TBD | Auto-added placeholder (pending metadata) |"
        ),
    ),
}

MASTER_BACKLINK_BULLET = "- `prds/DOC-master-catalog-prd-v1.0.md`"
CROSS_REF_HEADER = "**Cross-References:**"
CHANGE_LOG_HEADING = "## 9. Change Log"
CHANGE_LOG_SUMMARY_PREFIX = "Auto-added catalog rows for"


@dataclass
class AuditState:
    master_text: str
    issues: List[AuditIssue]
    missing_catalog: List[str]
    missing_backlinks: List[str]
    actual_docs: Set[str]
    catalog_docs: Set[str]


@dataclass
class CatalogUpdateResult:
    updated_text: Optional[str]
    inserted_docs: List[str]
    skipped_docs: List[str]
    change_log_updated: bool


@dataclass
class FixReport:
    catalog_updates: List[str]
    backlink_updates: List[str]
    change_log_updated: bool
    skipped_catalog: List[str]
    skipped_backlinks: List[str]

    def has_changes(self) -> bool:
        return bool(self.catalog_updates or self.backlink_updates or self.change_log_updated)

    def summary_lines(self) -> List[str]:
        lines: List[str] = []
        fmt = lambda docs: ", ".join(f"`{doc}`" for doc in sorted(dict.fromkeys(docs)))
        if self.catalog_updates:
            lines.append(f"Inserted master catalog rows for: {fmt(self.catalog_updates)}")
        if self.backlink_updates:
            lines.append(f"Added master catalog backlink to: {fmt(self.backlink_updates)}")
        if self.change_log_updated:
            lines.append("Recorded auto-fix entry in change log.")
        if self.skipped_catalog:
            lines.append(f"Skipped catalog auto-insert for: {fmt(self.skipped_catalog)}")
        if self.skipped_backlinks:
            lines.append(f"Skipped backlink fix (write conflict or read error) for: {fmt(self.skipped_backlinks)}")
        return lines


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


def check_required_references() -> List[Tuple[str, str]]:
    """Ensure key PRDs reference mandatory REF documents."""
    violations: List[Tuple[str, str]] = []

    for prd_name, required_refs in REQUIRED_REFERENCES.items():
        prd_path = PRDS_DIR / prd_name
        if not prd_path.exists():
            violations.append((prd_name, "PRD file missing while enforcing required references"))
            continue

        content = read_path_text(prd_path)
        for ref in required_refs:
            if ref not in content:
                violations.append((prd_name, f"Missing required reference to {ref}"))

    return violations


def check_companion_document_compliance() -> List[Tuple[str, str]]:
    """Validate companion document naming and cross-references."""
    violations = []
    
    # Find all -impl docs
    impl_docs = list(PRDS_DIR.glob("*-impl-v*.md"))
    
    for impl_doc in impl_docs:
        impl_name = impl_doc.name
        
        # Extract slug from impl doc
        match = IMPL_PATTERN.match(impl_name)
        if not match:
            violations.append((impl_name, "Companion doc doesn't match naming pattern"))
            continue
        
        slug = match.group(1)
        
        # Check if corresponding main doc exists
        main_docs = list(PRDS_DIR.glob(f"{slug}-prd-v*.md"))
        if not main_docs:
            violations.append((impl_name, f"Companion doc has no main doc ({slug}-prd-v*.md)"))
            continue
        
        main_doc = main_docs[0]
        main_name = main_doc.name
        
        # Check impl doc references main doc
        impl_content = read_path_text(impl_doc)
        if main_name not in impl_content:
            violations.append((impl_name, f"Companion doc doesn't reference main doc {main_name}"))
        
        # Check main doc references impl doc (warning only)
        main_content = read_path_text(main_doc)
        if impl_name not in main_content:
            # This is a warning, not an error
            violations.append((main_name, f"Main doc should reference companion {impl_name} (warning)"))
    
    return violations


def is_planned_companion(line_content: str) -> bool:
    """Check if companion is marked as planned/future."""
    return bool(re.search(r'_\(.*?planned.*?\)_', line_content, re.IGNORECASE))


def parse_companion_links(line_content: str) -> List[str]:
    """Extract companion doc links from markdown line, handling multiple formats.
    
    Supports:
    - Single link: [file.md](file.md)
    - Multiple links: [file1.md](file1.md), [file2.md](file2.md)
    - Bare filename: STD-name-impl-v1.0.md
    - List format with bullets
    """
    # Extract all markdown links: [file.md]
    links = re.findall(r'\[([A-Z]{3,4}-[a-z0-9\-]+\.md)\]', line_content)
    
    # Also match bare filenames (backward compat)
    if not links:
        links = re.findall(r'\b([A-Z]{3,4}-[a-z0-9\-]+\.md)\b', line_content)
    
    # Normalize paths (reject external/relative paths)
    normalized = []
    for link in links:
        # Reject external or relative paths
        if '../' in link or link.startswith('http') or '/' in link:
            continue
        normalized.append(link)
    
    return normalized


def validate_case_sensitive_path(doc_name: str, all_docs: Set[str]) -> Optional[str]:
    """Ensure exact case match, even on case-insensitive filesystems (macOS)."""
    if doc_name not in all_docs:
        # Check for case-insensitive match (would work on macOS, break on Linux)
        lower_match = [d for d in all_docs if d.lower() == doc_name.lower()]
        if lower_match:
            return f'Case mismatch: {doc_name} (found: {lower_match[0]}) - will break on Linux CI'
        return f'File not found: {doc_name}'
    return None


def check_companion_links_markdown() -> List[Tuple[str, str]]:
    """Validate bidirectional companion doc links using markdown headers.

    Enhancements (v2):
    - Anchored regex patterns (line start)
    - Handles multiple links (comma-separated)
    - "Planned" exemption
    - Case-sensitive validation
    - Path normalization
    
    Returns list of (doc_name, error_message) tuples.
    """
    issues = []
    all_docs = {f.name for f in PRDS_DIR.glob('*.md')}
    
    # Anchored patterns for header fields
    COMPANION_DOCS_PATTERN = re.compile(
        r'^\s*(?:[-*]\s*)?\*\*Companion Docs:\*\*\s*(.+?)$',
        re.MULTILINE
    )
    COMPANION_OF_PATTERN = re.compile(
        r'^\s*(?:[-*]\s*)?\*\*Companion Of:\*\*\s*(.+?)$',
        re.MULTILINE
    )
    
    for doc_path in PRDS_DIR.glob('*.md'):
        try:
            content = read_path_text(doc_path)
        except Exception:
            continue
        
        # Limit search to first 60 lines (header area)
        header_content = '\n'.join(content.splitlines()[:60])
        doc_name = doc_path.name
        
        # Check forward links (STD -> IMPL) via **Companion Docs:** field
        companion_docs_match = COMPANION_DOCS_PATTERN.search(header_content)
        if companion_docs_match:
            line_content = companion_docs_match.group(1)
            
            # Skip validation if marked as planned
            if is_planned_companion(line_content):
                continue
            
            # Parse companion links
            companions = parse_companion_links(line_content)
            for companion in companions:
                # Case-sensitive validation
                if error := validate_case_sensitive_path(companion, all_docs):
                    issues.append((doc_name, error))
                    continue
                
                # Check reverse link exists
                try:
                    companion_content = read_path_text(PRDS_DIR / companion)
                    companion_header = '\n'.join(companion_content.splitlines()[:60])
                    
                    # Verify reverse link
                    if COMPANION_OF_PATTERN.search(companion_header):
                        if doc_name not in companion_header:
                            issues.append((
                                doc_name,
                                f'Broken bidirectional link: {companion} does not reference {doc_name} in **Companion Of:**'
                            ))
                except Exception:
                    pass
        
        # Check reverse links (IMPL -> STD) via **Companion Of:** field
        companion_of_match = COMPANION_OF_PATTERN.search(header_content)
        if companion_of_match:
            line_content = companion_of_match.group(1)
            
            # Parse parent links
            parents = parse_companion_links(line_content)
            for parent in parents:
                # Case-sensitive validation
                if error := validate_case_sensitive_path(parent, all_docs):
                    issues.append((doc_name, error))
    
    return issues


def extract_doc_from_row(line: str) -> str:
    match = re.search(r"`([^`]+)`", line)
    if match:
        return match.group(1)
    cell = line.strip().strip("|").split("|", 1)[0].strip()
    return cell


def find_table_data_range(lines: List[str], heading: str) -> Optional[Tuple[int, int]]:
    try:
        heading_idx = lines.index(heading)
    except ValueError:
        return None

    header_idx: Optional[int] = None
    for idx in range(heading_idx + 1, len(lines)):
        if lines[idx].startswith("|"):
            header_idx = idx
            break

    if header_idx is None:
        return None

    data_start = header_idx + 2  # skip header row + separator row
    data_end = data_start
    while data_end < len(lines) and lines[data_end].startswith("|"):
        data_end += 1

    return data_start, data_end


def add_change_log_entry(lines: List[str], docs: List[str]) -> bool:
    if not docs:
        return False

    try:
        heading_idx = lines.index(CHANGE_LOG_HEADING)
    except ValueError:
        return False

    header_idx: Optional[int] = None
    for idx in range(heading_idx + 1, len(lines)):
        if lines[idx].startswith("|"):
            header_idx = idx
            break

    if header_idx is None:
        return False

    data_start = header_idx + 2
    data_end = data_start
    while data_end < len(lines) and lines[data_end].startswith("|"):
        data_end += 1

    summary = f"{CHANGE_LOG_SUMMARY_PREFIX} {', '.join(f'`{doc}`' for doc in docs)} via audit_doc_catalog.py --fix"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_row = f"| TBD | {timestamp} | {summary} | #TBD |"

    for idx in range(data_start, data_end):
        if summary in lines[idx]:
            return False

    lines.insert(data_start, new_row)
    return True


def update_master_catalog_text(
    master_text: str, missing_docs: List[str], logger
) -> CatalogUpdateResult:
    if not missing_docs:
        return CatalogUpdateResult(updated_text=None, inserted_docs=[], skipped_docs=[], change_log_updated=False)

    lines = master_text.splitlines()
    trailing_newline = master_text.endswith("\n")

    inserted: List[str] = []
    skipped: List[str] = []

    for doc in missing_docs:
        doc_type = classify_doc(doc)
        section = CATALOG_SECTIONS.get(doc_type)
        if not section:
            skipped.append(doc)
            logger.warning(
                "No catalog section mapping for doc `%s` (type `%s`); skipping auto-insert.",
                doc,
                doc_type,
            )
            continue

        range_result = find_table_data_range(lines, section.heading)
        if range_result is None:
            skipped.append(doc)
            logger.warning(
                "Unable to locate table for section `%s`; skipping auto-insert for `%s`.",
                section.heading,
                doc,
            )
            continue

        data_start, data_end = range_result
        data_lines = lines[data_start:data_end]

        existing_docs = {extract_doc_from_row(row) for row in data_lines}
        if doc in existing_docs:
            continue

        data_lines.append(section.stub_builder(doc))
        data_lines.sort(key=lambda row: extract_doc_from_row(row).lower())
        lines[data_start:data_end] = data_lines
        data_end = data_start + len(data_lines)
        inserted.append(doc)

    if not inserted:
        return CatalogUpdateResult(updated_text=None, inserted_docs=[], skipped_docs=skipped, change_log_updated=False)

    change_log_updated = add_change_log_entry(lines, inserted)
    new_text = "\n".join(lines)
    if trailing_newline and not new_text.endswith("\n"):
        new_text += "\n"

    return CatalogUpdateResult(
        updated_text=new_text,
        inserted_docs=inserted,
        skipped_docs=skipped,
        change_log_updated=change_log_updated,
    )


def find_cross_ref_insert_index(lines: List[str]) -> int:
    for idx, line in enumerate(lines):
        if line.strip() == "---":
            return idx
    for idx, line in enumerate(lines):
        if line.strip() == "":
            return idx
    return len(lines)


def ensure_master_backlink(text: str) -> Optional[str]:
    if MASTER_DOC_NAME in text:
        return None

    lines = text.splitlines()
    trailing_newline = text.endswith("\n")

    for idx, line in enumerate(lines):
        if line.strip() == CROSS_REF_HEADER:
            insert_idx = idx + 1
            while insert_idx < len(lines) and lines[insert_idx].lstrip().startswith(("-", "*")):
                insert_idx += 1
            lines.insert(insert_idx, MASTER_BACKLINK_BULLET)
            new_text = "\n".join(lines)
            if trailing_newline and not new_text.endswith("\n"):
                new_text += "\n"
            return new_text

    insert_idx = find_cross_ref_insert_index(lines)
    block = [CROSS_REF_HEADER, MASTER_BACKLINK_BULLET, ""]
    if insert_idx > 0 and lines[insert_idx - 1].strip():
        block.insert(0, "")
    lines[insert_idx:insert_idx] = block
    new_text = "\n".join(lines)
    if trailing_newline and not new_text.endswith("\n"):
        new_text += "\n"
    return new_text


def write_with_backup(path: Path, original_text: str, new_text: str) -> Path:
    backup_path = path.with_suffix(path.suffix + ".bak")
    if backup_path.exists():
        raise RuntimeError(f"Backup already exists: {backup_path}")

    backup_path.write_text(original_text, encoding="utf-8")
    path.write_text(new_text, encoding="utf-8")
    return backup_path


def perform_audit() -> AuditState:
    master_text = read_master_catalog()
    actual_docs = get_prd_names()
    catalog_docs = extract_master_entries(master_text)

    missing_catalog = sorted(actual_docs - catalog_docs)
    missing_backlinks = docs_missing_master_link(actual_docs)

    issues: List[AuditIssue] = []

    for name in missing_catalog:
        issues.append(AuditIssue("error", "Missing from master catalog", doc=name))

    for name in sorted(catalog_docs - actual_docs):
        issues.append(AuditIssue("error", "Catalog entry has no matching file", doc=name))

    for name in missing_backlinks:
        issues.append(AuditIssue("error", "Missing backlink to master catalog", doc=name))

    for file_path, description in check_old_prd_references():
        issues.append(AuditIssue("error", description, doc=file_path))

    for file_path, description in check_readme_prd_references():
        issues.append(AuditIssue("error", description, doc=file_path))

    for file_path, description in check_yaml_prd_patterns():
        issues.append(AuditIssue("error", description, doc=file_path))

    for doc, description in check_required_references():
        issues.append(AuditIssue("error", description, doc=doc))

    for doc, description in check_companion_document_compliance():
        if "(warning)" in description:
            issues.append(AuditIssue("warning", description, doc=doc))
        else:
            issues.append(AuditIssue("error", description, doc=doc))

    for doc, description in check_companion_links_markdown():
        if "(warning)" in description:
            issues.append(AuditIssue("warning", description, doc=doc))
        else:
            issues.append(AuditIssue("error", description, doc=doc))

    return AuditState(
        master_text=master_text,
        issues=issues,
        missing_catalog=missing_catalog,
        missing_backlinks=missing_backlinks,
        actual_docs=actual_docs,
        catalog_docs=catalog_docs,
    )


def apply_fixes(state: AuditState, logger) -> FixReport:
    report = FixReport(
        catalog_updates=[],
        backlink_updates=[],
        change_log_updated=False,
        skipped_catalog=[],
        skipped_backlinks=[],
    )

    master_result = update_master_catalog_text(state.master_text, state.missing_catalog, logger)
    if master_result.skipped_docs:
        report.skipped_catalog.extend(master_result.skipped_docs)

    if master_result.updated_text is not None:
        master_path = PRDS_DIR / MASTER_DOC_NAME
        try:
            write_with_backup(master_path, state.master_text, master_result.updated_text)
        except RuntimeError as exc:
            report.skipped_catalog.extend(master_result.inserted_docs)
            logger.warning("Skipping catalog update for `%s`: %s", MASTER_DOC_NAME, exc)
        else:
            report.catalog_updates.extend(master_result.inserted_docs)
            report.change_log_updated = master_result.change_log_updated

    for doc in state.missing_backlinks:
        doc_path = PRDS_DIR / doc
        try:
            original_text = read_prd_text(doc)
        except FileNotFoundError:
            report.skipped_backlinks.append(doc)
            logger.warning("Unable to read `%s` for backlink fix (file missing).", doc)
            continue

        new_text = ensure_master_backlink(original_text)
        if not new_text or new_text == original_text:
            continue

        try:
            write_with_backup(doc_path, original_text, new_text)
            report.backlink_updates.append(doc)
        except RuntimeError as exc:
            report.skipped_backlinks.append(doc)
            logger.warning("Skipping backlink fix for `%s`: %s", doc, exc)

    return report


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit documentation catalog consistency.")
    parser.add_argument(
        "--fix",
        "--fix-missing",
        dest="fix",
        action="store_true",
        help="Automatically insert missing catalog entries and master backlinks (with backups).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logger = get_logger("audit.doc_catalog")

    try:
        state = perform_audit()
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    if args.fix:
        report = apply_fixes(state, logger)

        summary_lines = report.summary_lines()
        if report.has_changes():
            logger.info("Applied auto-fixes:")
            for line in summary_lines:
                logger.info(" - %s", line)
        else:
            if summary_lines:
                logger.info("Auto-fix review:")
                for line in summary_lines:
                    logger.info(" - %s", line)
            else:
                logger.info("No auto-fix changes required.")

        try:
            state = perform_audit()
        except FileNotFoundError as exc:
            logger.error(str(exc))
            return 1

        if state.issues:
            emit_issues(logger, state.issues)
            counts = count_by_severity(state.issues)
            logger.error(
                "Documentation catalog audit failed after attempted fixes (%s errors).",
                counts.get("error", 0),
            )
            return exit_code_from_issues(state.issues)

        logger.info("Documentation catalog audit passed after applying fixes.")
        return 0

    if not state.issues:
        logger.info("Documentation catalog audit passed.")
        return 0

    emit_issues(logger, state.issues)
    counts = count_by_severity(state.issues)
    logger.error(
        "Documentation catalog audit failed (%s errors).",
        counts.get("error", 0),
    )
    return exit_code_from_issues(state.issues)


if __name__ == "__main__":
    sys.exit(main())
