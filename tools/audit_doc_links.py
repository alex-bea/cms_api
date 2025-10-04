#!/usr/bin/env python3
"""Validate that cross-references inside PRDs point to real files and required links exist.
Also validates forward/backward reference symmetry and integration points."""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from typing import Dict, Set, Tuple

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
    iter_prd_paths,
    read_path_text,
)

CODE_REF_PATTERN = re.compile(
    r"`([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)`"
)

# Pattern for cross-reference sections (without backticks)
CROSS_REF_PATTERN = re.compile(
    r"\*\*([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md):?\*\*"
)

# Pattern to find integration points like "QTS Section 3.3 → Test Patterns PRD Section 1"
INTEGRATION_PATTERN = re.compile(
    r"([A-Z]{3,4})\s+Section\s+([0-9\.]+)\s*→\s*([A-Z]{3,4})\s+([A-Za-z\s]+)\s+Section\s+([0-9\.]+)"
)

MANDATORY_REFS = {
    "STD": {MASTER_DOC_NAME},
    "REF": {MASTER_DOC_NAME},
    "PRD": {MASTER_DOC_NAME},
    "RUN": {MASTER_DOC_NAME},
    "DOC": set(),
}


def build_reference_map() -> Dict[str, Set[str]]:
    """Build a map of document references: {doc_name: {referenced_docs}}."""
    reference_map = defaultdict(set)
    docs = get_prd_names()
    
    for path in sorted(iter_prd_paths()):
        text = read_path_text(path)
        # Find both backtick references and cross-reference section references
        refs = set(CODE_REF_PATTERN.findall(text))
        cross_refs = set(CROSS_REF_PATTERN.findall(text))
        all_refs = refs.union(cross_refs)
        # Only include references to existing documents
        valid_refs = all_refs.intersection(docs)
        reference_map[path.name] = valid_refs
    
    return dict(reference_map)


def validate_reference_symmetry(reference_map: Dict[str, Set[str]]) -> list[AuditIssue]:
    """Validate that cross-references are symmetric where expected."""
    issues = []
    
    # Documents that should have symmetric references (both ways)
    symmetric_pairs = {
        ("DOC-test-patterns-prd-v1.0.md", "STD-qa-testing-prd-v1.0.md"),
        ("DOC-test-patterns-prd-v1.0.md", "RUN-global-operations-prd-v1.0.md"),
    }
    
    for source_doc, referenced_docs in reference_map.items():
        for referenced_doc in referenced_docs:
            # Check if this is a symmetric pair that should reference back
            is_symmetric_pair = (
                (source_doc, referenced_doc) in symmetric_pairs or
                (referenced_doc, source_doc) in symmetric_pairs
            )
            
            if is_symmetric_pair:
                if referenced_doc in reference_map:
                    referenced_doc_refs = reference_map[referenced_doc]
                    if source_doc not in referenced_doc_refs:
                        issues.append(
                            AuditIssue(
                                "warning",
                                f"Missing back-reference: {referenced_doc} should reference {source_doc}",
                                doc=source_doc,
                            )
                        )
    
    return issues


def validate_integration_points(reference_map: Dict[str, Set[str]]) -> list[AuditIssue]:
    """Validate that declared integration points exist in both documents."""
    issues = []
    docs = get_prd_names()
    
    for path in sorted(iter_prd_paths()):
        text = read_path_text(path)
        
        # Find integration point declarations
        integration_matches = INTEGRATION_PATTERN.findall(text)
        
        for match in integration_matches:
            source_prefix, source_section, target_prefix, target_name, target_section = match
            
            # Find the target document
            target_doc = None
            for doc_name in docs:
                if doc_name.startswith(target_prefix) and target_name.lower().replace(" ", "-") in doc_name.lower():
                    target_doc = doc_name
                    break
            
            if target_doc and target_doc in reference_map:
                # Check if integration point exists in target document
                target_text = read_path_text(PRDS_DIR / target_doc)
                
                # Look for corresponding integration point in target
                # This is a simplified check - in practice, you might want more sophisticated matching
                if f"Section {target_section}" not in target_text:
                    issues.append(
                        AuditIssue(
                            "warning",
                            f"Integration point target section {target_section} not found in {target_doc}",
                            doc=path.name,
                        )
                    )
    
    return issues


def main() -> int:
    logger = get_logger("audit.doc_links")
    issues: list[AuditIssue] = []
    docs = get_prd_names()

    # Build reference map for advanced validation
    reference_map = build_reference_map()

    # Basic validation: check that references point to existing files
    for path in sorted(iter_prd_paths()):
        text = read_path_text(path)
        # Check both backtick references and cross-reference section references
        refs = set(CODE_REF_PATTERN.findall(text))
        cross_refs = set(CROSS_REF_PATTERN.findall(text))
        all_refs = refs.union(cross_refs)
        for ref in sorted(all_refs - docs):
            issues.append(
                AuditIssue(
                    "error",
                    f"References missing document: {ref}",
                    doc=path.name,
                )
            )

        # Check mandatory references
        category = classify_doc(path.name)
        required = MANDATORY_REFS.get(category, set())
        for req in required:
            if req not in text and path.name != MASTER_DOC_NAME:
                issues.append(
                    AuditIssue(
                        "error",
                        f"Missing required reference to {req}",
                        doc=path.name,
                    )
                )

    # Advanced validation: reference symmetry
    symmetry_issues = validate_reference_symmetry(reference_map)
    issues.extend(symmetry_issues)
    
    # Advanced validation: integration points
    integration_issues = validate_integration_points(reference_map)
    issues.extend(integration_issues)

    # Only fail on errors, not warnings
    error_issues = [issue for issue in issues if issue.severity == "error"]
    warning_issues = [issue for issue in issues if issue.severity == "warning"]
    
    if warning_issues:
        emit_issues(logger, warning_issues)
        counts = count_by_severity(warning_issues)
        logger.warning("Link audit warnings (%s warnings).", counts.get("warning", 0))
    
    if not error_issues:
        if warning_issues:
            logger.info("Link audit passed with warnings.")
        else:
            logger.info("Link audit passed.")
        return 0

    emit_issues(logger, error_issues)
    counts = count_by_severity(error_issues)
    logger.error("Link audit failed (%s errors).", counts.get("error", 0))
    return exit_code_from_issues(error_issues)


if __name__ == "__main__":
    sys.exit(main())
