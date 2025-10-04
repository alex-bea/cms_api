#!/usr/bin/env python3
"""Advanced cross-reference validation for PRD documents.

This tool provides detailed analysis of cross-references between documents,
including forward/backward symmetry, integration points, and reference completeness.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

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

# Pattern to find section headers like "## 3. QA Lifecycle & Architecture"
SECTION_PATTERN = re.compile(r"^##\s+([0-9\.]+)\s+(.+)$", re.MULTILINE)


def build_reference_map() -> Dict[str, Set[str]]:
    """Build a comprehensive map of document references."""
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


def extract_sections(text: str) -> Dict[str, str]:
    """Extract section numbers and titles from document text."""
    sections = {}
    for match in SECTION_PATTERN.findall(text):
        section_num, section_title = match
        sections[section_num] = section_title.strip()
    return sections


def validate_symmetric_references(reference_map: Dict[str, Set[str]]) -> List[AuditIssue]:
    """Validate that important cross-references are symmetric."""
    issues = []
    
    # Define pairs that should have symmetric references
    symmetric_pairs = {
        ("DOC-test-patterns-prd-v1.0.md", "STD-qa-testing-prd-v1.0.md"),
        ("DOC-test-patterns-prd-v1.0.md", "RUN-global-operations-prd-v1.0.md"),
        # Add more pairs as needed
    }
    
    for source_doc, referenced_docs in reference_map.items():
        for referenced_doc in referenced_docs:
            is_symmetric_pair = (
                (source_doc, referenced_doc) in symmetric_pairs or
                (referenced_doc, source_doc) in symmetric_pairs
            )
            
            if is_symmetric_pair and referenced_doc in reference_map:
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


def validate_integration_points(reference_map: Dict[str, Set[str]]) -> List[AuditIssue]:
    """Validate that declared integration points exist in target documents."""
    issues = []
    docs = get_prd_names()
    
    for path in sorted(iter_prd_paths()):
        text = read_path_text(path)
        integration_matches = INTEGRATION_PATTERN.findall(text)
        
        for match in integration_matches:
            source_prefix, source_section, target_prefix, target_name, target_section = match
            
            # Find the target document
            target_doc = None
            for doc_name in docs:
                if doc_name.startswith(target_prefix) and target_name.lower().replace(" ", "-") in doc_name.lower():
                    target_doc = doc_name
                    break
            
            if target_doc and Path(PRDS_DIR / target_doc).exists():
                target_text = read_path_text(PRDS_DIR / target_doc)
                target_sections = extract_sections(target_text)
                
                if target_section not in target_sections:
                    issues.append(
                        AuditIssue(
                            "warning",
                            f"Integration point target section {target_section} not found in {target_doc}",
                            doc=path.name,
                        )
                    )
    
    return issues


def analyze_reference_patterns(reference_map: Dict[str, Set[str]]) -> Dict[str, any]:
    """Analyze patterns in cross-references."""
    analysis = {
        "total_docs": len(reference_map),
        "total_references": sum(len(refs) for refs in reference_map.values()),
        "most_referenced": [],
        "least_referenced": [],
        "reference_clusters": defaultdict(list),
    }
    
    # Count incoming references
    incoming_refs = defaultdict(int)
    for refs in reference_map.values():
        for ref in refs:
            incoming_refs[ref] += 1
    
    # Find most and least referenced documents
    sorted_refs = sorted(incoming_refs.items(), key=lambda x: x[1], reverse=True)
    analysis["most_referenced"] = sorted_refs[:5]
    analysis["least_referenced"] = [doc for doc, count in sorted_refs if count == 0 and doc != MASTER_DOC_NAME]
    
    # Find reference clusters (documents that reference each other)
    for doc, refs in reference_map.items():
        for ref in refs:
            if ref in reference_map and doc in reference_map[ref]:
                cluster_key = tuple(sorted([doc, ref]))
                analysis["reference_clusters"][cluster_key].append((doc, ref))
    
    return analysis


def generate_reference_report(reference_map: Dict[str, Set[str]]) -> str:
    """Generate a detailed reference report."""
    analysis = analyze_reference_patterns(reference_map)
    
    report = []
    report.append("# Cross-Reference Analysis Report")
    report.append("")
    
    report.append("## Summary")
    report.append(f"- Total documents: {analysis['total_docs']}")
    report.append(f"- Total references: {analysis['total_references']}")
    report.append("")
    
    report.append("## Most Referenced Documents")
    for doc, count in analysis["most_referenced"]:
        report.append(f"- `{doc}`: {count} references")
    report.append("")
    
    report.append("## Least Referenced Documents")
    for doc in analysis["least_referenced"]:
        report.append(f"- `{doc}`: 0 references")
    report.append("")
    
    report.append("## Reference Clusters (Mutual References)")
    for cluster, pairs in analysis["reference_clusters"].items():
        if pairs:
            report.append(f"- `{cluster[0]}` ↔ `{cluster[1]}`")
    report.append("")
    
    report.append("## Detailed Reference Map")
    for doc in sorted(reference_map.keys()):
        refs = reference_map[doc]
        if refs:
            report.append(f"### {doc}")
            for ref in sorted(refs):
                report.append(f"- References: `{ref}`")
            report.append("")
    
    return "\n".join(report)


def main() -> int:
    logger = get_logger("audit.cross_references")
    issues: List[AuditIssue] = []
    
    # Build reference map
    reference_map = build_reference_map()
    
    # Validate symmetric references
    symmetry_issues = validate_symmetric_references(reference_map)
    issues.extend(symmetry_issues)
    
    # Validate integration points
    integration_issues = validate_integration_points(reference_map)
    issues.extend(integration_issues)
    
    # Generate report
    report = generate_reference_report(reference_map)
    
    # Output report to file
    report_file = Path("cross_reference_report.md")
    report_file.write_text(report)
    logger.info(f"Cross-reference report written to {report_file}")
    
    # Handle issues
    error_issues = [issue for issue in issues if issue.severity == "error"]
    warning_issues = [issue for issue in issues if issue.severity == "warning"]
    
    if warning_issues:
        emit_issues(logger, warning_issues)
        counts = count_by_severity(warning_issues)
        logger.warning("Cross-reference audit warnings (%s warnings).", counts.get("warning", 0))
    
    if not error_issues:
        if warning_issues:
            logger.info("Cross-reference audit passed with warnings.")
        else:
            logger.info("Cross-reference audit passed.")
        return 0
    
    emit_issues(logger, error_issues)
    counts = count_by_severity(error_issues)
    logger.error("Cross-reference audit failed (%s errors).", counts.get("error", 0))
    return exit_code_from_issues(error_issues)


if __name__ == "__main__":
    sys.exit(main())
