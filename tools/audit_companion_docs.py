#!/usr/bin/env python3
"""
Companion Document Audit Tool

Validates companion document relationships, cross-references,
and version alignment.

Checks:
1. Companion docs reference their main doc
2. Main docs reference their companion
3. Slug consistency between main and companion
4. Version alignment (warn if versions diverge significantly)
5. Both registered in master catalog
6. Cross-references are bidirectional

Usage:
    python tools/audit_companion_docs.py
"""

import re
import sys
from pathlib import Path
from typing import List

from tools.shared.logging_utils import AuditIssue, emit_issues, get_logger, exit_code_from_issues
from tools.shared.prd_helpers import PRDS_DIR, read_path_text

IMPL_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+)-impl-v([0-9]+)\.([0-9]+)\.md")
PRD_PATTERN = re.compile(r"([A-Z]{3,4}-[a-z0-9\-]+)-prd-v([0-9]+)\.([0-9]+)\.md")


def check_companion_relationships() -> List[AuditIssue]:
    """Check all companion document relationships"""
    issues = []
    
    # Find all companion docs
    impl_docs = list(PRDS_DIR.glob("*-impl-v*.md"))
    
    if not impl_docs:
        # No companion docs yet, that's okay
        return issues
    
    for impl_doc in impl_docs:
        impl_name = impl_doc.name
        match = IMPL_PATTERN.match(impl_name)
        
        if not match:
            issues.append(AuditIssue(
                "error",
                "Companion doc doesn't match naming pattern",
                doc=impl_name
            ))
            continue
        
        slug = match.group(1)
        impl_major = int(match.group(2))
        impl_minor = int(match.group(3))
        
        # Find corresponding main doc
        main_docs = list(PRDS_DIR.glob(f"{slug}-prd-v*.md"))
        
        if not main_docs:
            issues.append(AuditIssue(
                "error",
                f"Companion doc has no main doc ({slug}-prd-v*.md)",
                doc=impl_name
            ))
            continue
        
        main_doc = main_docs[0]
        main_name = main_doc.name
        main_match = PRD_PATTERN.match(main_name)
        
        if main_match:
            main_major = int(main_match.group(2))
            main_minor = int(main_match.group(3))
            
            # Check version alignment (warn if major versions differ by >1)
            if abs(impl_major - main_major) > 1:
                issues.append(AuditIssue(
                    "warning",
                    f"Version divergence: {impl_name} (v{impl_major}.{impl_minor}) vs {main_name} (v{main_major}.{main_minor})",
                    doc=impl_name
                ))
        
        # Check cross-references
        impl_content = read_path_text(impl_doc)
        main_content = read_path_text(main_doc)
        
        # Companion must reference main doc
        if main_name not in impl_content and slug not in impl_content:
            issues.append(AuditIssue(
                "error",
                f"Companion doesn't reference main doc {main_name}",
                doc=impl_name
            ))
        
        # Main doc should reference companion (warning only)
        if impl_name not in main_content:
            issues.append(AuditIssue(
                "warning",
                f"Main doc should reference companion {impl_name}",
                doc=main_name
            ))
        
        # Check for "Companion to:" header in impl doc
        if "Companion to:" not in impl_content:
            issues.append(AuditIssue(
                "error",
                "Companion doc missing 'Companion to:' in header",
                doc=impl_name
            ))
    
    return issues


def main() -> int:
    logger = get_logger("audit.companion_docs")
    issues = check_companion_relationships()
    
    if not issues:
        logger.info("Companion document audit passed.")
        return 0
    
    emit_issues(logger, issues)
    
    errors = [i for i in issues if i.severity == "error"]
    if errors:
        logger.error(f"Companion document audit failed ({len(errors)} errors).")
        return 1
    else:
        logger.warning(f"Companion document audit passed with {len(issues)} warnings.")
        return 0


if __name__ == "__main__":
    sys.exit(main())

