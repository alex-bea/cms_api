#!/usr/bin/env python3
"""Validate that cross-references inside PRDs point to real files and required links exist."""

from __future__ import annotations

import re
import sys

from tools.shared.logging_utils import (
    AuditIssue,
    count_by_severity,
    emit_issues,
    exit_code_from_issues,
    get_logger,
)
from tools.shared.prd_helpers import (
    MASTER_DOC_NAME,
    classify_doc,
    get_prd_names,
    iter_prd_paths,
    read_path_text,
)

CODE_REF_PATTERN = re.compile(
    r"`([A-Z]{3,4}-[a-z0-9\-]+(?:-prd-v[0-9]+\.[0-9]+)?\.md)`"
)

MANDATORY_REFS = {
    "STD": {MASTER_DOC_NAME},
    "REF": {MASTER_DOC_NAME},
    "PRD": {MASTER_DOC_NAME},
    "RUN": {MASTER_DOC_NAME},
    "DOC": set(),
}


def main() -> int:
    logger = get_logger("audit.doc_links")
    issues: list[AuditIssue] = []
    docs = get_prd_names()

    for path in sorted(iter_prd_paths()):
        text = read_path_text(path)
        refs = set(CODE_REF_PATTERN.findall(text))
        for ref in sorted(refs - docs):
            issues.append(
                AuditIssue(
                    "error",
                    f"References missing document: {ref}",
                    doc=path.name,
                )
            )

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

    if not issues:
        logger.info("Link audit passed.")
        return 0

    emit_issues(logger, issues)
    counts = count_by_severity(issues)
    logger.error("Link audit failed (%s errors).", counts.get("error", 0))
    return exit_code_from_issues(issues)


if __name__ == "__main__":
    sys.exit(main())
