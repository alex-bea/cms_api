#!/usr/bin/env python3
"""Cross-check PRD headers against the master catalog.

Warnings/errors when:
- A doc listed in the master catalog has no corresponding file.
- A PRD's status (from header) mismatches the catalog entry.
- Master entries lack concrete "Last Reviewed" dates (still 'YYYY-MM-DD').

Exit code 0 when everything aligns; otherwise prints discrepancies and exits 1.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

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
)
HEADER_PATTERN = re.compile(r"^\*\*(?P<field>[^:]+):\*\*\s*(?P<value>.+?)\s*$")
VALID_STATUSES = ("Adopted", "Draft", "Deprecated", "Archived", "Retired")
STATUS_PATTERN = re.compile(r"(" + "|".join(VALID_STATUSES) + r")[^\s]*", re.IGNORECASE)
DEFAULT_STATUS = "Adopted"

@dataclass
class DocHeader:
    status: str


def parse_header(path: Path) -> DocHeader:
    lines = read_path_text(path).splitlines()[:20]
    status_line = next((line for line in lines if "**Status:**" in line), None)
    if not status_line:
        return DocHeader(status="")

    match = HEADER_PATTERN.match(status_line.strip())
    if not match:
        return DocHeader(status="")

    return DocHeader(status=match.group("value").strip())


def extract_table_entries(text: str) -> Dict[str, str]:
    tables: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("| `STD") and not line.startswith("| `REF") and not line.startswith("| `PRD") and not line.startswith("| `RUN"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 3:
            continue
        name = cells[0].strip("` ")
        status = cells[1]
        tables[name] = status
    return tables


def main() -> int:
    logger = get_logger("audit.doc_metadata")
    issues: list[AuditIssue] = []

    try:
        master_text = read_master_catalog()
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    catalog_status = extract_table_entries(master_text)
    actual_docs = get_prd_names()

    for name in sorted(catalog_status):
        if name not in actual_docs:
            issues.append(
                AuditIssue(
                    "error",
                    "Catalog lists document that is missing on disk",
                    doc=name,
                )
            )

    placeholder_dates = [line for line in master_text.splitlines() if "YYYY-MM-DD" in line]
    for line in placeholder_dates:
        issues.append(
            AuditIssue(
                "error",
                f"Placeholder last-reviewed date remains: {line.strip()}",
                doc=MASTER_DOC_NAME,
            )
        )

    for name, status in catalog_status.items():
        if name not in actual_docs:
            continue
        header = parse_header(PRDS_DIR / name)

        header_status_norm = STATUS_PATTERN.search(header.status or "")
        catalog_status_norm = STATUS_PATTERN.search(status or "")

        header_status = (
            header_status_norm.group(0).lower()
            if header_status_norm
            else DEFAULT_STATUS.lower()
        )
        catalog_status = (
            catalog_status_norm.group(0).lower()
            if catalog_status_norm
            else DEFAULT_STATUS.lower()
        )

        if header_status != catalog_status:
            issues.append(
                AuditIssue(
                    "error",
                    f"Status mismatch: header='{header.status or DEFAULT_STATUS}' vs catalog='{status or DEFAULT_STATUS}'",
                    doc=name,
                )
            )

    if not issues:
        logger.info("Metadata audit passed.")
        return 0

    emit_issues(logger, issues)
    counts = count_by_severity(issues)
    logger.error(
        "Metadata audit failed (%s errors).",
        counts.get("error", 0),
    )
    return exit_code_from_issues(issues)


if __name__ == "__main__":
    sys.exit(main())
