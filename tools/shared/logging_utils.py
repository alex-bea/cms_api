from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

_LOGGER_CONFIGURED = False
_LOG_FORMAT = "[%(levelname)s] %(message)s"


@dataclass
class AuditIssue:
    severity: str
    message: str
    doc: str | None = None


def get_logger(name: str = "audit") -> logging.Logger:
    global _LOGGER_CONFIGURED
    if not _LOGGER_CONFIGURED:
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
        _LOGGER_CONFIGURED = True
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def emit_issues(logger: logging.Logger, issues: Iterable[AuditIssue]) -> None:
    severity_to_level = {
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
    }
    for issue in issues:
        level = severity_to_level.get(issue.severity.lower(), logging.INFO)
        prefix = f"{issue.doc} – " if issue.doc else ""
        logger.log(level, "%s%s", prefix, issue.message)


def exit_code_from_issues(issues: Iterable[AuditIssue]) -> int:
    return 1 if any(issue.severity.lower() == "error" for issue in issues) else 0


def count_by_severity(issues: Iterable[AuditIssue]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for issue in issues:
        key = issue.severity.lower()
        counts[key] = counts.get(key, 0) + 1
    return counts
