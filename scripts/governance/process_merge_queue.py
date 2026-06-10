#!/usr/bin/env python3
"""Inspect queued-for-merge work tracker tasks for end-of-day sync."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.work_tracker import load_tracker, validate_tracker  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report queued tasks without changing files.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Reserved for future automated queue closeout; currently reports only.",
    )
    return parser.parse_args(argv)


def queued_for_merge_payload() -> dict[str, Any]:
    tracker = load_tracker(ROOT)
    validation = validate_tracker(tracker)
    candidates = [
        {
            "id": task["id"],
            "title": task["title"],
            "parent_id": task["parent_id"],
            "updated_at": task["updated_at"],
            "current_task": task.get("current_task"),
            "next_action": task.get("next_action"),
            "resume_from": task.get("resume_from"),
        }
        for task in tracker.tasks
        if task.get("status") == "queued_for_merge"
    ]
    return {
        "status": "error" if validation.errors else "ok",
        "errors": validation.errors,
        "warnings": validation.warnings,
        "queued_for_merge_count": len(candidates),
        "candidates": candidates,
        "applied": False,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = queued_for_merge_payload()
    if args.apply and payload["queued_for_merge_count"]:
        payload["status"] = "manual_review_required"
        payload["warnings"] = [
            *payload["warnings"],
            "--apply is not automated yet; reconcile queued_for_merge tasks in YAML during EOD sync.",
        ]

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Queued-for-merge tasks: {payload['queued_for_merge_count']}")
        for candidate in payload["candidates"]:
            print(f"- {candidate['id']}: {candidate['title']}")
        for error in payload["errors"]:
            print(f"ERROR: {error}", file=sys.stderr)
        for warning in payload["warnings"]:
            print(f"WARN: {warning}")

    return 1 if payload["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
