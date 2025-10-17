#!/usr/bin/env python3
"""
Synchronise changelog entries with GitHub issues and project cards.

Workflow:
    1. Parse CHANGELOG.md for issue references in a named section.
    2. For each referenced issue:
        * ensure it exists,
        * optionally close the issue,
        * move the corresponding Project item to the "Done" status,
        * optionally leave an audit comment.

Usage examples:
    python3 tools/mark_tasks_done.py --project-number 5 --owner @me --section Unreleased --dry-run
    python3 tools/mark_tasks_done.py --project-number 5 --owner alex-bea --section "2025-10-01" --close-issues
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHANGELOG_PATH = ROOT / "CHANGELOG.md"

ISSUE_PATTERN = re.compile(r"(?:GH-|#)(\d+)\b")


@dataclass
class IssueReference:
    """Container for a changelog reference to a GitHub issue."""

    number: int
    line_number: int
    line_text: str


def run(cmd: List[str], *, capture: bool = True, check: bool = True) -> str:
    """Run a subprocess command and return its stdout."""
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=capture,
        check=check,
    )
    return result.stdout.strip() if capture else ""


def get_repo_owner_and_name() -> Tuple[str, str]:
    """Derive the GitHub owner and repository from git remote."""
    remote_url = run(["git", "config", "--get", "remote.origin.url"])
    if remote_url.startswith("git@github.com:"):
        owner_repo = remote_url.split(":", 1)[1]
    elif remote_url.startswith("https://github.com/"):
        owner_repo = remote_url.split("github.com/", 1)[1]
    else:
        raise RuntimeError(f"Unsupported remote URL format: {remote_url}")

    owner_repo = owner_repo.rstrip(".git")
    owner, repo = owner_repo.split("/", 1)
    return owner, repo


def extract_section(text: str, heading: str) -> Tuple[str, int]:
    """
    Return the text of a changelog section and its starting line number (1-based).

    The section is identified by a markdown heading `## [heading]`.
    """
    lines = text.splitlines()
    start_idx: Optional[int] = None

    target_heading = f"## [{heading}]"
    for idx, line in enumerate(lines):
        if line.strip() == target_heading:
            start_idx = idx + 1  # content begins on next line
            break

    if start_idx is None:
        raise ValueError(f"Section '{heading}' not found in changelog.")

    end_idx = len(lines)
    for idx in range(start_idx, len(lines)):
        if lines[idx].startswith("## "):
            end_idx = idx
            break

    section_lines = lines[start_idx:end_idx]
    section_text = "\n".join(section_lines)
    return section_text, start_idx + 1  # convert to 1-based for human-friendly reporting


def extract_issue_references(section_text: str, base_line: int) -> List[IssueReference]:
    """Return issue references discovered in the section text."""
    refs: List[IssueReference] = []
    for offset, line in enumerate(section_text.splitlines()):
        for match in ISSUE_PATTERN.finditer(line):
            number = int(match.group(1))
            refs.append(
                IssueReference(
                    number=number,
                    line_number=base_line + offset,
                    line_text=line.strip(),
                )
            )
    return refs


def extract_commit_range(since_ref: str) -> str:
    """Return commit messages/bodies newer than the provided ref/date."""
    try:
        log_output = run(
            [
                "git",
                "log",
                f"{since_ref}..HEAD",
                "--pretty=format:%H%n%s%n%b%n----END-COMMIT----",
            ]
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Failed to read git log since '{since_ref}'.") from exc
    return log_output


def load_project_metadata(owner: str, project_number: int) -> Tuple[str, str, Dict[str, str]]:
    """Fetch project node ID and the Status field metadata."""
    project_view = json.loads(
        run(
            [
                "gh",
                "project",
                "view",
                str(project_number),
                "--owner",
                owner,
                "--format",
                "json",
            ]
        )
    )
    project_id = project_view["id"]

    field_list = json.loads(
        run(
            [
                "gh",
                "project",
                "field-list",
                str(project_number),
                "--owner",
                owner,
                "--format",
                "json",
            ]
        )
    )
    status_field = next(
        (
            field
            for field in field_list["fields"]
            if field.get("name") == "Status"
        ),
        None,
    )
    if not status_field:
        raise RuntimeError("Status field not found in project.")

    status_field_id = status_field["id"]
    status_options = {
        option["name"]: option["id"]
        for option in status_field.get("options", [])
    }
    return project_id, status_field_id, status_options


def load_project_items(owner: str, project_number: int) -> Dict[int, Dict[str, str]]:
    """Return mapping of issue number to project item metadata."""
    raw = json.loads(
        run(
            [
                "gh",
                "project",
                "item-list",
                str(project_number),
                "--owner",
                owner,
                "--format",
                "json",
                "--limit",
                "1000",
            ]
        )
    )
    mapping: Dict[int, Dict[str, str]] = {}
    for item in raw.get("items", []):
        content = item.get("content") or {}
        if content.get("type") != "Issue":
            continue
        number = content.get("number")
        if number is None:
            continue
        mapping[number] = {
            "item_id": item["id"],
            "title": content.get("title", ""),
            "url": content.get("url", ""),
        }
    return mapping


def close_issue(issue_number: int, dry_run: bool) -> None:
    """Close a GitHub issue."""
    if dry_run:
        print(f"[dry-run] Would close issue #{issue_number}")
        return
    run(["gh", "issue", "close", str(issue_number)], capture=False)


def comment_on_issue(issue_number: int, message: str, dry_run: bool) -> None:
    """Leave a comment on an issue for auditability."""
    if dry_run:
        print(f"[dry-run] Would comment on issue #{issue_number}: {message}")
        return
    run(
        ["gh", "issue", "comment", str(issue_number), "--body", message],
        capture=False,
    )


def set_project_status(
    item_id: str,
    project_id: str,
    status_field_id: str,
    status_option_id: str,
    dry_run: bool,
) -> None:
    """Update a project item's status."""
    if dry_run:
        print(
            f"[dry-run] Would set project item {item_id} status -> option {status_option_id}"
        )
        return
    run(
        [
            "gh",
            "project",
            "item-edit",
            "--id",
            item_id,
            "--project-id",
            project_id,
            "--field-id",
            status_field_id,
            "--single-select-option-id",
            status_option_id,
        ],
        capture=False,
    )


def check_issue_exists(issue_number: int) -> Dict[str, str]:
    """Fetch issue metadata."""
    try:
        return json.loads(
            run(
                [
                    "gh",
                    "issue",
                    "view",
                    str(issue_number),
                    "--json",
                    "number,state,title,url",
                ]
            )
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Issue #{issue_number} not found or inaccessible.") from exc


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Mark GitHub tasks done from changelog.")
    parser.add_argument(
        "--project-number",
        type=int,
        required=True,
        help="GitHub Project (v2) number to update.",
    )
    parser.add_argument(
        "--owner",
        help="Project owner login (use @me for current user). Defaults to repo owner.",
    )
    parser.add_argument(
        "--section",
        default="Unreleased",
        help="Changelog section heading to scan (default: 'Unreleased').",
    )
    parser.add_argument(
        "--changelog",
        default=str(CHANGELOG_PATH),
        help="Path to changelog file (default: CHANGELOG.md in repo root).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview actions without modifying issues or project items.",
    )
    parser.add_argument(
        "--close-issues",
        action="store_true",
        help="Close referenced issues after marking them Done.",
    )
    parser.add_argument(
        "--comment",
        action="store_true",
        help="Leave an audit comment on issues that are marked Done.",
    )
    parser.add_argument(
        "--commits-since",
        help="Optional git ref/date range. When set, commits newer than the ref "
             "(e.g. 'v1.2.3', 'HEAD~10', '2025-10-01') are scanned for issue references.",
    )

    args = parser.parse_args(argv)

    changelog_path = pathlib.Path(args.changelog)
    changelog_text = changelog_path.read_text(encoding="utf-8")
    section_text, base_line = extract_section(changelog_text, args.section)
    issue_refs = extract_issue_references(section_text, base_line)

    if not issue_refs:
        print(f"No issue references found in section '{args.section}'.")

    commit_refs: List[IssueReference] = []
    if args.commits_since:
        commit_text = extract_commit_range(args.commits_since)
        commit_refs = extract_issue_references(commit_text, base_line=0)
        if commit_refs:
            print(f"Found {len(commit_refs)} issue reference(s) in commits since '{args.commits_since}'.")
        else:
            print(f"No issue references found in commits since '{args.commits_since}'.")

    all_refs = issue_refs + commit_refs
    if not all_refs:
        print("No issue references detected in changelog or commit range.")
        return 0

    print(f"Found {len(issue_refs)} issue reference(s) in section '{args.section}'.")

    repo_owner, _ = get_repo_owner_and_name()
    project_owner = args.owner or repo_owner

    project_id, status_field_id, status_options = load_project_metadata(
        project_owner, args.project_number
    )
    if "Done" not in status_options:
        raise RuntimeError("Project status options do not include 'Done'.")
    done_option_id = status_options["Done"]

    project_items = load_project_items(project_owner, args.project_number)

    processed: List[int] = []
    skipped: List[str] = []

    seen: set[int] = set()
    for ref in all_refs:
        if ref.number in seen:
            continue
        seen.add(ref.number)

        issue_data = check_issue_exists(ref.number)
        issue_number = issue_data["number"]
        issue_url = issue_data["url"]

        item = project_items.get(issue_number)
        if not item:
            skipped.append(
                f"Issue #{issue_number} not found in project {args.project_number}."
            )
            continue

        print(
            f"Marking issue #{issue_number} ({issue_data['title']}) as Done "
            f"(line {ref.line_number}: {ref.line_text})"
        )
        set_project_status(
            item_id=item["item_id"],
            project_id=project_id,
            status_field_id=status_field_id,
            status_option_id=done_option_id,
            dry_run=args.dry_run,
        )

        if args.close_issues and issue_data["state"].upper() != "CLOSED":
            close_issue(issue_number, args.dry_run)

        if args.comment:
            comment_body = (
                f"Marked **Done** via changelog section '{args.section}' "
                f"(line {ref.line_number})."
            )
            comment_on_issue(issue_number, comment_body, args.dry_run)

        processed.append(issue_number)

    print("\nSummary:")
    if processed:
        print(f"  Updated issues: {', '.join(f'#{n}' for n in processed)}")
    else:
        print("  Updated issues: none")

    if args.commits_since:
        print(f"  Commit scan range: {args.commits_since}..HEAD")

    if skipped:
        print("  Skipped:")
        for message in skipped:
            print(f"    - {message}")

    if args.dry_run:
        print("\nDry-run complete. No changes were applied.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:  # pragma: no cover - surface nice error
        print(f"Error: {err}", file=sys.stderr)
        raise SystemExit(1)
