"""Utility to migrate unchecked Markdown checkboxes into GitHub Project tasks."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
from typing import Iterable, Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKBOX_PATTERN = re.compile(r"^\s*[-*]\s*\[\s*\]\s*(.+)$")
HEADING_PATTERN = re.compile(r"^(#+)\s+(.*)")
DEFAULT_LABELS = ["from-docs-import", "triage"]

# Files to scan for checkboxes. Adjust as needed.
DEFAULT_GLOBS = [
    "planning/project/NEXT_TODOS.md",
    "planning/project/INGESTOR_DEVELOPMENT_TASKS.md",
    "planning/project/STATUS_REPORT.md",
    "planning/project/TESTING_SUMMARY.md",
    "planning/project/github_tasks_plan.md",
]


def git_repo_url() -> str:
    raw = (
        subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"], text=True
        )
        .strip()
        .removesuffix(".git")
    )
    if raw.startswith("git@github.com:"):
        user_repo = raw.split(":", 1)[1]
        return f"https://github.com/{user_repo}"
    return raw


def current_commit() -> str:
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    )


def github_anchor(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text).strip().lower()
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug


def headed_lines(
    lines: Iterable[str],
) -> Iterable[tuple[int, str, Optional[str]]]:
    current_heading: Optional[str] = None
    for index, line in enumerate(lines, start=1):
        heading_match = HEADING_PATTERN.match(line)
        if heading_match:
            current_heading = heading_match.group(2).strip()
        yield index, line.rstrip("\n"), current_heading


def create_issue(title: str, body: str) -> str:
    cmd = ["gh", "issue", "create", "--title", title, "--body", body]
    for label in DEFAULT_LABELS:
        cmd.extend(["--label", label])
    output = subprocess.check_output(cmd, text=True).strip()
    print(f"Created issue: {output}")
    return output


def add_to_project(item_url: str, project_number: str, owner: str) -> None:
    subprocess.check_call(
        ["gh", "project", "item-add", project_number, "--owner", owner, "--url", item_url]
    )


def migrate_file(
    rel_path: pathlib.Path,
    project_number: str,
    repo_url: str,
    commit_sha: str,
    owner: str,
) -> int:
    created = 0
    full_path = ROOT / rel_path
    lines = full_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line_no, line, heading in headed_lines(lines):
        match = CHECKBOX_PATTERN.match(line)
        if not match:
            continue

        title = match.group(1).strip()
        anchor = f"#{github_anchor(heading)}" if heading else ""
        rel_posix = rel_path.as_posix()
        source_url = f"{repo_url}/blob/{commit_sha}/{rel_posix}{anchor}"
        body = (
            f"Imported from `{rel_posix}` (line {line_no}).\n\n"
            f"**Source:** {source_url}\n\n"
            "Please add acceptance criteria and assign an owner."
        )
        issue_url = create_issue(title, body)
        add_to_project(issue_url, project_number, owner)
        created += 1
    return created


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python tools/migrate_checkboxes_to_project.py <PROJECT_NUMBER> [glob...]", file=sys.stderr)
        return 2

    project_number = argv[1]
    globs = argv[2:] if len(argv) > 2 else DEFAULT_GLOBS

    repo_url = git_repo_url()
    owner = repo_url.rstrip("/").split("github.com/", 1)[1].split("/", 1)[0]
    commit_sha = current_commit()

    total = 0
    for pattern in globs:
        for path in ROOT.glob(pattern):
            if not path.exists():
                continue
            if path.is_dir():
                continue
            rel_path = path.relative_to(ROOT)
            print(f"Scanning {rel_path}...")
            total += migrate_file(rel_path, project_number, repo_url, commit_sha, owner)

    print(f"Migration complete. Created {total} issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
