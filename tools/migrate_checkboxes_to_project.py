#!/usr/bin/env python3
"""Chunked migration of Markdown checkboxes into GitHub Project issues."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
import time
from typing import Iterable, List, Optional, Tuple

RE_ITEM = re.compile(r"^\s*[-*]\s*\[\s*\]\s*(.+)$")
RE_HEADING = re.compile(r"^(?P<hashes>#+)\s+(?P<title>.+?)\s*$")

DEFAULT_LABELS = ["from-docs-import", "triage"]
DEFAULT_SLEEP = 0.8  # seconds between API calls


def run(cmd: List[str], capture: bool = True, check: bool = True) -> str:
    if capture:
        result = subprocess.run(cmd, text=True, capture_output=True, check=check)
        return result.stdout.strip()
    subprocess.run(cmd, check=check)
    return ""


def repo_http_url() -> str:
    url = run(["git", "config", "--get", "remote.origin.url"])
    if url.endswith(".git"):
        url = url[:-4]
    if url.startswith("git@github.com:"):
        url = "https://github.com/" + url.split(":", 1)[1]
    return url


def md_anchor(title: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", title).strip().lower()
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug


def search_existing_issue(title: str) -> Optional[str]:
    try:
        out = run(
            [
                "gh",
                "issue",
                "list",
                "--state",
                "open",
                "--search",
                f'"{title}" in:title',
                "--json",
                "title,url",
            ]
        )
        data = json.loads(out)
    except Exception:
        return None

    for item in data:
        if item.get("title") == title:
            return item.get("url")
    return None


def create_issue(
    title: str,
    body: str,
    labels: List[str],
    assignee: Optional[str],
    dry_run: bool,
) -> str:
    if dry_run:
        return "(dry-run) #0"

    cmd = ["gh", "issue", "create", "--title", title, "--body", body]
    for label in labels:
        cmd += ["--label", label]
    if assignee:
        cmd += ["--assignee", assignee]
    return run(cmd)


def add_to_project(
    issue_url: str,
    project_number: int,
    owner: str,
    dry_run: bool,
) -> None:
    if dry_run:
        return
    run(
        [
            "gh",
            "project",
            "item-add",
            str(project_number),
            "--owner",
            owner,
            "--url",
            issue_url,
        ],
        capture=False,
    )


def iter_md_items(
    path: pathlib.Path, heading: Optional[str]
) -> Iterable[Tuple[int, str, str]]:
    """Yield (line_no, checkbox_text, current_heading)."""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    current_heading: Optional[str] = None
    in_scope = heading is None

    for line_no, line in enumerate(lines, start=1):
        match_heading = RE_HEADING.match(line)
        if match_heading:
            current_heading = match_heading.group("title").strip()
            if heading is None:
                in_scope = True
            else:
                in_scope = bool(
                    current_heading == heading
                    or re.fullmatch(heading, current_heading)
                )
            continue

        match_item = RE_ITEM.match(line)
        if match_item and in_scope:
            yield line_no, match_item.group(1).strip(), current_heading or ""


def replace_block_with_pointer(
    path: pathlib.Path,
    heading: str,
    issue_urls: List[str],
) -> None:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    output: List[str] = []
    in_scope = False
    replaced = False
    i = 0

    while i < len(lines):
        line = lines[i]
        match_heading = RE_HEADING.match(line)
        if match_heading:
            current = match_heading.group("title").strip()
            in_scope = bool(current == heading or re.fullmatch(heading, current))
            output.append(line)
            i += 1
            continue

        if in_scope and not replaced:
            start = i
            while i < len(lines) and RE_ITEM.match(lines[i]):
                i += 1
            if i > start:
                pointer = (
                    "> **Tasks now tracked in Project:** "
                    + (", ".join(issue_urls) if issue_urls else "(migrated)")
                )
                output.append(pointer)
                replaced = True
                continue

        output.append(line)
        i += 1

    path.write_text("\n".join(output) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate unchecked Markdown checkboxes to GitHub Project tasks."
    )
    parser.add_argument("project_number", type=int, help="Project (v2) number")
    parser.add_argument("paths", nargs="+", help="Markdown file paths")
    parser.add_argument(
        "--heading",
        help="Exact or regex heading title to scope migration (per file).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum issues to create in this run (0 = no limit).",
    )
    parser.add_argument(
        "--labels",
        default="from-docs-import,triage",
        help="Comma-separated labels for created issues.",
    )
    parser.add_argument("--assignee", help="Assign created issues to this user.")
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Pause between API calls (seconds).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without creating issues or editing files.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace the migrated block under the heading with a pointer note.",
    )
    args = parser.parse_args()

    repo_url = repo_http_url()
    commit_sha = run(["git", "rev-parse", "HEAD"])
    labels = [label.strip() for label in args.labels.split(",") if label.strip()]
    owner = repo_url.rstrip("/").split("github.com/", 1)[1].split("/", 1)[0]

    created_urls: List[str] = []
    created_count = 0

    for path_str in args.paths:
        path = pathlib.Path(path_str)
        if not path.exists():
            print(f"⚠️  Missing file: {path}")
            continue
        if not path.is_file():
            print(f"⚠️  Skipping non-file path: {path}")
            continue

        for line_no, title, section_heading in iter_md_items(path, args.heading):
            if args.limit and created_count >= args.limit:
                break

            existing = search_existing_issue(title)
            if existing:
                print(f"↩️  Found existing issue for '{title}': {existing}")
                created_urls.append(existing)
                continue

            anchor = f"#{md_anchor(section_heading)}" if section_heading else ""
            source = (
                f"{repo_url}/blob/{commit_sha}/{path.as_posix()}{anchor}"
            )
            body = (
                f"Imported from `{path.as_posix()}` line {line_no}.\n\n"
                f"**Source:** {source}\n\n"
                "Add acceptance criteria and assign an owner."
            )

            issue_url = create_issue(
                title, body, labels, args.assignee, args.dry_run
            )
            print(f"✅ Created: {title} -> {issue_url}")
            created_urls.append(issue_url)
            created_count += 1

            add_to_project(issue_url, args.project_number, owner, args.dry_run)
            time.sleep(args.sleep)

        if args.replace and not args.dry_run and args.heading:
            replace_block_with_pointer(path, args.heading, created_urls)

    if created_urls:
        print("\nCreated or linked issues:")
        for url in created_urls:
            print(f"- {url}")
    else:
        print("No issues created.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
