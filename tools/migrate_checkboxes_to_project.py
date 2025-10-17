#!/usr/bin/env python3
"""
Migrate Markdown checkboxes into GitHub Project tasks with dedupe and completion checks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

RE_ITEM = re.compile(r"^\s*[-*]\s*\[\s*\]\s*(.+)$")
RE_HEADING = re.compile(r"^(?P<hashes>#+)\s+(?P<title>.+?)\s*$")
DOC_TASK_TOKEN = re.compile(r"dtid:([a-f0-9]{10})", re.IGNORECASE)

DEFAULT_LABELS = ["from-docs-import", "triage"]
DEFAULT_SLEEP = 0.8  # seconds


def run(cmd: List[str], *, capture: bool = True, check: bool = True) -> str:
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


def md_anchor(heading: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", heading or "").strip().lower()
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug


def normalize_title(value: str) -> str:
    collapsed = re.sub(r"[\s\-_–—]+", " ", value.strip(), flags=re.UNICODE)
    cleaned = re.sub(r"[^\w\s]", "", collapsed, flags=re.UNICODE)
    return cleaned.casefold()


def doc_task_id(path: pathlib.Path, line_no: int, title: str) -> str:
    raw = f"{path.as_posix()}|{line_no}|{title}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


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
        cmd.extend(["--label", label])
    if assignee:
        cmd.extend(["--assignee", assignee])
    return run(cmd)


def add_to_project(issue_url: str, project_number: int, owner: str, dry_run: bool) -> None:
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


def search_issues_by_token(token: str, state: str) -> List[dict]:
    try:
        out = run(
            [
                "gh",
                "issue",
                "list",
                "--state",
                state,
                "--search",
                f'"{token}" in:body',
                "--json",
                "title,url,state",
            ]
        )
        return json.loads(out)
    except Exception:
        return []


def search_closed_exact_title(title: str) -> List[dict]:
    try:
        out = run(
            [
                "gh",
                "issue",
                "list",
                "--state",
                "closed",
                "--search",
                f'"{title}" in:title',
                "--json",
                "title,url,state",
            ]
        )
        return json.loads(out)
    except Exception:
        return []


def fetch_project_snapshot(
    project_number: int,
    owner: str,
) -> Tuple[Dict[str, List[dict]], Dict[str, List[dict]]]:
    try:
        out = run(
            [
                "gh",
                "project",
                "item-list",
                str(project_number),
                "--owner",
                owner,
                "--format",
                "json",
            ]
        )
        data = json.loads(out)
        raw_items = data.get("items", []) if isinstance(data, dict) else data
    except Exception as exc:
        print(f"⚠️  Unable to read project items: {exc}")
        raw_items = []

    by_title: Dict[str, List[dict]] = {}
    by_token: Dict[str, List[dict]] = {}

    for item in raw_items:
        if isinstance(item, str):
            continue

        content = item.get("content") or {}
        title = (content.get("title") or item.get("title") or "").strip()
        status = (item.get("status") or "").strip()
        url = content.get("url") or item.get("url")
        body = content.get("body") or item.get("body") or ""

        norm = normalize_title(title)
        by_title.setdefault(norm, []).append(
            {"title": title, "status": status, "url": url, "body": body}
        )

        match = DOC_TASK_TOKEN.search(body)
        if match:
            token = match.group(1)
            by_token.setdefault(token, []).append(
                {"title": title, "status": status, "url": url, "body": body}
            )

    return by_title, by_token


def iter_md_items(
    path: pathlib.Path, heading: Optional[str]
) -> Iterable[Tuple[int, str, str]]:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    current_heading: Optional[str] = None
    in_scope = heading is None

    for line_no, line in enumerate(lines, start=1):
        heading_match = RE_HEADING.match(line)
        if heading_match:
            current_heading = heading_match.group("title").strip()
            if heading is None:
                in_scope = True
            else:
                in_scope = bool(
                    current_heading == heading
                    or re.fullmatch(heading, current_heading)
                )
            continue

        item_match = RE_ITEM.match(line)
        if item_match and in_scope:
            yield line_no, item_match.group(1).strip(), current_heading or ""


def replace_block_with_note(
    path: pathlib.Path,
    heading: str,
    links: List[str],
) -> None:
    unique: List[str] = []
    seen = set()
    for link in links:
        if link not in seen:
            unique.append(link)
            seen.add(link)

    note = (
        "> **Tasks now tracked in Project:** "
        + (", ".join(unique) if unique else "(migrated)")
    )

    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    output: List[str] = []
    in_scope = False
    replaced = False
    i = 0

    while i < len(lines):
        line = lines[i]
        heading_match = RE_HEADING.match(line)
        if heading_match:
            current = heading_match.group("title").strip()
            in_scope = bool(current == heading or re.fullmatch(heading, current))
            output.append(line)
            i += 1
            continue

        if in_scope and not replaced:
            start = i
            while i < len(lines) and RE_ITEM.match(lines[i]):
                i += 1
            if i > start:
                output.append(note)
                replaced = True
                continue

        output.append(line)
        i += 1

    path.write_text("\n".join(output) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chunked migration of Markdown checkboxes to GitHub Project tasks."
    )
    parser.add_argument("project_number", type=int)
    parser.add_argument("paths", nargs="+", help="Markdown files to process")
    parser.add_argument("--heading", help="Exact or regex heading scope per file")
    parser.add_argument("--limit", type=int, default=0, help="Max issues to create")
    parser.add_argument(
        "--labels",
        default="from-docs-import,triage",
        help="Comma-separated labels for created issues",
    )
    parser.add_argument("--assignee", help="Assign created issues to this user")
    parser.add_argument(
        "--sleep", type=float, default=DEFAULT_SLEEP, help="Pause between creations"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace the scoped checkbox block with a note",
    )
    parser.add_argument(
        "--skip-completed",
        action="store_true",
        help="Skip recreating items when an issue is already closed",
    )
    parser.add_argument(
        "--mark-complete",
        action="store_true",
        help="Annotate the source block with completion info when skipping",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt before creating a new issue",
    )
    args = parser.parse_args()

    repo = repo_http_url()
    sha = run(["git", "rev-parse", "HEAD"])
    labels = [label.strip() for label in args.labels.split(",") if label.strip()]

    repo = repo_http_url()
    owner = repo.rstrip("/").split("github.com/", 1)[1].split("/", 1)[0]

    by_title, by_token = fetch_project_snapshot(args.project_number, owner)

    created: List[str] = []
    reused: List[str] = []
    completed: List[str] = []
    ambiguous: List[str] = []
    skipped: List[str] = []

    section_links: Dict[pathlib.Path, List[str]] = {}

    count = 0

    for path_str in args.paths:
        path = pathlib.Path(path_str)
        if not path.exists():
            print(f"⚠️  Missing file: {path}")
            continue
        if not path.is_file():
            print(f"⚠️  Skipping non-file path: {path}")
            continue

        section_links.setdefault(path, [])

        for line_no, title, section in iter_md_items(path, heading=args.heading):
            if args.limit and count >= args.limit:
                break

            dtid = doc_task_id(path, line_no, title)
            token = f"dtid:{dtid}"
            norm_title = normalize_title(title)

            # Pass 1: match DocTaskID in open issues
            open_hits = search_issues_by_token(token, state="open")
            token_items = by_token.get(dtid, [])
            if open_hits:
                url = open_hits[0]["url"]
                print(f"↩️  Linked (DocTaskID): {title} -> {url}")
                reused.append(url)
                section_links[path].append(url)
                continue
            if token_items:
                url = token_items[0]["url"]
                print(f"↩️  Linked (Project DocTaskID): {title} -> {url}")
                reused.append(url)
                section_links[path].append(url)
                continue

            # Pass 2: normalized title in project items
            title_matches = by_title.get(norm_title, [])
            if len(title_matches) == 1:
                url = title_matches[0]["url"]
                print(f"↩️  Linked (title): {title} -> {url}")
                reused.append(url)
                section_links[path].append(url)
                continue
            if len(title_matches) > 1:
                ambiguous.append(title)
                print(f"⚠️  Ambiguous matches for '{title}' – resolve manually.")
                continue

            # Pass 3: closed issues (DocTaskID first, then title)
            closed_hits = search_issues_by_token(token, state="closed")
            if not closed_hits:
                closed_hits = search_closed_exact_title(title)
            if closed_hits:
                url = closed_hits[0]["url"]
                print(f"✔️  Already completed: {title} via {url}")
                completed.append(url)
                if args.mark_complete:
                    section_links[path].append(f"✅ {url}")
                if args.skip_completed or args.mark_complete:
                    continue

            # Create new issue
            if args.interactive and not args.dry_run:
                answer = input(f"Create new issue for '{title}'? [Y/n] ").strip().lower()
                if answer.startswith("n"):
                    skipped.append(title)
                    continue

            anchor = f"#{md_anchor(section)}" if section else ""
            source = f"{repo}/blob/{sha}/{path.as_posix()}{anchor}"
            body = (
                f"Imported from `{path.as_posix()}` line {line_no}.\n\n"
                f"**Source:** {source}\n\n"
                f"DocTaskID: {token}\n\n"
                "Add acceptance criteria and assign an owner."
            )

            issue_url = create_issue(title, body, labels, args.assignee, args.dry_run)
            print(f"✅ Created: {title} -> {issue_url}")
            created.append(issue_url)
            section_links[path].append(issue_url)
            count += 1

            if not args.dry_run:
                add_to_project(issue_url, args.project_number, owner, dry_run=False)
                # Update caches so repeated titles within this run resolve
                by_title.setdefault(norm_title, []).append(
                    {"title": title, "status": "To Do", "url": issue_url}
                )
                by_token.setdefault(dtid, []).append(
                    {"title": title, "status": "To Do", "url": issue_url}
                )
                time.sleep(args.sleep)

        # Replace checkbox block for this path/heading
        if args.replace and not args.dry_run and args.heading:
            replace_block_with_note(path, args.heading, section_links[path])

    # Summary
    print("\n=== Migration Summary ===")
    print(f"Created:   {len(created)}")
    print(f"Reused:    {len(reused)}")
    print(f"Completed: {len(completed)}")
    print(f"Ambiguous: {len(ambiguous)}")
    print(f"Skipped:   {len(skipped)}")

    if ambiguous:
        print("\n⚠️  Ambiguous titles:")
        for item in ambiguous:
            print(f"- {item}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
