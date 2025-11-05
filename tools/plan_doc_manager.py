#!/usr/bin/env python3
"""Manage planning Markdown documents.

Features:
    * Discover plan-like markdown files in one or more roots
    * Generate summary reports (plain text or JSON)
    * Normalize metadata (Status / Tags) with optional write-back
    * Optionally build an index document enumerating all plans

Safe by default – no files are modified unless --write is provided.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Discovery & parsing helpers
# ---------------------------------------------------------------------------


PLAN_KEYWORDS = ("plan", "planning")
HEADER_PATTERN = re.compile(r"^#{1,6}\s+(?P<title>.+)$")
METADATA_PATTERN = re.compile(r"^\*\*(?P<field>[^:]+):\*\*\s*(?P<value>.+)$")


@dataclass
class PlanDocument:
    path: Path
    title: Optional[str]
    status: Optional[str]
    owners: List[str]
    tags: List[str]
    last_reviewed: Optional[str]
    last_modified: Optional[str]
    headings: List[str]
    metadata_block_range: Tuple[int, int]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["path"] = str(self.path)
        return data


def is_plan_candidate(path: Path) -> bool:
    """Heuristic to decide if a markdown file should be treated as a plan."""
    name = path.stem.lower()
    if any(keyword in name for keyword in PLAN_KEYWORDS):
        return True
    # Inspect parent directories for plan markers
    parent_bits = [part.lower() for part in path.parts]
    if any("plan" in part for part in parent_bits):
        return True
    # As fallback, peek into first heading for "Plan"
    try:
        with path.open("r", encoding="utf-8") as fh:
            for _ in range(10):
                line = fh.readline()
                if not line:
                    break
                if line.strip().startswith("#") and "plan" in line.lower():
                    return True
    except OSError:
        return False
    return False


def discover_plan_files(roots: Sequence[Path]) -> List[Path]:
    files: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        if root.is_file() and root.suffix.lower() == ".md":
            if is_plan_candidate(root):
                files.append(root)
            continue
        for path in root.rglob("*.md"):
            if is_plan_candidate(path):
                files.append(path)
    return sorted({path for path in files})


def parse_metadata(path: Path) -> PlanDocument:
    title: Optional[str] = None
    status: Optional[str] = None
    owners: List[str] = []
    tags: List[str] = []
    last_reviewed: Optional[str] = None
    headings: List[str] = []
    metadata_start = None
    metadata_end = None

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Failed reading {path}: {exc}") from exc

    lines = text.splitlines()

    for idx, line in enumerate(lines[:200]):
        stripped = line.strip()
        if not stripped:
            continue
        if metadata_start is None and stripped.startswith("**"):
            metadata_start = idx

        header_match = HEADER_PATTERN.match(stripped)
        if header_match:
            heading_title = header_match.group("title").strip()
            headings.append(heading_title)
            if title is None and stripped.startswith("#"):
                title = heading_title
            continue

        meta_match = METADATA_PATTERN.match(stripped)
        if meta_match:
            field = meta_match.group("field").strip().lower()
            value = meta_match.group("value").strip()
            if field == "status":
                status = value
            elif field in {"owner", "owners"}:
                owners = [part.strip() for part in re.split(r",|;", value) if part.strip()]
            elif field == "tags":
                tags = [part.strip() for part in re.split(r",|;", value) if part.strip()]
            elif field == "last reviewed":
                last_reviewed = value
            continue

        if metadata_start is not None and metadata_end is None and stripped.startswith("#"):
            metadata_end = idx
            break

    if metadata_start is None:
        metadata_start = 0
    if metadata_end is None:
        metadata_end = min(len(lines), 40)

    last_modified = _git_last_modified(path)

    return PlanDocument(
        path=path,
        title=title,
        status=status,
        owners=owners,
        tags=tags,
        last_reviewed=last_reviewed,
        last_modified=last_modified,
        headings=headings,
        metadata_block_range=(metadata_start, metadata_end),
    )


def _git_last_modified(path: Path) -> Optional[str]:
    """Return last commit date for path, falling back to filesystem timestamp."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cs", "--", str(path)],
            capture_output=True,
            check=True,
            text=True,
        )
        date_str = result.stdout.strip()
        if date_str:
            return date_str
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    try:
        mtime = path.stat().st_mtime
        return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------


def update_document_metadata(
    doc: PlanDocument,
    *,
    ensure_plan_tag: bool,
    additional_tags: Sequence[str],
    new_status: Optional[str],
    write: bool,
) -> Tuple[bool, List[str]]:
    """Apply metadata updates to a document.

    Returns (changed?, messages)
    """
    messages: List[str] = []
    try:
        lines = doc.path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeError(f"Failed to read {doc.path}: {exc}") from exc

    metadata_start, metadata_end = doc.metadata_block_range
    # Ensure we have enough space to insert metadata; copy slice
    block = lines[metadata_start:metadata_end]

    tag_set = {tag for tag in doc.tags}
    for tag in additional_tags:
        if tag:
            tag_set.add(tag)
    if ensure_plan_tag:
        tag_set.add("Plan")

    updated = False

    # Update or insert tags line
    desired_tags_line = None
    if tag_set:
        desired_tags_line = f"**Tags:** {', '.join(sorted(tag_set))}"

    tag_idx = None
    status_idx = None
    for rel_idx, content in enumerate(block):
        stripped = content.strip()
        match = METADATA_PATTERN.match(stripped)
        if not match:
            continue
        field = match.group("field").strip().lower()
        if field == "tags":
            tag_idx = rel_idx
        elif field == "status":
            status_idx = rel_idx

    if desired_tags_line:
        if tag_idx is not None:
            if block[tag_idx].strip() != desired_tags_line:
                block[tag_idx] = desired_tags_line
                updated = True
                messages.append(f"Updated tags -> {desired_tags_line}")
        else:
            insert_at = 0
            if status_idx is not None:
                insert_at = status_idx + 1
            block.insert(insert_at, desired_tags_line)
            updated = True
            messages.append(f"Added tags line -> {desired_tags_line}")

    if new_status:
        normalized_status = new_status.strip()
        if status_idx is not None:
            existing = block[status_idx].strip()
            if existing != f"**Status:** {normalized_status}":
                block[status_idx] = f"**Status:** {normalized_status}"
                updated = True
                messages.append(f"Updated status -> {normalized_status}")
        else:
            block.insert(0, f"**Status:** {normalized_status}")
            updated = True
            messages.append(f"Added status -> {normalized_status}")

    if not updated:
        return False, messages

    # Reassemble document
    new_lines = list(lines)
    new_lines[metadata_start:metadata_end] = block
    new_content = "\n".join(new_lines) + "\n"

    if write:
        doc.path.write_text(new_content, encoding="utf-8")
    return True, messages


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_report(docs: Iterable[PlanDocument]) -> None:
    docs = list(docs)
    if not docs:
        print("No plan documents found.")
        return

    width = max(len(str(doc.path)) for doc in docs) + 2
    header = f"{'Document'.ljust(width)}Status / Tags / Owner / Last Modified"
    print(header)
    print("-" * len(header))
    for doc in docs:
        status = doc.status or "Unknown"
        tags = ", ".join(doc.tags) if doc.tags else "-"
        owners = ", ".join(doc.owners) if doc.owners else "-"
        last_modified = doc.last_modified or "-"
        print(f"{str(doc.path).ljust(width)}{status} | {tags} | {owners} | {last_modified}")


def write_index(index_path: Path, docs: Iterable[PlanDocument]) -> None:
    table_lines = [
        "# Plan Document Index",
        "",
        "| Document | Status | Tags | Owners | Last Reviewed | Last Modified |",
        "|---|---|---|---|---|---|",
    ]
    for doc in sorted(docs, key=lambda d: str(d.path)):
        status = doc.status or "-"
        tags = ", ".join(doc.tags) if doc.tags else "-"
        owners = ", ".join(doc.owners) if doc.owners else "-"
        reviewed = doc.last_reviewed or "-"
        modified = doc.last_modified or "-"
        table_lines.append(
            f"| `{doc.path}` | {status} | {tags} | {owners} | {reviewed} | {modified} |"
        )
    table_lines.append("")
    index_path.write_text("\n".join(table_lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan documentation manager")
    parser.add_argument(
        "--roots",
        nargs="+",
        default=["artifacts"],
        help="Directories or files to scan (default: artifacts)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print plain-text report of discovered plan documents",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary of plan documents",
    )
    parser.add_argument(
        "--apply-tags",
        action="store_true",
        help="Ensure each plan doc has a Tags line containing 'Plan'",
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Additional tag to add (can be used multiple times)",
    )
    parser.add_argument(
        "--status",
        help="Set or update the status metadata (e.g., 'Complete', 'Draft v1.0')",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist metadata changes (required for modifications)",
    )
    parser.add_argument(
        "--update-index",
        type=Path,
        help="Write (or overwrite) a Markdown index summarizing all plan docs",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root (used for resolving relative paths). Default: current directory.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    roots = [Path(root).resolve() if Path(root).is_absolute() else repo_root / root for root in args.roots]

    docs = [parse_metadata(path) for path in discover_plan_files(roots)]

    if args.apply_tags or args.tag or args.status:
        modifications = False
        for doc in docs:
            changed, messages = update_document_metadata(
                doc,
                ensure_plan_tag=args.apply_tags,
                additional_tags=args.tag,
                new_status=args.status,
                write=args.write,
            )
            if changed:
                modifications = True
                prefix = "[WRITE]" if args.write else "[DRY]"
                print(f"{prefix} {doc.path}:")
                for msg in messages:
                    print(f"   - {msg}")
        if modifications and not args.write:
            print("\nNOTE: No files were modified (missing --write). Re-run with --write to persist.")

    if args.report:
        print_report(docs)

    if args.json:
        json_payload = [doc.to_dict() for doc in docs]
        print(json.dumps(json_payload, indent=2))

    if args.update_index:
        if not args.write:
            print("Skipping index update (requires --write).")
        else:
            write_index(args.update_index, docs)
            print(f"Wrote index to {args.update_index}")

    if not any([args.report, args.json, args.apply_tags, args.tag, args.status, args.update_index]):
        # Default action: show report for convenience
        print_report(docs)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
