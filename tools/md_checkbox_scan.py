"""Detect unchecked Markdown checkboxes outside whitelisted directories."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence

ALLOWED_PREFIXES: tuple[str, ...] = (
    "docs/templates/",
    "prds/_templates/",
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKBOX_PATTERN = re.compile(r"^\s*[-*]\s*\[\s*\]", re.IGNORECASE)


def is_allowed(path: pathlib.Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return rel.startswith(ALLOWED_PREFIXES)


def iter_markdown_files() -> Iterable[pathlib.Path]:
    for path in ROOT.rglob("*.md"):
        rel_parts = path.relative_to(ROOT).parts
        if ".git" in rel_parts or "node_modules" in rel_parts:
            continue
        yield path


def iter_staged_markdown_files() -> Iterable[pathlib.Path]:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for rel_path in result.stdout.splitlines():
        path = ROOT / rel_path
        if path.suffix.lower() == ".md" and path.is_file():
            yield path


def scan_files(markdown_files: Iterable[pathlib.Path]) -> list[str]:
    violations: list[str] = []

    for md_file in markdown_files:
        if is_allowed(md_file):
            continue

        for line_no, line in enumerate(
            md_file.read_text(encoding="utf-8", errors="ignore").splitlines(),
            start=1,
        ):
            if CHECKBOX_PATTERN.search(line):
                violations.append(
                    f"{md_file}:{line_no}: unchecked checkbox not allowed"
                )
                break  # one hit per file is enough

    return violations


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when unchecked Markdown checkboxes appear outside "
            "allowed templates."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--staged",
        action="store_true",
        help="Scan only staged Markdown files in the git index.",
    )
    mode.add_argument(
        "--all",
        action="store_true",
        help="Run a full-repository Markdown audit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    markdown_files = (
        iter_staged_markdown_files() if args.staged else iter_markdown_files()
    )
    violations = scan_files(markdown_files)

    if violations:
        print("❌ Markdown checkbox policy violations detected:")
        for violation in violations:
            print(f"- {violation}")
        return 1

    print("✅ No prohibited Markdown checkboxes found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
