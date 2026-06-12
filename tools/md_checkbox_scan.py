"""Fail the build when unchecked Markdown checkboxes appear outside whitelisted directories."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from typing import Iterable

ALLOWED_PREFIXES: tuple[str, ...] = (
    "docs/templates/",
    "prds/_template/",
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKBOX_PATTERN = re.compile(r"^\s*[-*]\s*\[\s*\]", re.IGNORECASE)


def is_allowed(path: pathlib.Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return rel.startswith(ALLOWED_PREFIXES)


def iter_markdown_files(
    paths: Iterable[pathlib.Path] | None = None,
) -> Iterable[pathlib.Path]:
    if paths is not None:
        for path in paths:
            if path.suffix == ".md" and path.exists():
                yield path
        return

    for path in ROOT.rglob("*.md"):
        rel_parts = path.relative_to(ROOT).parts
        if ".git" in rel_parts or "node_modules" in rel_parts:
            continue
        yield path


def changed_paths(base: str) -> list[pathlib.Path]:
    result = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=ACMRT",
            f"{base}...HEAD",
            "--",
            "*.md",
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return [ROOT / line.strip() for line in result.stdout.splitlines() if line.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--changed-against",
        help="Only scan Markdown files changed against this git ref.",
    )
    args = parser.parse_args(argv)

    violations: list[str] = []
    paths = changed_paths(args.changed_against) if args.changed_against else None

    for md_file in iter_markdown_files(paths):
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

    if violations:
        print("❌ Markdown checkbox policy violations detected:")
        for violation in violations:
            print(f"- {violation}")
        return 1

    print("✅ No prohibited Markdown checkboxes found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
