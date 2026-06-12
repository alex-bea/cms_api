"""Ensure TODO comments reference an owner and GitHub issue identifier."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from typing import Iterable

ROOT = pathlib.Path(__file__).resolve().parents[1]
TODO_PATTERN = re.compile(r"#\s*TODO\b", re.IGNORECASE)
VALID_TODO_PATTERN = re.compile(
    r"#\s*TODO\(\s*[^,()]+,\s*GH-\d+\s*\)\s*:", re.IGNORECASE
)
SKIP_DIRS = {".git", ".venv", ".venv_gpci", "env", "__pycache__", "site-packages"}
SKIP_FILES = {"tools/github_tasks_setup.py"}
CODE_GLOBS = ("*.py",)


def iter_code_files(
    paths: Iterable[pathlib.Path] | None = None,
) -> Iterable[pathlib.Path]:
    if paths is not None:
        for path in paths:
            rel = path.relative_to(ROOT).as_posix()
            if rel in SKIP_FILES:
                continue
            if any(part in SKIP_DIRS for part in path.relative_to(ROOT).parts):
                continue
            if path.suffix == ".py" and path.exists():
                yield path
        return

    for pattern in CODE_GLOBS:
        for path in ROOT.rglob(pattern):
            rel = path.relative_to(ROOT).as_posix()
            if rel in SKIP_FILES:
                continue
            if any(part in SKIP_DIRS for part in path.parts):
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
            "*.py",
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
        help="Only scan Python files changed against this git ref.",
    )
    args = parser.parse_args(argv)

    violations: list[str] = []
    paths = changed_paths(args.changed_against) if args.changed_against else None

    for path in iter_code_files(paths):
        for line_no, line in enumerate(
            path.read_text(encoding="utf-8", errors="ignore").splitlines(),
            start=1,
        ):
            if TODO_PATTERN.search(line) and not VALID_TODO_PATTERN.search(line):
                violations.append(
                    f"{path}:{line_no}: naked TODO – use '# TODO(owner, GH-123): message'"
                )

    if violations:
        print("❌ TODO policy violations detected:")
        for violation in violations:
            print(f"- {violation}")
        return 1

    print("✅ No naked TODO comments found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
