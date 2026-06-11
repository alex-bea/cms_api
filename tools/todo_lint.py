"""Ensure TODO comments reference an owner and GitHub issue identifier."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence

ROOT = pathlib.Path(__file__).resolve().parents[1]
TODO_PATTERN = re.compile(r"#\s*TODO\b", re.IGNORECASE)
VALID_TODO_PATTERN = re.compile(r"#\s*TODO\(\s*[^,()]+,\s*GH-\d+\s*\)\s*:", re.IGNORECASE)
SKIP_DIRS = {".git", ".venv", ".venv_gpci", "env", "__pycache__", "site-packages"}
SKIP_FILES = {"tools/github_tasks_setup.py"}
CODE_GLOBS = ("*.py",)


def should_scan(path: pathlib.Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    if rel in SKIP_FILES:
        return False
    if any(part in SKIP_DIRS for part in path.parts):
        return False
    return True


def iter_code_files() -> Iterable[pathlib.Path]:
    for pattern in CODE_GLOBS:
        for path in ROOT.rglob(pattern):
            if not should_scan(path):
                continue
            yield path


def iter_staged_code_files() -> Iterable[pathlib.Path]:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for rel_path in result.stdout.splitlines():
        path = ROOT / rel_path
        if path.suffix == ".py" and path.is_file() and should_scan(path):
            yield path


def scan_files(paths: Iterable[pathlib.Path]) -> list[str]:
    violations: list[str] = []

    for path in paths:
        for line_no, line in enumerate(
            path.read_text(encoding="utf-8", errors="ignore").splitlines(),
            start=1,
        ):
            if TODO_PATTERN.search(line) and not VALID_TODO_PATTERN.search(line):
                violations.append(
                    f"{path}:{line_no}: naked TODO – use '# TODO(owner, GH-123): message'"
                )

    return violations


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail when TODO comments do not include an owner and GH issue."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--staged",
        action="store_true",
        help="Scan only staged Python files in the git index.",
    )
    mode.add_argument(
        "--all",
        action="store_true",
        help="Run a full-repository TODO audit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    paths = iter_staged_code_files() if args.staged else iter_code_files()
    violations = scan_files(paths)

    if violations:
        print("❌ TODO policy violations detected:")
        for violation in violations:
            print(f"- {violation}")
        return 1

    print("✅ No naked TODO comments found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
