"""Ensure TODO comments reference an owner and GitHub issue identifier."""

from __future__ import annotations

import pathlib
import re
import sys
from typing import Iterable

ROOT = pathlib.Path(__file__).resolve().parents[1]
TODO_PATTERN = re.compile(r"#\s*TODO\b", re.IGNORECASE)
VALID_TODO_PATTERN = re.compile(r"#\s*TODO\(\s*[^,()]+,\s*GH-\d+\s*\)\s*:", re.IGNORECASE)
SKIP_DIRS = {".git", ".venv", "env", "__pycache__"}
CODE_GLOBS = ("*.py",)


def iter_code_files() -> Iterable[pathlib.Path]:
    for pattern in CODE_GLOBS:
        for path in ROOT.rglob(pattern):
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            yield path


def main() -> int:
    violations: list[str] = []

    for path in iter_code_files():
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
