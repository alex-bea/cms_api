#!/usr/bin/env python3
"""Validate CMS API work tracker state."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.work_tracker import check_command  # noqa: E402


def main() -> int:
    return check_command(ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
