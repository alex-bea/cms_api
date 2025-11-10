#!/usr/bin/env python3
"""Backward-compatible wrapper for the packaged repair CLI."""

from cms_pricing.ops.repair_snapshot_paths import main

if __name__ == "__main__":
    raise SystemExit(main())
