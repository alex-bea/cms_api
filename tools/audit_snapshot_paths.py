#!/usr/bin/env python3
"""Backward-compatible wrapper for the packaged audit CLI."""

from cms_pricing.ops.audit_snapshot_paths import main

if __name__ == "__main__":
    raise SystemExit(main())
