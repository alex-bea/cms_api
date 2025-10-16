#!/usr/bin/env python3
"""
Audit layout registry column definitions against schema contracts.

Enforces STD-parser-contracts-prd-v1.0.md §7.3:
  - Layout column names must exactly match schema contract columns.
  - Natural key columns must be present in the layout.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

LAYOUT_MODULE = "cms_pricing.ingestion.parsers.layout_registry"
CONTRACTS_DIR = Path("cms_pricing/ingestion/contracts")


def _iter_layouts(module) -> List[Tuple[str, Dict]]:
    layouts = []
    for attr in dir(module):
        if attr.endswith("_LAYOUT"):
            dataset = attr[: -len("_LAYOUT")].lower()
            layout = getattr(module, attr)
            if isinstance(layout, dict) and "columns" in layout:
                layouts.append((dataset, layout))
    return layouts


def _find_latest_schema_file(dataset: str, contracts_dir: Path) -> Path:
    matches = sorted(contracts_dir.glob(f"cms_{dataset}_v*.json"))
    if not matches:
        raise FileNotFoundError(f"No schema contract found for dataset '{dataset}'.")
    return matches[-1]


def _load_schema_contract(schema_file: Path) -> Dict:
    return json.loads(schema_file.read_text(encoding="utf-8"))


def audit_layout_schema_alignment(
    contracts_dir: Path = CONTRACTS_DIR,
    layout_module_name: str = LAYOUT_MODULE,
) -> List[str]:
    module = importlib.import_module(layout_module_name)
    errors: List[str] = []

    for dataset, layout in _iter_layouts(module):
        try:
            schema_file = _find_latest_schema_file(dataset, contracts_dir)
        except FileNotFoundError as exc:
            errors.append(str(exc))
            continue

        schema = _load_schema_contract(schema_file)
        schema_columns = set(schema.get("columns", {}).keys())
        natural_keys = set(schema.get("natural_keys", []))
        layout_columns = set(layout.get("columns", {}).keys())

        missing = schema_columns - layout_columns
        if missing:
            errors.append(
                f"[{dataset}] layout missing schema columns: {sorted(missing)} "
                f"(schema={schema_file.name})"
            )

        extra = layout_columns - schema_columns
        if extra:
            errors.append(
                f"[{dataset}] layout columns not defined in schema: {sorted(extra)} "
                f"(schema={schema_file.name})"
            )

        missing_keys = natural_keys - layout_columns
        if missing_keys:
            errors.append(
                f"[{dataset}] natural key columns absent from layout: {sorted(missing_keys)} "
                f"(schema={schema_file.name})"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit layout/schema alignment.")
    parser.add_argument(
        "--contracts-dir",
        default=str(CONTRACTS_DIR),
        help="Directory containing schema contract JSON files.",
    )
    parser.add_argument(
        "--layout-module",
        default=LAYOUT_MODULE,
        help="Module path containing layout definitions.",
    )
    args = parser.parse_args()

    errors = audit_layout_schema_alignment(
        contracts_dir=Path(args.contracts_dir),
        layout_module_name=args.layout_module,
    )
    if errors:
        print("Layout/schema alignment audit failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Layout/schema alignment audit passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

