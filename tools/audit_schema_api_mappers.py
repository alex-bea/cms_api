#!/usr/bin/env python3
"""
Audit schema ↔ API column mappers.

Ensures every schema column referenced in mapper dictionaries exists in the
corresponding schema contract JSON. Intentionally scoped to detect regressions
highlighted in STD-parser-contracts-prd-v1.0.md §6.6.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import importlib

MAPPERS_MODULE = "cms_pricing.mappers"
CONTRACTS_DIR = Path("cms_pricing/ingestion/contracts")


def _iter_schema_to_api_mappings(module) -> List[Tuple[str, Dict[str, str]]]:
    mappings = []
    for attr in dir(module):
        if attr.endswith("_SCHEMA_TO_API"):
            dataset = attr[: -len("_SCHEMA_TO_API")].lower()
            mapping = getattr(module, attr)
            if isinstance(mapping, dict):
                mappings.append((dataset, mapping))
    return mappings


def _find_latest_schema_file(dataset: str, contracts_dir: Path) -> Path:
    pattern = f"cms_{dataset}_v*.json"
    matches = sorted(contracts_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No schema contract found for dataset '{dataset}' ({pattern}).")
    return matches[-1]


def _load_schema_columns(schema_file: Path) -> List[str]:
    data = json.loads(schema_file.read_text(encoding="utf-8"))
    columns = data.get("columns", {})
    return list(columns.keys())


def audit_schema_api_mappers(
    contracts_dir: Path = CONTRACTS_DIR,
    mappers_module_name: str = MAPPERS_MODULE,
) -> List[str]:
    """Return list of error messages."""
    module = importlib.import_module(mappers_module_name)
    errors: List[str] = []

    for dataset, mapping in _iter_schema_to_api_mappings(module):
        try:
            schema_file = _find_latest_schema_file(dataset, contracts_dir)
        except FileNotFoundError as exc:
            errors.append(str(exc))
            continue

        schema_columns = set(_load_schema_columns(schema_file))
        for schema_col in mapping.keys():
            if schema_col not in schema_columns:
                errors.append(
                    f"[{dataset}] mapper references '{schema_col}' but schema file "
                    f"{schema_file.name} does not contain that column."
                )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit schema ↔ API column mappers.")
    parser.add_argument(
        "--contracts-dir",
        default=str(CONTRACTS_DIR),
        help="Directory containing schema contract JSON files.",
    )
    parser.add_argument(
        "--module",
        default=MAPPERS_MODULE,
        help="Python module path containing schema/API mapping dictionaries.",
    )
    args = parser.parse_args()

    errors = audit_schema_api_mappers(
        contracts_dir=Path(args.contracts_dir),
        mappers_module_name=args.module,
    )
    if errors:
        print("Schema/API mapper audit failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Schema/API mapper audit passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

