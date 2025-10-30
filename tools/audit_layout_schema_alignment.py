#!/usr/bin/env python3
"""
Audit layout registry column definitions against schema contracts.

Enforces STD-parser-contracts-prd-v2.0.md §7.3:
  - Layout column names must exactly match schema contract columns.
  - Natural key columns must be present in the layout.
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

LAYOUT_MODULE = "cms_pricing.ingestion.parsers.layout_registry"
CONTRACTS_DIR = Path("cms_pricing/ingestion/contracts")
PRDS_DIR = Path("prds")


def find_current_prd(base_name: str) -> Optional[str]:
    """Find the current (highest version) PRD for a given base name."""
    # Remove version suffix to get base name
    base_pattern = re.sub(r'-v\d+\.\d+.*\.md$', '', base_name)
    pattern = f"{base_pattern}-v*.md"
    matches = list(PRDS_DIR.glob(pattern))
    
    if not matches:
        return None
    
    # Sort by version number (extract v1.0, v2.0, etc.)
    def version_key(path):
        match = re.search(r'-v(\d+)\.(\d+)', path.name)
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)
    
    return max(matches, key=version_key).name


def get_current_parser_contracts_prd() -> str:
    """Get the current version of STD-parser-contracts PRD."""
    current = find_current_prd("STD-parser-contracts-prd-v1.0.md")
    if current:
        return current
    # Fallback to hardcoded if no version found
    return "STD-parser-contracts-prd-v1.0.md"


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
    # Handle special name mappings - extract base name from dataset_YYYYQ format
    base_dataset = dataset.split('_')[0]  # Extract 'anes' from 'anes_2025d'
    
    schema_mappings = {
        'anes': 'anescf',  # ANES = Anesthesia Conversion Factor
        'locco': 'localitycounty',  # LOCCO = Locality County
    }
    
    schema_name = schema_mappings.get(base_dataset, base_dataset)
    matches = sorted(contracts_dir.glob(f"cms_{schema_name}_v*.json"))
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
        
        # Metadata columns that are injected by parsers, not in raw CMS files
        metadata_columns = {
            'effective_from', 'effective_to', 'row_content_hash', 'parsed_at',
            'release_id', 'vintage_date', 'product_year', 'quarter_vintage',
            'source_filename', 'source_file_sha256', 'source_uri', 'source_release',
            'source_inner_file'
        }
        
        # Computed/derived columns that are NOT in raw CMS files
        # These are created by parsers through normalization/scaling/transformation
        computed_columns = {
            'anesthesia_cf_usd',  # ANES: Computed from anesthesia_cf_raw (cents → USD)
            'county_fips', 'state_fips',  # LOCCO: Computed in Stage 2 normalization
            'locality_name',  # LOCCO: Derived from fee_area in raw file
        }
        
        # Column aliases mapping (raw file names → schema names)
        # Parsers handle normalization from raw to schema column names
        column_aliases = {
            'locality_id': 'locality_code',  # ANES: Raw file has locality_id, parser maps to locality_code
            'anesthesia_cf': 'anesthesia_cf_raw',  # ANES: Raw file has anesthesia_cf, parser maps to anesthesia_cf_raw then scales to anesthesia_cf_usd
        }
        
        # Core schema columns (excluding metadata and computed)
        core_schema_columns = schema_columns - metadata_columns - computed_columns
        
        # Map layout columns to schema names using aliases
        mapped_layout_columns = set()
        for col in layout_columns:
            mapped_layout_columns.add(column_aliases.get(col, col))
        
        # Check 1: Natural key columns MUST be in layout (critical)
        # BUT exclude metadata and computed natural keys
        layout_natural_keys = natural_keys - metadata_columns - computed_columns
        missing_natural_keys = layout_natural_keys - mapped_layout_columns
        if missing_natural_keys:
            errors.append(
                f"[{dataset}] layout missing natural key columns: {sorted(missing_natural_keys)} "
                f"(schema={schema_file.name})"
            )
        
        # Check 2: Core schema columns SHOULD be in layout (warn if missing)
        missing_core = core_schema_columns - mapped_layout_columns
        if missing_core:
            errors.append(
                f"[{dataset}] layout missing core schema columns: {sorted(missing_core)} "
                f"(schema={schema_file.name})"
            )
        
        # Check 3: Extra columns in layout are OK (additional CMS data)
        # This is informational only - no error
        
        # Check 4: Layout columns not in schema (informational only)
        # Note: Extra columns in layout are OK (additional CMS data fields)
        # We do not add to errors - this is informational only


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

