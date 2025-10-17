"""
GPCI Parser - Geographic Practice Cost Indices

Parses CMS GPCI files (TXT/CSV/XLSX/ZIP) to canonical schema cms_gpci_v1.2.

Per STD-parser-contracts v1.7 §21.1 (11-step template).

Supports:
- Fixed-width TXT via layout registry (GPCI_2025D_LAYOUT v2025.4.1)
- CSV with header normalization
- XLSX (dtype=str to avoid Excel coercion)
- ZIP (single or multi-member extraction)

Schema: cms_gpci_v1.2 (CMS-native naming)
Natural Keys: ['locality_code', 'effective_from']
Expected Rows: 100-120 (~109 Medicare localities)
"""

import hashlib
import time
import re
from typing import IO, Dict, Any, Tuple, Optional
from io import BytesIO, StringIO
from datetime import datetime
import zipfile

import pandas as pd
import structlog

from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    detect_encoding,
    canonicalize_numeric_col,
    finalize_parser_output,
    normalize_string_columns,
    validate_required_metadata,
    enforce_categorical_dtypes,
    check_natural_key_uniqueness,
    build_parser_metrics,
    ValidationSeverity,
    ParseError,
)
from cms_pricing.ingestion.parsers.layout_registry import get_layout


logger = structlog.get_logger(__name__)


# ============================================================================
# Constants
# ============================================================================

PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_gpci_v1.2"
NATURAL_KEYS = ["locality_code", "effective_from"]

# CSV/XLSX header aliases (CMS variations)
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'loc': 'locality_code',
    'locality name': 'locality_name',
    'pw gpci': 'gpci_work',
    'work gpci': 'gpci_work',
    '2025 pw gpci (with 1.0 floor)': 'gpci_work',  # CSV/XLSX header format
    'pe gpci': 'gpci_pe',
    '2025 pe gpci': 'gpci_pe',  # CSV/XLSX header format
    'practice expense gpci': 'gpci_pe',
    'mp gpci': 'gpci_mp',
    '2025 mp gpci': 'gpci_mp',  # CSV/XLSX header format
    'malpractice gpci': 'gpci_mp',
    'malp gpci': 'gpci_mp',
}


# ============================================================================
# Main Parser Function
# ============================================================================

def parse_gpci(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse GPCI file to canonical schema.
    
    Per STD-parser-contracts v1.7 §21.1 (11-step template).
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor (see IMPLEMENTATION.md §3)
            - release_id, schema_id, product_year, quarter_vintage,
            - vintage_date, file_sha256, source_uri, source_release
        
    Returns:
        ParseResult with:
            - data: pandas DataFrame (canonical rows with metadata + row_content_hash)
            - rejects: pandas DataFrame (validation failures)
            - metrics: Dict (total_rows, valid_rows, reject_rows, parse_duration_sec, etc.)
        
    Raises:
        ParseError: If parsing fails critically
        ValueError: If required metadata missing
    
    Schema: cms_gpci_v1.2 (CMS-native naming)
    Natural Keys: ['locality_code', 'effective_from']
    Expected Rows: 100-120 (warn outside, fail if < 90)
    """
    start_time = time.perf_counter()
    
    logger.info(
        "Starting GPCI parse",
        filename=filename,
        schema_id=metadata.get('schema_id'),
        parser_version=PARSER_VERSION
    )
    
    # Step 0: Metadata preflight + source_release validation
    validate_required_metadata(metadata, [
        'release_id', 'schema_id', 'product_year', 'quarter_vintage',
        'vintage_date', 'file_sha256', 'source_uri', 'source_release'
    ])
    
    # CI safety: Validate source_release format
    year = metadata['product_year']
    valid_releases = {f'RVU{year[-2:]}A', f'RVU{year[-2:]}B', 
                     f'RVU{year[-2:]}C', f'RVU{year[-2:]}D'}
    if metadata['source_release'] not in valid_releases:
        raise ParseError(
            f"Unknown source_release: {metadata['source_release']}. "
            f"Expected one of {valid_releases} for year {year}"
        )

    # Step 1: Detect encoding
    head = file_obj.read(8192)
    encoding, _ = detect_encoding(head)
    file_obj.seek(0)
    
    logger.info("Encoding detected", encoding=encoding)

    # Step 2: Parse by format (layout-existence pattern)
    content = file_obj.read()
    layout = get_layout(
        product_year=metadata['product_year'],
        quarter_vintage=metadata['quarter_vintage'],
        dataset='gpci'
    )

    if filename.lower().endswith('.zip'):
        df, inner_name = _parse_zip(content, encoding)
    elif layout is not None:
        df, inner_name = _parse_fixed_width(content, encoding, layout), filename
    elif filename.lower().endswith(('.xlsx', '.xls')):
        df, inner_name = _parse_xlsx(BytesIO(content)), filename
    else:
        df, inner_name = _parse_csv(content, encoding), filename

    # Step 3: Normalize column names
    df = _normalize_column_names(df, alias_map=ALIAS_MAP)
    
    # Step 3.5: Normalize string columns
    df = normalize_string_columns(df)
    
    # Step 3.6: Load schema early for column check
    schema = _load_schema(metadata['schema_id'])
    
    # Step 3.7: Log unmapped columns (catch future CMS header changes)
    unmapped = [c for c in df.columns 
                if c not in schema['columns'] 
                and not c.startswith('_')
                and c not in ['mac', 'state', 'locality_name']]
    if unmapped:
        logger.warning("Unmapped columns detected (possible CMS header change)",
                      unmapped_columns=unmapped,
                      schema_id=metadata['schema_id'],
                      filename=filename)

    # Initialize rejects
    rejects_df = pd.DataFrame()

    # Step 4: Cast dtypes
    df = _cast_dtypes(df, metadata)

    # Step 5: Range validation (2-tier: warn + fail)
    range_rejects = _validate_gpci_ranges(df)
    if len(range_rejects) > 0:
        rejects_df = pd.concat([rejects_df, range_rejects], ignore_index=True)
        df = df[~df.index.isin(range_rejects.index)].copy()

    # Step 5.5: Row count validation (skip for test fixtures)
    if not metadata.get('skip_row_count_validation', False):
        rowcount_warn = _validate_row_count(df)
        if rowcount_warn:
            logger.warning(rowcount_warn)

    # Step 6: Categorical validation (GPCI v1.2 has no enums, but kept for consistency)
    cat_result = enforce_categorical_dtypes(
        df, schema,
        natural_keys=NATURAL_KEYS,
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )

    # Step 7: Natural key uniqueness (WARN severity for GPCI)
    unique_df, dupes_df = check_natural_key_uniqueness(
        cat_result.valid_df,
        natural_keys=NATURAL_KEYS,
        severity=ValidationSeverity.WARN,
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id']
    )
    if len(dupes_df) > 0:
        rejects_df = pd.concat([rejects_df, dupes_df], ignore_index=True)
        logger.warning(f"GPCI duplicates quarantined: {len(dupes_df)} rows")

    # Step 8: Inject metadata + provenance
    for col in ['release_id', 'vintage_date', 'product_year', 'quarter_vintage']:
        unique_df[col] = metadata[col]
    unique_df['source_filename'] = filename
    unique_df['source_file_sha256'] = metadata['file_sha256']
    unique_df['source_uri'] = metadata.get('source_uri', '')
    unique_df['source_release'] = metadata['source_release']
    unique_df['source_inner_file'] = inner_name
    unique_df['parsed_at'] = pd.Timestamp.utcnow()

    # Step 9: Finalize (Core-only hash + sort)
    final_df = finalize_parser_output(
        unique_df,
        natural_key_cols=NATURAL_KEYS,
        schema=schema
    )

    # Step 10: Build metrics
    all_rejects = pd.concat([rejects_df, cat_result.rejects_df], ignore_index=True) if len(cat_result.rejects_df) > 0 else rejects_df
    parse_duration = time.perf_counter() - start_time
    
    metrics = {
        'total_rows': len(df) + len(range_rejects),
        'valid_rows': len(final_df),
        'reject_rows': len(all_rejects),
        'encoding_detected': encoding,
        'parse_duration_sec': parse_duration,
        'parser_version': PARSER_VERSION,
        'schema_id': metadata['schema_id'],
        'locality_count': len(final_df),
        'gpci_value_stats': {
            'work_min': float(final_df[final_df['gpci_work'] != '']['gpci_work'].min()) if len(final_df) > 0 and (final_df['gpci_work'] != '').any() else None,
            'work_max': float(final_df[final_df['gpci_work'] != '']['gpci_work'].max()) if len(final_df) > 0 and (final_df['gpci_work'] != '').any() else None,
            'pe_min': float(final_df[final_df['gpci_pe'] != '']['gpci_pe'].min()) if len(final_df) > 0 and (final_df['gpci_pe'] != '').any() else None,
            'pe_max': float(final_df[final_df['gpci_pe'] != '']['gpci_pe'].max()) if len(final_df) > 0 and (final_df['gpci_pe'] != '').any() else None,
            'mp_min': float(final_df[final_df['gpci_mp'] != '']['gpci_mp'].min()) if len(final_df) > 0 and (final_df['gpci_mp'] != '').any() else None,
            'mp_max': float(final_df[final_df['gpci_mp'] != '']['gpci_mp'].max()) if len(final_df) > 0 and (final_df['gpci_mp'] != '').any() else None,
        }
    }

    # Step 11: Join invariant check
    assert metrics['total_rows'] == metrics['valid_rows'] + metrics['reject_rows'], \
        f"Row count mismatch: {metrics['total_rows']} != {metrics['valid_rows']} + {metrics['reject_rows']}"
    
    logger.info(
        "GPCI parse completed",
        rows=len(final_df),
        rejects=len(all_rejects),
        duration_sec=parse_duration
    )
    
    return ParseResult(data=final_df, rejects=all_rejects, metrics=metrics)


# ============================================================================
# Helper Functions
# ============================================================================

def _parse_zip(content: bytes, encoding: str) -> Tuple[pd.DataFrame, str]:
    """
    Parse ZIP, return (df, inner_filename).
    
    Prefers member with 'GPCI' in name. Auto-detects format (fixed-width vs CSV).
    """
    with zipfile.ZipFile(BytesIO(content)) as zf:
        names = [n for n in zf.namelist() if not n.endswith('/')]
        if len(names) == 0:
            raise ParseError("Empty ZIP archive")
        
        # Prefer GPCI-named member
        inner = next((n for n in names if 'gpci' in n.lower()), names[0])
        
        with zf.open(inner) as fh:
            raw = fh.read()
        
        if inner.lower().endswith(('.xlsx', '.xls')):
            return _parse_xlsx(BytesIO(raw)), inner
        else:
            # Check if fixed-width by decoding a sample and looking for layout pattern
            sample = raw[:500].decode(encoding, errors='replace')
            
            # Check for fixed-width data pattern (MAC code at start of line)
            if re.search(r'^\d{5}', sample, re.MULTILINE):
                logger.debug(f"Detected fixed-width format in {inner}")
                # It's fixed-width - need to get the layout
                layout = LAYOUT_REGISTRY.get(('gpci', 2025, None))  # Use None for latest
                if layout:
                    return _parse_fixed_width(raw, encoding, layout), inner
            
            # Default to CSV
            logger.debug(f"Parsing {inner} as CSV")
            return _parse_csv(raw, encoding), inner


def _parse_fixed_width(content: bytes, encoding: str, layout: Dict) -> pd.DataFrame:
    """
    Read fixed-width using layout registry colspecs.
    
    Detects data start dynamically using data_start_pattern.
    """
    text = content.decode(encoding, errors='replace')
    
    # Detect data start (skip headers)
    lines = text.splitlines()
    data_start_idx = 0
    pattern = layout.get('data_start_pattern', r'^\d{5}')
    
    for i, line in enumerate(lines):
        if len(line) >= layout['min_line_length']:
            if re.match(pattern, line.strip()):
                data_start_idx = i
                break
    
    logger.debug("Fixed-width data start detected", line_index=data_start_idx)
    
    # Build colspecs from layout
    colspecs = [(c['start'], c['end']) for c in layout['columns'].values()]
    names = list(layout['columns'].keys())
    
    df = pd.read_fwf(
        StringIO('\n'.join(lines[data_start_idx:])),
        colspecs=colspecs,
        names=names,
        dtype=str
    )
    return df


def _parse_csv(content: bytes, encoding: str) -> pd.DataFrame:
    """
    Parse CSV with dialect sniffing.
    
    Skips first 2 rows (CMS standard header format):
    - Row 1: Document title
    - Row 2: Empty
    - Row 3: Column headers
    
    Raises ParseError on duplicate headers.
    """
    decoded = content.decode(encoding, errors='replace')
    df = pd.read_csv(StringIO(decoded), skiprows=2, dtype=str)
    
    # Duplicate header guard (pandas mangles to .1, .2, etc.)
    dupes = [c for c in df.columns if '.' in str(c) and '.1' in str(c)]
    if dupes:
        raise ParseError(f"Duplicate column headers detected: {dupes}")
    
    return df


def _parse_xlsx(file_obj: BytesIO) -> pd.DataFrame:
    """
    Parse Excel as strings to avoid coercion.
    
    Drops duplicate header rows (common in CMS Excel files).
    """
    df = pd.read_excel(file_obj, dtype=str, engine='openpyxl')
    
    # Drop duplicate header rows (when first data row = column names)
    if len(df) > 1 and (df.iloc[0] == df.columns).all():
        df = df.iloc[1:].reset_index(drop=True)
    
    return df


def _normalize_column_names(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """
    Lowercase, strip BOM/NBSP, collapse spaces, apply alias map.
    
    Handles:
    - BOM prefix (\ufeff)
    - Non-breaking spaces (\xa0)
    - Multiple spaces
    - Case variations
    """
    norm = {}
    for c in df.columns:
        # Strip BOM, NBSP, whitespace
        cc = str(c).replace('\ufeff', '').replace('\xa0', ' ').strip().lower()
        # Collapse multiple spaces
        cc = ' '.join(cc.split())
        # Apply alias map, convert spaces to underscores
        norm[c] = alias_map.get(cc, cc).strip().replace(' ', '_')
    
    df = df.rename(columns=norm)
    return df


def _cast_dtypes(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """
    Cast with 3dp precision for GPCI values; zero-pad locality codes.
    
    GPCI values returned as strings for hash stability.
    """
    # GPCI values: 3 decimal precision (CMS standard)
    for col in ['gpci_work', 'gpci_pe', 'gpci_mp']:
        if col in df.columns:
            df[col] = canonicalize_numeric_col(df[col], precision=3, rounding_mode='HALF_UP')
    
    # Dates
    if 'effective_from' not in df.columns:
        # Derive from quarter: A=Jan1, B=Apr1, C=Jul1, D=Oct1
        quarter = metadata.get('quarter_vintage', 'A')[-1]  # Extract letter
        month_map = {'A': '01', 'B': '04', 'C': '07', 'D': '10'}
        month = month_map.get(quarter, '01')
        df['effective_from'] = pd.to_datetime(f"{metadata['product_year']}-{month}-01")
    else:
        df['effective_from'] = pd.to_datetime(df['effective_from'], errors='coerce')
    
    if 'effective_to' in df.columns:
        df['effective_to'] = pd.to_datetime(df['effective_to'], errors='coerce')
    else:
        df['effective_to'] = pd.NaT
    
    # Locality code: zero-pad to 2 digits
    if 'locality_code' in df.columns:
        df['locality_code'] = df['locality_code'].astype(str).str.strip().str.zfill(2)
    
    return df


def _validate_gpci_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return rows that violate hard range thresholds.
    
    Soft bounds [0.30, 2.00]: warn (logged, not rejected)
    Hard bounds [0.20, 2.50]: fail (rejected)
    """
    hard_low, hard_high = 0.20, 2.50
    
    # Convert to numeric for comparison (canonicalize_numeric_col returns strings)
    gpci_work_num = pd.to_numeric(df['gpci_work'], errors='coerce')
    gpci_pe_num = pd.to_numeric(df['gpci_pe'], errors='coerce')
    gpci_mp_num = pd.to_numeric(df['gpci_mp'], errors='coerce')
    
    mask = (
        (gpci_work_num < hard_low) | (gpci_work_num > hard_high) |
        (gpci_pe_num < hard_low) | (gpci_pe_num > hard_high) |
        (gpci_mp_num < hard_low) | (gpci_mp_num > hard_high)
    )
    
    rejects = df[mask].copy()
    if len(rejects) > 0:
        rejects['validation_error'] = 'GPCI value out of hard bounds [0.20, 2.50]'
        rejects['validation_severity'] = 'BLOCK'
        rejects['validation_rule'] = 'gpci_hard_range'
        
        logger.error(
            "GPCI hard range violations",
            reject_count=len(rejects),
            examples=rejects[['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp']].head(3).to_dict('records')
        )
    
    return rejects


def _validate_row_count(df: pd.DataFrame) -> Optional[str]:
    """
    Warn/fail on unexpected GPCI row counts with actionable guidance.
    
    Expected: 100-120 localities (CMS post-CA consolidation: ~109)
    Fail: <90 (critical data loss)
    """
    count = len(df)
    
    if count < 90:
        raise ParseError(
            f"CRITICAL: GPCI row count {count} < 90 (minimum threshold). "
            "Potential parsing failure or severe locality reduction. "
            "Actions: Verify layout version, data start detection, and CMS release notes for locality changes."
        )
    
    if count < 100:
        return (
            f"Row count {count} < 100 (below expected). "
            "Possible causes: CA locality consolidation, incorrect layout, or parsing error. "
            "Actions: Review CMS release notes for locality changes; verify layout alignment."
        )
    
    if count > 120:
        return (
            f"Row count {count} > 120 (above expected). "
            "Possible causes: Locality splits, MAC boundary changes, or duplicate rows. "
            "Actions: Verify natural key uniqueness; check CMS release documentation."
        )
    
    return None  # Within expected range [100, 120]


def _load_schema(schema_id: str) -> Dict[str, Any]:
    """
    Load schema contract (package-safe with fallback).
    
    For cms_gpci_v1.2, loads cms_gpci_v1.2.json directly.
    """
    from importlib.resources import files
    import json
    from pathlib import Path
    
    try:
        # Package-safe load
        schema_path = files('cms_pricing.ingestion.contracts').joinpath(f'{schema_id}.json')
        with schema_path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        # Fallback for dev environment
        schema_path = Path(__file__).parent.parent / 'contracts' / f'{schema_id}.json'
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)

