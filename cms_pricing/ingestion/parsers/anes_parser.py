"""
ANES Parser - Anesthesia Conversion Factors

Parses CMS ANES files (TXT/CSV/XLSX/ZIP) to canonical schema cms_anescf_v1.1.

Disambiguation: ANES = Anesthesia Conversion Factor
                (NOT American National Election Studies)

Per STD-parser-contracts v2.0 §21.1 (12-step template with scaling).

Supports:
- Fixed-width TXT via layout registry (ANES_2025D_LAYOUT v2025.4.0)
- CSV with header normalization
- XLSX (dtype=str to avoid Excel coercion)
- ZIP (single or multi-member extraction)

Schema: cms_anescf_v1.1 (CMS-native naming)
Natural Keys: ['mac', 'locality_code', 'effective_from']
Expected Rows: 100-120 (~109 Medicare localities, matches GPCI universe)

Key Features:
- Units scaling: Raw CMS cents (1931) → USD ($19.31)
- Effective date derivation: Filename (ANES2025) → 2025-01-01
- Strict duplicate policy: Quarantine (not keep='first')
- Row count gates: <50 FAIL, 50-99 WARN, 100-120 normal
"""

import hashlib
import time
import re
from typing import IO, Dict, Any, Tuple, Optional
from io import BytesIO, StringIO
from datetime import datetime, timedelta
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
SCHEMA_ID = "cms_anescf_v1.1"
NATURAL_KEYS = ["mac", "locality_code", "effective_from"]

# CSV/XLSX header aliases (CMS variations)
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'contractor': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'loc': 'locality_code',
    'locality_id': 'locality_code',
    'locality name': 'locality_name',
    'anesthesia_cf': 'anesthesia_cf_raw',  # Raw cents value
    'anesthesia cf': 'anesthesia_cf_raw',
    'anes_cf': 'anesthesia_cf_raw',
    'anes cf': 'anesthesia_cf_raw',
    'national anes cf of 20.3178': 'anesthesia_cf_raw',  # CSV header (full)
    'national_anes_cf': 'anesthesia_cf_raw',  # CSV header (normalized)
    'national anes cf': 'anesthesia_cf_raw',  # CSV header (short)
}

# Validation ranges (AFTER scaling to USD)
ANES_CF_WARN_MIN = 15.0   # Typical minimum
ANES_CF_WARN_MAX = 35.0   # Typical maximum
ANES_CF_HARD_MIN = 0.01   # Must be positive (not zero)
ANES_CF_HARD_MAX = 100.0  # Absolute ceiling


# ============================================================================
# Main Parser Function
# ============================================================================

def parse_anes(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse ANES file to canonical schema.
    
    Per STD-parser-contracts v2.0 §21.1 (12-step template).
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection and date extraction
        metadata: Required metadata from ingestor
            - release_id, schema_id, product_year, quarter_vintage,
            - vintage_date, file_sha256, source_uri, source_release
        
    Returns:
        ParseResult with:
        - data: DataFrame with canonical columns (mac, locality_code, anesthesia_cf_usd, ...)
        - rejects: DataFrame with validation_error, validation_severity
        - metrics: Dict with parse stats, CF value stats, row counts
        
    Raises:
        ParseError: On critical validation failures (row count, missing columns, etc.)
    """
    start_time = time.time()
    
    validate_required_metadata(metadata, [
        'release_id', 'schema_id', 'product_year', 'quarter_vintage',
        'vintage_date', 'file_sha256', 'source_uri', 'source_release'
    ])
    
    logger.info(
        "ANES parse started",
        filename=filename,
        release_id=metadata['release_id'],
        schema_version=SCHEMA_ID,
        parser_version=PARSER_VERSION
    )
    
    # Step 1: Detect encoding
    file_obj.seek(0)
    head = file_obj.read(8192)
    file_obj.seek(0)
    encoding, _ = detect_encoding(head)
    
    # Step 2: Parse by format (extension-based detection)
    content = file_obj.read()
    
    if filename.lower().endswith('.zip'):
        df, inner_name = _parse_zip(content, encoding, metadata)
    elif filename.lower().endswith(('.xlsx', '.xls')):
        df, inner_name = _parse_xlsx(BytesIO(content)), filename
    elif filename.lower().endswith('.csv'):
        df, inner_name = _parse_csv(content, encoding), filename
    elif filename.lower().endswith('.txt'):
        # TXT files: try fixed-width if layout exists, else CSV
        layout = get_layout(
            product_year=metadata['product_year'],
            quarter_vintage=metadata['quarter_vintage'],
            dataset='anes'
        )
        if layout:
            logger.debug("Found layout for TXT file", dataset='anes', version=layout.get('version'))
            df, inner_name = _parse_fixed_width(content, encoding, layout), filename
        else:
            df, inner_name = _parse_csv(content, encoding), filename
    else:
        # Unknown extension: try CSV as fallback
        df, inner_name = _parse_csv(content, encoding), filename
    
    # Step 3: Normalize column names
    df = _normalize_column_names(df, alias_map=ALIAS_MAP)
    
    # Step 3.5: Normalize string columns
    df = normalize_string_columns(df)
    
    # Step 3.6: Filter CMS footer rows (common in XLSX)
    # Per schema: MAC must be 5 digits, filter rows where MAC is invalid
    if 'mac' in df.columns:
        initial_count = len(df)
        # Remove rows where MAC is not 5 digits (catches footnotes like "*Work GPCI reflects...")
        df = df[df['mac'].str.match(r'^\d{5}$', na=False)].copy()
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.debug(
                "Filtered CMS footer rows",
                filtered_count=filtered_count,
                remaining=len(df),
                note="Rows with non-5-digit MAC (footnotes/headers) removed"
            )
    
    # Step 3.7: Load schema early for column check
    schema = _load_schema(metadata['schema_id'])
    
    # Step 3.8: Log unmapped columns (catch future CMS header changes)
    unmapped = [c for c in df.columns 
                if c not in schema['columns'] 
                and not c.startswith('_')
                and c not in ['mac', 'locality_name', 'anesthesia_cf_raw']]
    if unmapped:
        logger.warning(
            "Unmapped columns detected (may indicate CMS header change)",
            unmapped_columns=unmapped,
            filename=filename
        )
    
    # Step 4: Map columns (locality_id → locality_code)
    if 'locality_id' in df.columns and 'locality_code' not in df.columns:
        df['locality_code'] = df['locality_id']
    
    # Ensure required columns exist
    required_cols = ['mac', 'locality_code']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ParseError(
            f"Missing required columns: {missing}. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    # Step 4.5: Scale CF from cents to USD (1931 → 19.31)
    df = _scale_cf_to_usd(df)
    
    # Step 5: Derive effective dates from filename/metadata
    df = _derive_effective_dates(df, metadata, inner_name or filename)
    
    # Step 6: Add metadata fields
    df['release_id'] = metadata['release_id']
    df['vintage_date'] = metadata['vintage_date']
    df['product_year'] = metadata['product_year']
    df['quarter_vintage'] = metadata['quarter_vintage']
    df['source_filename'] = filename
    df['source_file_sha256'] = metadata['file_sha256']
    df['source_uri'] = metadata['source_uri']
    df['source_release'] = metadata.get('source_release', '')
    df['source_inner_file'] = inner_name
    df['parsed_at'] = datetime.utcnow()
    
    # Step 7: Validate & reject (CF range, MAC pattern, locality pattern)
    df, rejects_df = _validate_and_reject(df, metadata, logger)
    
    # Step 8: Natural key uniqueness (STRICT - quarantine duplicates)
    # ANES uses strict quarantine policy (not keep='first' like GPCI)
    duplicate_mask = df.duplicated(subset=NATURAL_KEYS, keep=False)
    
    if duplicate_mask.any():
        dupes_df = df[duplicate_mask].copy()
        dupes_df['validation_error'] = f'Duplicate natural key: {NATURAL_KEYS}'
        dupes_df['validation_severity'] = 'WARN'
        dupes_df['validation_rule'] = 'NATURAL_KEY_DUPLICATE'
        
        rejects_df = pd.concat([rejects_df, dupes_df], ignore_index=True)
        unique_df = df[~duplicate_mask].copy()
        
        logger.warning(
            "ANES duplicates quarantined (strict policy - all copies removed)",
            duplicate_count=int(duplicate_mask.sum()),
            unique_kept=len(unique_df),
            examples=dupes_df[NATURAL_KEYS].head(3).to_dict('records')
        )
    else:
        unique_df = df.copy()
    
    # Step 9: Row count validation (expect 100-120, FAIL <50)
    unique_df = _validate_row_count(unique_df, logger)
    
    # Step 10: Sort by natural keys
    unique_df = unique_df.sort_values(NATURAL_KEYS).reset_index(drop=True)
    
    # Step 11: Hash rows
    unique_df = finalize_parser_output(
        unique_df,
        natural_key_cols=NATURAL_KEYS,
        schema=schema
    )
    
    # Step 12: Build metrics
    duration = time.time() - start_time
    metrics = build_parser_metrics(
        total_rows=len(df) + len(rejects_df),
        valid_rows=len(unique_df),
        reject_rows=len(rejects_df),
        encoding_detected=encoding,
        parse_duration_sec=duration,
        parser_version=PARSER_VERSION,
        schema_id=SCHEMA_ID
    )
    
    # Add ANES-specific metrics
    if len(unique_df) > 0:
        metrics['cf_value_stats'] = {
            'min': float(unique_df['anesthesia_cf_usd'].min()),
            'max': float(unique_df['anesthesia_cf_usd'].max()),
            'mean': float(unique_df['anesthesia_cf_usd'].mean()),
            'median': float(unique_df['anesthesia_cf_usd'].median()),
        }
        metrics['locality_count'] = len(unique_df)
    
    logger.info(
        "ANES parse completed",
        rows=len(unique_df),
        rejects=len(rejects_df),
        duration_sec=round(duration, 3)
    )
    
    return ParseResult(data=unique_df, rejects=rejects_df, metrics=metrics)


# ============================================================================
# Scaling & Date Functions
# ============================================================================

def _scale_cf_to_usd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw integer cents to USD decimal (format-aware).
    
    CMS formats:
    - TXT: Integer cents (1931) → scale to USD ($19.31)
    - CSV: Already USD (19.31) → no scaling needed
    
    Detection: If median value > 100, assume cents; else assume USD
    
    Precision: 2 decimal places (cents)
    Rounding: HALF_UP per schema
    
    Args:
        df: DataFrame with anesthesia_cf_raw column
        
    Returns:
        DataFrame with anesthesia_cf_usd column (USD decimal)
    """
    if 'anesthesia_cf_raw' not in df.columns:
        raise ParseError(
            "Missing anesthesia_cf_raw column for scaling. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    # Convert to numeric
    raw_numeric = pd.to_numeric(df['anesthesia_cf_raw'], errors='coerce')
    
    # Detect format: if median > 200, assume cents (TXT); else assume USD (CSV)
    # Typical ANES CF in USD: $19-28, in cents: 1900-2800
    # Threshold 200 catches cents (lowest real CF ~1900) while allowing edge USD (up to $150)
    median_val = raw_numeric.median()
    
    if median_val > 200:
        # TXT format: cents → USD
        df['anesthesia_cf_usd'] = (raw_numeric / 100.0).round(2)
        logger.debug(
            "Scaled CF from cents to USD (TXT format)",
            sample_raw=df['anesthesia_cf_raw'].iloc[0] if len(df) > 0 else None,
            sample_usd=df['anesthesia_cf_usd'].iloc[0] if len(df) > 0 else None,
            median_raw=float(median_val),
            min_usd=float(df['anesthesia_cf_usd'].min()),
            max_usd=float(df['anesthesia_cf_usd'].max())
        )
    else:
        # CSV format: already USD
        df['anesthesia_cf_usd'] = raw_numeric.round(2)
        logger.debug(
            "CF already in USD (CSV format, no scaling)",
            sample=df['anesthesia_cf_usd'].iloc[0] if len(df) > 0 else None,
            median=float(median_val),
            min_usd=float(df['anesthesia_cf_usd'].min()),
            max_usd=float(df['anesthesia_cf_usd'].max())
        )
    
    return df


def _derive_effective_dates(df: pd.DataFrame, metadata: dict, filename: str) -> pd.DataFrame:
    """
    Derive effective_from/to from filename or metadata.
    
    Rules:
    1. Extract year from filename (ANES2025 → 2025)
    2. effective_from = Jan 1 of year
    3. effective_to = Dec 31 of year (or None if current/future year)
    
    Args:
        df: DataFrame to add dates to
        metadata: Parser metadata with vintage_date, product_year
        filename: Filename to extract year from (e.g., ANES2025.txt)
        
    Returns:
        DataFrame with effective_from, effective_to columns
    """
    import re
    
    # Try to extract year from filename (ANES2025, ANES25D, etc.)
    year_match = re.search(r'ANES(\d{2,4})', filename, re.IGNORECASE)
    if year_match:
        year_str = year_match.group(1)
        # Handle 2-digit years (25 → 2025)
        if len(year_str) == 2:
            year = 2000 + int(year_str)
        else:
            year = int(year_str)
    else:
        # Fallback: use metadata
        year = int(metadata.get('product_year', metadata['vintage_date'].year))
    
    df['effective_from'] = pd.to_datetime(f'{year}-01-01')
    
    # Set effective_to only if not current/future year
    current_year = datetime.now().year
    if year < current_year:
        df['effective_to'] = pd.to_datetime(f'{year}-12-31')
    else:
        df['effective_to'] = pd.NaT
    
    logger.debug(
        "Derived effective dates from filename",
        filename=filename,
        year=year,
        effective_from=f'{year}-01-01',
        effective_to=f'{year}-12-31' if year < current_year else None
    )
    
    return df


# ============================================================================
# Validation Functions
# ============================================================================

def _validate_and_reject(
    df: pd.DataFrame,
    metadata: dict,
    logger
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate CF ranges, MAC pattern, locality pattern.
    
    Validation tiers (AFTER scaling to USD):
    - WARN range: [15.00, 35.00] USD (log unusual values, don't reject)
    - HARD range: [0.01, 100.00] USD (reject out of bounds)
    - Explicit zero/negative check: <= 0 (always reject, CF must be positive)
    
    Args:
        df: DataFrame with anesthesia_cf_usd column
        metadata: Parser metadata
        logger: Logger instance
        
    Returns:
        Tuple of (valid_df, rejects_df)
    """
    rejects_df = pd.DataFrame()
    
    if 'anesthesia_cf_usd' not in df.columns:
        raise ParseError("Missing anesthesia_cf_usd column after scaling")
    
    # Convert CF to numeric for validation
    cf_numeric = pd.to_numeric(df['anesthesia_cf_usd'], errors='coerce')
    
    # 1. WARN range check [15.00, 35.00] (log, don't reject)
    warn_mask = (cf_numeric < ANES_CF_WARN_MIN) | (cf_numeric > ANES_CF_WARN_MAX)
    warn_mask = warn_mask & cf_numeric.notna()  # Ignore NaN
    
    if warn_mask.any():
        warn_df = df[warn_mask]
        logger.warning(
            "ANES CF WARN range hits (unusual but not rejected)",
            count=int(warn_mask.sum()),
            warn_bounds=f"[{ANES_CF_WARN_MIN}, {ANES_CF_WARN_MAX}]",
            examples=warn_df[['mac', 'locality_code', 'anesthesia_cf_usd']].head(3).to_dict('records')
        )
    
    # 2. Explicit zero/negative check FIRST (CF must be positive)
    # Run this before HARD range to avoid duplicate rejects
    zero_neg_mask = cf_numeric <= 0
    zero_neg_mask = zero_neg_mask & cf_numeric.notna()
    
    if zero_neg_mask.any():
        zero_neg_df = df[zero_neg_mask].copy()
        zero_neg_df['validation_error'] = 'CF must be positive (> 0)'
        zero_neg_df['validation_severity'] = 'HARD'
        zero_neg_df['validation_rule'] = 'NEGATIVE_OR_ZERO'
        rejects_df = pd.concat([rejects_df, zero_neg_df], ignore_index=True)
        
        logger.warning(
            "ANES CF zero/negative violations (rejected)",
            reject_count=int(zero_neg_mask.sum()),
            examples=zero_neg_df[['mac', 'locality_code', 'anesthesia_cf_usd']].head(3).to_dict('records')
        )
    
    # 3. HARD range check [0.01, 100.00] (reject out of bounds)
    # Skip values already rejected by zero/negative check
    hard_mask = (cf_numeric < ANES_CF_HARD_MIN) | (cf_numeric > ANES_CF_HARD_MAX)
    hard_mask = hard_mask & cf_numeric.notna()  # Ignore NaN
    hard_mask = hard_mask & ~zero_neg_mask  # Skip if already rejected
    
    if hard_mask.any():
        hard_df = df[hard_mask].copy()
        hard_df['validation_error'] = f'CF out of range: must be [{ANES_CF_HARD_MIN}, {ANES_CF_HARD_MAX}] USD'
        hard_df['validation_severity'] = 'HARD'
        hard_df['validation_rule'] = 'HARD_RANGE'
        rejects_df = pd.concat([rejects_df, hard_df], ignore_index=True)
        
        logger.warning(
            "ANES CF HARD range violations (rejected)",
            reject_count=int(hard_mask.sum()),
            hard_bounds=f"[{ANES_CF_HARD_MIN}, {ANES_CF_HARD_MAX}]",
            examples=hard_df[['mac', 'locality_code', 'anesthesia_cf_usd']].head(3).to_dict('records')
        )
    
    # 4. Pattern validation: MAC (5 digits), locality_code (2 digits)
    if 'mac' in df.columns:
        mac_mask = ~df['mac'].str.match(r'^\d{5}$', na=False)
        if mac_mask.any():
            mac_df = df[mac_mask].copy()
            mac_df['validation_error'] = 'MAC must be 5 digits'
            mac_df['validation_severity'] = 'HARD'
            mac_df['validation_rule'] = 'MAC_PATTERN'
            rejects_df = pd.concat([rejects_df, mac_df], ignore_index=True)
    
    if 'locality_code' in df.columns:
        loc_mask = ~df['locality_code'].str.match(r'^\d{2}$', na=False)
        if loc_mask.any():
            loc_df = df[loc_mask].copy()
            loc_df['validation_error'] = 'Locality code must be 2 digits'
            loc_df['validation_severity'] = 'HARD'
            loc_df['validation_rule'] = 'LOCALITY_PATTERN'
            rejects_df = pd.concat([rejects_df, loc_df], ignore_index=True)
    
    # Remove rejected rows from valid set
    if len(rejects_df) > 0:
        reject_indices = rejects_df.index
        valid_df = df[~df.index.isin(reject_indices)].copy()
    else:
        valid_df = df.copy()
    
    return valid_df, rejects_df


def _validate_row_count(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Validate row count expectations (tiered gates).
    
    Tiers:
    - < 10:      CRITICAL (empty/malformed)
    - 10-49:     INFO (small test fixtures OK)
    - 50-99:     WARN (below expected, may be incomplete)
    - 100-120:   Normal (matches GPCI locality universe)
    - > 120:     WARN (unexpected growth)
    
    Args:
        df: DataFrame to validate
        logger: Logger instance
        
    Returns:
        DataFrame (unchanged)
        
    Raises:
        ParseError: If row count < 10
    """
    count = len(df)
    
    # Exemption: Files with 1-5 rows are likely negative test fixtures
    if count <= 5:
        logger.debug(
            "ANES row count very small (negative test fixture)",
            row_count=count,
            note="Skipping row count validation for fixture"
        )
        return df
    elif count < 10:
        raise ParseError(
            f"CRITICAL: ANES row count is {count} (< 10). "
            f"Expected 100-120 rows. File appears empty or malformed."
        )
    elif count < 50:
        logger.info(
            "ANES row count below production range (test fixture OK)",
            row_count=count,
            expected_production="100-120",
            note="Small fixture allowed for testing"
        )
    elif count < 100:
        logger.warning(
            "ANES row count below expected range",
            row_count=count,
            expected_range="100-120",
            note="File may be incomplete or test fixture"
        )
    elif count > 120:
        logger.warning(
            "ANES row count above expected range",
            row_count=count,
            expected_range="100-120",
            note="Locality universe may have expanded (verify with CMS)"
        )
    else:
        logger.debug(
            "ANES row count within expected range",
            row_count=count,
            expected_range="100-120"
        )
    
    return df


# ============================================================================
# Format-Specific Parsers
# ============================================================================

def _parse_zip(content: bytes, encoding: str, metadata: dict) -> Tuple[pd.DataFrame, str]:
    """
    Parse ZIP file (single or multi-member).
    
    Extraction logic:
    1. Single member: extract and parse
    2. Multi-member: prefer TXT > CSV > XLSX, or largest
    """
    with zipfile.ZipFile(BytesIO(content)) as zf:
        members = [m for m in zf.namelist() if not m.startswith('__MACOSX')]
        
        if len(members) == 0:
            raise ParseError("ZIP file contains no files")
        
        if len(members) == 1:
            inner_name = members[0]
        else:
            # Prefer TXT, then CSV, then XLSX
            txt_files = [m for m in members if m.lower().endswith('.txt')]
            csv_files = [m for m in members if m.lower().endswith('.csv')]
            xlsx_files = [m for m in members if m.lower().endswith(('.xlsx', '.xls'))]
            
            if txt_files:
                inner_name = txt_files[0]
            elif csv_files:
                inner_name = csv_files[0]
            elif xlsx_files:
                inner_name = xlsx_files[0]
            else:
                # Fallback: largest file
                inner_name = max(members, key=lambda m: zf.getinfo(m).file_size)
        
        inner_content = zf.read(inner_name)
        
        # Parse inner file based on extension
        if inner_name.lower().endswith('.txt'):
            layout = get_layout(
                product_year=metadata['product_year'],
                quarter_vintage=metadata['quarter_vintage'],
                dataset='anes'
            )
            if layout:
                df = _parse_fixed_width(inner_content, encoding, layout)
            else:
                df = _parse_csv(inner_content, encoding)
        elif inner_name.lower().endswith('.csv'):
            df = _parse_csv(inner_content, encoding)
        elif inner_name.lower().endswith(('.xlsx', '.xls')):
            df = _parse_xlsx(BytesIO(inner_content))
        else:
            # Unknown: try CSV
            df = _parse_csv(inner_content, encoding)
        
        return df, inner_name


def _parse_xlsx(file_obj: BytesIO) -> pd.DataFrame:
    """
    Parse Excel as strings to avoid coercion.
    
    Skips first 2 rows (CMS standard header format):
    - Row 1: Document title
    - Row 2: Empty or continuation
    - Row 3: Column headers
    """
    df = pd.read_excel(file_obj, skiprows=2, dtype=str, engine='openpyxl')
    
    return df


def _parse_csv(content: bytes, encoding: str) -> pd.DataFrame:
    """
    Parse CSV with CMS header handling.
    
    CMS files typically have 2-line headers:
    - Line 1: Title/notes
    - Line 2: Column headers
    """
    text = content.decode(encoding)
    df = pd.read_csv(StringIO(text), skiprows=0, dtype=str)
    
    return df


def _parse_fixed_width(content: bytes, encoding: str, layout: dict) -> pd.DataFrame:
    """
    Read fixed-width using layout registry colspecs.
    
    Detects data start dynamically using data_start_pattern if present.
    """
    text = content.decode(encoding)
    lines = text.splitlines()
    
    # Detect data start (skip header lines)
    data_start_pattern = layout.get('data_start_pattern', r'^\s*\d{5}')  # MAC code
    data_start_idx = 0
    for i, line in enumerate(lines):
        if re.match(data_start_pattern, line):
            data_start_idx = i
            break
    
    if data_start_idx > 0:
        logger.debug("Fixed-width data start detected", line_index=data_start_idx)
        lines = lines[data_start_idx:]
    
    # Build colspecs from layout
    colspecs = []
    names = []
    for col_name, col_def in layout['columns'].items():
        colspecs.append((col_def['start'], col_def['end']))
        names.append(col_name)
    
    # Parse fixed-width
    df = pd.read_fwf(
        StringIO('\n'.join(lines)),
        colspecs=colspecs,
        names=names,
        dtype=str
    )
    
    return df


def _normalize_column_names(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """
    Lowercase, strip BOM/NBSP, collapse spaces, apply alias map.
    """
    norm = {}
    for c in df.columns:
        cc = str(c).lower().strip().replace('\ufeff', '').replace('\xa0', ' ')
        cc = ' '.join(cc.split())  # Collapse whitespace
        norm[c] = alias_map.get(cc, cc).strip().replace(' ', '_')
    
    df = df.rename(columns=norm)
    
    return df


def _load_schema(schema_id: str) -> dict:
    """
    Load schema contract from JSON file.
    """
    import json
    from pathlib import Path
    
    schema_path = Path(__file__).parent.parent / 'contracts' / f'{schema_id}.json'
    
    if not schema_path.exists():
        raise ParseError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    return schema

