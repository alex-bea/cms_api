"""
PPRRVU Parser - Physician/Practitioner Relative Value Units

Parses CMS MPFS PPRRVU files (fixed-width TXT, CSV, XLSX) to canonical schema.

Per STD-parser-contracts v1.2 §21.
"""

from typing import IO, Dict, Any, Optional
import pandas as pd
import structlog
from io import StringIO, BytesIO
from datetime import datetime

from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    ValidationResult,
    ValidationSeverity,
    CategoryValidationError,
    DuplicateKeyError,
    LayoutMismatchError,
    detect_encoding,
    enforce_categorical_dtypes,
    finalize_parser_output,
    check_natural_key_uniqueness,
    canonicalize_numeric_col,
    compute_row_id
)
from cms_pricing.ingestion.parsers.layout_registry import get_layout
import json
from pathlib import Path

logger = structlog.get_logger()

PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_pprrvu_v1.1"
NATURAL_KEYS = ["hcpcs", "modifier", "status_code", "effective_from"]


def parse_pprrvu(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse PPRRVU file to canonical schema.
    
    Per STD-parser-contracts v1.2 §21.1 (9-step template).
    
    Supports:
    - Fixed-width TXT (using layout registry)
    - CSV (header variations, case-insensitive matching)
    - XLSX (single sheet)
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor:
            - release_id: str
            - product_year: str (e.g., "2025")
            - quarter_vintage: str (e.g., "2025Q4")
            - vintage_date: datetime
            - file_sha256: str
            - source_uri: str (optional)
            - schema_id: str (should be "cms_pprrvu_v1.1")
            - layout_version: str (e.g., "v2025.4.0")
        
    Returns:
        ParseResult with:
            - data: Canonical DataFrame (valid rows, metadata injected, hashed, sorted)
            - rejects: Rejected rows (validation failures with provenance)
            - metrics: Parse metrics (duration, encoding, rows, etc.)
        
    Raises:
        ValueError: If required metadata missing
        DuplicateKeyError: If duplicate natural keys found (severity=BLOCK)
        LayoutMismatchError: If fixed-width parsing fails
    
    Example:
        >>> metadata = {
        ...     'release_id': 'mpfs_2025_q4',
        ...     'product_year': '2025',
        ...     'quarter_vintage': '2025Q4',
        ...     'vintage_date': datetime(2025, 10, 1),
        ...     'schema_id': 'cms_pprrvu_v1.1',
        ...     'layout_version': 'v2025.4.0',
        ...     'file_sha256': 'abc123...',
        ...     'source_uri': 'https://cms.gov/...'
        ... }
        >>> with open('PPRRVU2025.txt', 'rb') as f:
        ...     result = parse_pprrvu(f, 'PPRRVU2025.txt', metadata)
        >>> print(f"Parsed {len(result.data)} rows, {len(result.rejects)} rejects")
    """
    import time
    start_time = time.perf_counter()
    
    # Validate required metadata
    required = ['release_id', 'product_year', 'quarter_vintage', 'schema_id', 'file_sha256']
    missing = [k for k in required if k not in metadata]
    if missing:
        raise ValueError(f"Missing required metadata: {missing}")
    
    logger.info(
        "Starting PPRRVU parse",
        filename=filename,
        release_id=metadata['release_id'],
        schema_id=metadata['schema_id']
    )
    
    # Step 1: Detect encoding
    content = file_obj.read()
    encoding, content_clean = detect_encoding(content)
    logger.info("Encoding detected", encoding=encoding, filename=filename)
    
    # Step 2: Parse format (fixed-width vs CSV vs XLSX)
    try:
        if filename.lower().endswith('.txt'):
            df = _parse_fixed_width(content_clean, encoding, metadata)
        elif filename.lower().endswith('.xlsx'):
            df = _parse_xlsx(content_clean)
        else:
            df = _parse_csv(content_clean, encoding)
        
        logger.info(f"Parsed {len(df)} rows from {filename}")
    except Exception as e:
        raise LayoutMismatchError(f"Failed to parse {filename}: {e}") from e
    
    # Step 3: Normalize column names
    df = _normalize_column_names(df)
    
    # Step 4: Cast dtypes (explicit, no coercion)
    df = _cast_dtypes(df, metadata)
    
    # Step 5: Load schema contract (JSON file)
    # Schema files are named without minor version: cms_pprrvu_v1.0.json contains v1.1 spec
    schema_id = metadata.get('schema_id', SCHEMA_ID)
    # Strip minor version: cms_pprrvu_v1.1 → cms_pprrvu_v1.0
    schema_base = schema_id.rsplit('.', 1)[0] if '.' in schema_id else schema_id
    schema_file = Path(__file__).parent.parent / "contracts" / f"{schema_base}.0.json"
    
    with open(schema_file) as f:
        schema = json.load(f)
    
    # Step 6: Categorical validation (BEFORE casting to categorical)
    cat_result = enforce_categorical_dtypes(
        df, 
        schema,
        natural_keys=NATURAL_KEYS,
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )
    
    # Step 7: Natural key uniqueness check (BLOCK severity for PPRRVU)
    unique_df, dup_df = check_natural_key_uniqueness(
        cat_result.valid_df,
        natural_keys=NATURAL_KEYS,
        severity=ValidationSeverity.BLOCK,  # Hard-fail on duplicates
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id']
    )
    
    # Step 8: Inject metadata columns
    unique_df['release_id'] = metadata['release_id']
    unique_df['vintage_date'] = metadata.get('vintage_date')
    unique_df['product_year'] = metadata['product_year']
    unique_df['quarter_vintage'] = metadata['quarter_vintage']
    unique_df['source_filename'] = filename
    unique_df['source_file_sha256'] = metadata['file_sha256']
    unique_df['source_uri'] = metadata.get('source_uri', '')
    unique_df['parsed_at'] = pd.Timestamp.utcnow()
    unique_df['schema_id'] = metadata['schema_id']
    
    # Step 9: Finalize (hash + sort)
    final_df = finalize_parser_output(
        unique_df,
        NATURAL_KEYS,
        schema
    )
    
    # Step 10: Build metrics
    parse_duration = time.perf_counter() - start_time
    
    metrics = {
        **cat_result.metrics,  # From categorical validation
        'parser_version': PARSER_VERSION,
        'encoding_detected': encoding,
        'parse_duration_sec': parse_duration,
        'schema_id': metadata['schema_id'],
        'layout_version': metadata.get('layout_version', 'unknown'),
        'filename': filename,
        'total_rows': len(final_df) + len(cat_result.rejects_df),
        'rows_valid': len(final_df),
        'rows_rejected': len(cat_result.rejects_df)
    }
    
    logger.info(
        "PPRRVU parse completed",
        filename=filename,
        rows_valid=len(final_df),
        rows_rejected=len(cat_result.rejects_df),
        duration_sec=parse_duration,
        encoding=encoding
    )
    
    return ParseResult(
        data=final_df,
        rejects=cat_result.rejects_df,
        metrics=metrics
    )


# ============================================================================
# Helper Functions (Private)
# ============================================================================

def _parse_fixed_width(content: bytes, encoding: str, metadata: Dict) -> pd.DataFrame:
    """
    Parse fixed-width format using layout registry.
    
    Args:
        content: File bytes (BOM-stripped)
        encoding: Detected encoding
        metadata: Metadata dict with product_year, quarter_vintage
        
    Returns:
        DataFrame with raw parsed data
        
    Raises:
        LayoutMismatchError: If layout not found or parsing fails
    """
    text = content.decode(encoding)
    lines = text.strip().split('\n')
    
    # Get layout from registry
    year = metadata.get('product_year', '2025')
    quarter_vintage = metadata.get('quarter_vintage', '2025Q4')
    
    # layout_registry.get_layout(product_year, quarter_vintage, dataset)
    layout = get_layout(year, quarter_vintage, 'pprrvu')
    
    if layout is None:
        raise LayoutMismatchError(
            f"Layout not found for pprrvu year={year} quarter={quarter_vintage}. "
            f"Check layout_registry.py for registered layouts."
        )
    
    # Skip header rows (lines starting with 'HDR')
    data_lines = [line for line in lines if not line.startswith('HDR')]
    
    min_length = layout.get('min_line_length', 200)
    
    records = []
    for line_num, line in enumerate(data_lines, start=1):
        if len(line) < min_length:
            logger.debug(f"Skipping short line {line_num}: {len(line)} < {min_length}")
            continue
        
        try:
            record = {}
            for col_name, col_spec in layout['columns'].items():
                start = col_spec['start']
                end = col_spec['end']
                value = line[start:end].strip()
                record[col_name] = value if value else None
            
            records.append(record)
        except Exception as e:
            logger.warning(f"Failed to parse line {line_num}: {e}")
            continue
    
    return pd.DataFrame(records)


def _parse_csv(content: bytes, encoding: str) -> pd.DataFrame:
    """
    Parse CSV format with header detection.
    
    Args:
        content: File bytes
        encoding: Detected encoding
        
    Returns:
        DataFrame with raw parsed data
    """
    text = content.decode(encoding)
    return pd.read_csv(StringIO(text))


def _parse_xlsx(content: bytes) -> pd.DataFrame:
    """
    Parse XLSX format.
    
    Args:
        content: File bytes
        
    Returns:
        DataFrame with raw parsed data
    """
    return pd.read_excel(BytesIO(content), sheet_name=0)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to canonical snake_case.
    
    Handles CMS header variations (case, spaces, underscores).
    
    Args:
        df: Raw DataFrame
        
    Returns:
        DataFrame with normalized column names
    """
    COLUMN_ALIASES = {
        'HCPCS': 'hcpcs',
        'HCPCS_CODE': 'hcpcs',
        'HCPCS CODE': 'hcpcs',
        'CPT': 'hcpcs',
        'CPT/HCPCS': 'hcpcs',
        'MOD': 'modifier',
        'MODIFIER': 'modifier',
        'MOD1': 'modifier',
        'STATUS': 'status_code',
        'STATUS_CODE': 'status_code',
        'STAT': 'status_code',
        'WORK_RVU': 'work_rvu',
        'WORK RVU': 'work_rvu',
        'RVU_WORK': 'work_rvu',
        'WORK': 'work_rvu',
        'PE_NONFAC_RVU': 'pe_rvu_nonfac',
        'PE NONFAC RVU': 'pe_rvu_nonfac',
        'NON_FAC_PE_RVU': 'pe_rvu_nonfac',
        'PE_NONFAC': 'pe_rvu_nonfac',
        'PE_FAC_RVU': 'pe_rvu_fac',
        'PE FAC RVU': 'pe_rvu_fac',
        'FAC_PE_RVU': 'pe_rvu_fac',
        'PE_FAC': 'pe_rvu_fac',
        'MP_RVU': 'mp_rvu',
        'MALPRACTICE_RVU': 'mp_rvu',
        'MALPRACTICE RVU': 'mp_rvu',
        'MALP_RVU': 'mp_rvu',
        'MP': 'mp_rvu',
        'GLOBAL': 'global_days',
        'GLOBAL_DAYS': 'global_days',
        'GLOB_DAYS': 'global_days',
        'NA_IND': 'na_indicator',
        'NA': 'na_indicator',
        'NA_INDICATOR': 'na_indicator',
        'OPPS_CAP': 'opps_cap_applicable',
        'CAP': 'opps_cap_applicable',
        'OPPS_CAP_IND': 'opps_cap_applicable',
        'EFFECTIVE_DATE': 'effective_from',
        'EFFECTIVE': 'effective_from',
        'EFF_DATE': 'effective_from',
    }
    
    df = df.copy()
    df.columns = [
        COLUMN_ALIASES.get(c.strip().upper(), c.lower().strip().replace(' ', '_'))
        for c in df.columns
    ]
    return df


def _cast_dtypes(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """
    Cast columns to explicit dtypes (no coercion).
    
    Categorical conversion happens in validation step.
    
    Args:
        df: Normalized DataFrame
        metadata: Metadata dict with vintage_date
        
    Returns:
        DataFrame with explicit dtypes
    """
    df = df.copy()
    
    # Codes as strings (categorical conversion in Step 6)
    if 'hcpcs' in df.columns:
        df['hcpcs'] = df['hcpcs'].astype(str).str.strip().str.upper()
    
    if 'modifier' in df.columns:
        df['modifier'] = df['modifier'].fillna('').astype(str).str.strip().str.upper()
        df.loc[df['modifier'] == '', 'modifier'] = None  # Empty string → None
    
    if 'status_code' in df.columns:
        df['status_code'] = df['status_code'].astype(str).str.strip().str.upper()
    
    # RVUs as float64 (precision handled in canonicalize step)
    rvu_cols = ['work_rvu', 'pe_rvu_nonfac', 'pe_rvu_fac', 'mp_rvu']
    for col in rvu_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Apply schema-driven precision (2 decimals, HALF_UP)
            df[col] = canonicalize_numeric_col(df[col], precision=2, rounding='HALF_UP')
    
    # Global days as int
    if 'global_days' in df.columns:
        df['global_days'] = pd.to_numeric(df['global_days'], errors='coerce').fillna(0).astype('Int64')
    
    # Dates - use metadata vintage_date if effective_from not in file
    if 'effective_from' not in df.columns:
        df['effective_from'] = metadata.get('vintage_date', pd.Timestamp('2025-01-01'))
    elif 'effective_from' in df.columns:
        df['effective_from'] = pd.to_datetime(df['effective_from'], errors='coerce')
        # Fill NaT with vintage_date
        df['effective_from'] = df['effective_from'].fillna(metadata.get('vintage_date', pd.Timestamp('2025-01-01')))
    
    return df
