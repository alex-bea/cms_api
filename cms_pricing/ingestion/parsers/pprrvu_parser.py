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
import re

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
    compute_row_id
)
from cms_pricing.ingestion.parsers.layout_registry import get_layout
import json
from pathlib import Path

logger = structlog.get_logger()

PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_pprrvu_v1.1"
NATURAL_KEYS = ["hcpcs", "modifier", "status_code", "effective_from"]

CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")


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

    if 'status_code' not in df.columns:
        fallback = None
        if 'status' in df.columns:
            fallback = df['status']
        elif 'statusindicator' in df.columns:
            fallback = df['statusindicator']

        if fallback is not None:
            df['status_code'] = fallback.astype(str).str.strip().str.upper()
        else:
            df['status_code'] = 'NONE'

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
    
    min_length = layout.get('min_line_length', 165)
    
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


def _promote_header_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Promote the first row containing the HCPCS column to the header.
    """
    if df.empty:
        return df

    def _clean(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return value
        text = str(value)
        text = CONTROL_CHAR_RE.sub('', text)
        return text.strip()

    df = df.applymap(_clean)

    header_idx = None
    for idx in range(len(df)):
        first_val = str(df.iloc[idx, 0]).strip().lower()
        if first_val == 'hcpcs':
            header_idx = idx
            break

    if header_idx is not None:
        header = df.iloc[header_idx].fillna('').astype(str).map(lambda x: x.strip())
        df = df.iloc[header_idx + 1:].copy()
        df.columns = header
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # Drop any residual rows that are entirely blank after header promotion
    df = df[~(df.apply(lambda row: all((str(val).strip() == '' for val in row)), axis=1))]
    hcpcs_col = next((col for col in df.columns if str(col).strip().lower() == 'hcpcs'), None)
    if hcpcs_col is not None:
        df = df[df[hcpcs_col].notna()]
        df = df[df[hcpcs_col].astype(str).str.strip() != '']

    return df.reset_index(drop=True)


def _parse_csv(content: bytes, encoding: str) -> pd.DataFrame:
    """
    Parse CSV format with header detection.
    
    Args:
        content: File bytes
        encoding: Detected encoding
        
    Returns:
        DataFrame with raw parsed data
    """
    text = content.decode(encoding, errors='replace')
    df = pd.read_csv(StringIO(text), header=None, dtype=str)
    return _promote_header_row(df)


def _parse_xlsx(content: bytes) -> pd.DataFrame:
    """
    Parse XLSX format.
    
    Args:
        content: File bytes
        
    Returns:
        DataFrame with raw parsed data
    """
    df = pd.read_excel(BytesIO(content), sheet_name=0, header=None, dtype=str)
    return _promote_header_row(df)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to schema contract format.
    
    Fixed-width layout already uses schema names (rvu_work, etc.) - skip normalization.
    CSV/XLSX may use CMS header variations - apply aliases to schema format.
    
    Schema format (DB canonical): rvu_work, rvu_pe_nonfac, rvu_pe_fac, rvu_malp
    API format (presentation): work_rvu, pe_rvu_nonfac, pe_rvu_fac, mp_rvu
    
    Parser produces schema format. API layer transforms for presentation.
    
    Args:
        df: Raw DataFrame
        
    Returns:
        DataFrame with schema-canonical column names
    """
    # Check if columns already canonical (from fixed-width layout)
    schema_cols = {'hcpcs', 'modifier', 'status_code', 'rvu_work', 'rvu_pe_nonfac', 'rvu_pe_fac', 'rvu_malp'}
    
    if schema_cols.issubset(set(df.columns)):
        # Layout already uses schema names - no normalization needed
        logger.debug("Columns already canonical from layout")
        return df
    
    # Apply aliases for CSV/XLSX formats (map both old and new variants to schema)
    COLUMN_ALIASES = {
        # HCPCS/Codes
        'HCPCS': 'hcpcs',
        'HCPCS_CODE': 'hcpcs',
        'HCPCS CODE': 'hcpcs',
        'CPT': 'hcpcs',
        'CPT/HCPCS': 'hcpcs',
        
        # Modifier
        'MOD': 'modifier',
        'MODIFIER': 'modifier',
        'MOD1': 'modifier',
        
        # Status
        'STATUS': 'status_code',
        'status code': 'status_code',
        'STATUS_CODE': 'status_code',
        'CODE': 'status_code',
        'STAT': 'status_code',
        
        # Work RVU (support both API and schema formats)
        'WORK_RVU': 'rvu_work',  # API format
        'WORK RVU': 'rvu_work',
        'RVU_WORK': 'rvu_work',  # Schema format
        'WORK': 'rvu_work',
        
        # PE Non-Fac RVU
        'PE_NONFAC_RVU': 'rvu_pe_nonfac',  # API format
        'PE NONFAC RVU': 'rvu_pe_nonfac',
        'PE_RVU_NONFAC': 'rvu_pe_nonfac',
        'RVU_PE_NONFAC': 'rvu_pe_nonfac',  # Schema format
        'NON_FAC_PE_RVU': 'rvu_pe_nonfac',
        
        # PE Fac RVU
        'PE_FAC_RVU': 'rvu_pe_fac',  # API format
        'PE FAC RVU': 'rvu_pe_fac',
        'PE_RVU_FAC': 'rvu_pe_fac',
        'RVU_PE_FAC': 'rvu_pe_fac',  # Schema format
        'FAC_PE_RVU': 'rvu_pe_fac',
        
        # Malpractice RVU
        'MP_RVU': 'rvu_malp',  # API format (mp)
        'MALPRACTICE_RVU': 'rvu_malp',
        'MALPRACTICE RVU': 'rvu_malp',
        'MALP_RVU': 'rvu_malp',
        'RVU_MALP': 'rvu_malp',  # Schema format
        
        # Other fields
        'GLOBAL': 'global_days',
        'GLOBAL_DAYS': 'global_days',
        'NA_IND': 'na_indicator',
        'NA_INDICATOR': 'na_indicator',
        'OPPS_CAP': 'opps_cap_applicable',
        'OPPS_CAP_IND': 'opps_cap_applicable',
        'EFFECTIVE_DATE': 'effective_from',
        'EFFECTIVE_FROM': 'effective_from',
        'EFFECTIVE': 'effective_from',
        'FACTOR': 'conversion_factor',
        'BASE': 'endoscopic_base',
    }
    
    df = df.copy()
    df.columns = [
        COLUMN_ALIASES.get(str(c).strip().upper(), str(c).lower().strip().replace(' ', '_'))
        for c in df.columns
    ]
    return _apply_multiheader_aliases(df)


def _apply_multiheader_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle multi-row CMS headers (e.g., 2025 CSV layout) where repeated column
    labels like 'RVU' and 'PE RVU' correspond to different schema fields.

    We detect duplicates such as ['rvu', 'pe_rvu', 'pe_rvu', 'rvu', 'total', 'total']
    and hydrate the canonical schema columns if they are missing or empty.
    """
    columns = list(df.columns)

    def _indices(name: str) -> list[int]:
        return [idx for idx, col in enumerate(columns) if col == name]

    def _series_at(name: str, occurrence: int) -> Optional[pd.Series]:
        matches = _indices(name)
        if len(matches) <= occurrence:
            return None
        return df.iloc[:, matches[occurrence]].copy()

    def _needs_backfill(col: str) -> bool:
        if col not in df.columns:
            return True
        series = df[col]
        if series.empty:
            return True
        # Treat empty strings and NaNs as missing
        return series.replace({"": pd.NA}).isna().all()

    # Map duplicated RVU columns → canonical schema columns
    def _assign(target: str, source: str, occurrence: int = 0) -> None:
        if not _needs_backfill(target):
            return
        src = _series_at(source, occurrence)
        if src is None:
            return
        df[target] = src
        logger.info(
            "pprrvu_multiheader_backfill",
            target_column=target,
            source_column=source,
            occurrence=occurrence,
            non_null=int(src.replace({"": pd.NA}).dropna().shape[0]),
            total=len(src),
        )

    _assign("rvu_work", "rvu", 0)
    _assign("rvu_malp", "rvu", 1)
    _assign("rvu_pe_nonfac", "pe_rvu", 0)
    _assign("rvu_pe_fac", "pe_rvu", 1)
    _assign("total_nonfac", "total", 0)
    _assign("total_fac", "total", 1)
    _assign("na_indicator", "indicator", 0)

    if _needs_backfill("diag_imaging_family") and "family" in df.columns:
        series = df["family"].copy()
        df["diag_imaging_family"] = series
        logger.info(
            "pprrvu_multiheader_backfill",
            target_column="diag_imaging_family",
            source_column="family",
            occurrence=0,
            non_null=int(series.replace({"": pd.NA}).dropna().shape[0]),
            total=len(series),
        )

    # Normalize indicator-style columns: treat empty strings as nulls
    indicator_columns = [
        "na_indicator",
        "opps_cap_applicable",
        "bilateral_ind",
        "multiple_proc_ind",
        "assistant_surg_ind",
        "co_surg_ind",
        "team_surg_ind",
        "endoscopic_base",
    ]
    for col in indicator_columns:
        if col in df.columns:
            df[col] = df[col].replace({"": pd.NA})

    return df


def _cast_dtypes(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """
    Cast columns to explicit dtypes using schema contract names.
    
    Schema format (DB canonical): rvu_work, rvu_pe_nonfac, rvu_pe_fac, rvu_malp
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
    
    # RVUs as float64 with schema precision
    precision_map = {
        'rvu_work': 2,
        'rvu_pe_nonfac': 2,
        'rvu_pe_fac': 2,
        'rvu_malp': 2,
        'total_nonfac': 2,
        'total_fac': 2,
        'conversion_factor': 4,
    }
    for col, precision in precision_map.items():
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors='coerce')
            df[col] = numeric.round(precision)

    if 'na_indicator' in df.columns:
        df['na_indicator'] = df['na_indicator'].astype('string').str.strip().replace({'': pd.NA}).str.upper()

    if 'opps_cap_applicable' in df.columns:
        df['opps_cap_applicable'] = (
            df['opps_cap_applicable']
            .astype('string')
            .str.strip()
            .replace({'': pd.NA})
            .map({'Y': True, 'N': False})
            .astype('boolean')
        )

    # Global days as int
    if 'global_days' in df.columns:
        df['global_days'] = pd.to_numeric(df['global_days'], errors='coerce').fillna(0).astype('Int64')
    
    # Dates - inject from metadata (not in fixed-width file)
    if 'effective_from' not in df.columns:
        df['effective_from'] = metadata.get('vintage_date', pd.Timestamp('2025-01-01'))
    else:
        df['effective_from'] = pd.to_datetime(df['effective_from'], errors='coerce')
        df['effective_from'] = df['effective_from'].fillna(metadata.get('vintage_date', pd.Timestamp('2025-01-01')))
    
    return df
