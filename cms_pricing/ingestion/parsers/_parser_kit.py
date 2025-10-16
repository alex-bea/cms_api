"""
Shared parser utilities (internal).

Centralizes common logic across all CMS parsers to prevent drift.
Following STD-parser-contracts-prd-v1.1.

All parsers (PPRRVU, GPCI, Locality, ANES, OPPSCAP, CF) use these utilities for:
- Row content hashing (formal spec from §5.2 - 64-char SHA-256 with schema-driven precision)
- Metadata injection (§6.4)
- Dtype enforcement
- Deterministic output (sorting + hashing)
- Encoding detection (UTF-8 → CP1252 → Latin-1)
- ParseResult return type (data, rejects, metrics)
"""

import hashlib
import codecs
import pandas as pd
from typing import List, Dict, Any, Tuple, NamedTuple, Optional
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN
from enum import Enum
import structlog

logger = structlog.get_logger()


# ============================================================================
# Custom Exceptions (Phase 1 Enhancement)
# ============================================================================

class ParseError(Exception):
    """Base exception for parser errors."""
    pass


class DuplicateKeyError(ParseError):
    """Raised when duplicate natural keys are detected."""
    def __init__(self, message: str, duplicates: Optional[List[Dict]] = None):
        super().__init__(message)
        self.duplicates = duplicates or []


class CategoryValidationError(ParseError):
    """Raised when categorical validation fails (unknown values)."""
    def __init__(self, field: str, invalid_values: List[Any]):
        self.field = field
        self.invalid_values = invalid_values
        super().__init__(
            f"Categorical validation failed for field '{field}': "
            f"invalid values {invalid_values}"
        )


class LayoutMismatchError(ParseError):
    """Raised when fixed-width layout doesn't match file structure."""
    pass


class SchemaRegressionError(ParseError):
    """Raised when schema contains unexpected/banned fields."""
    def __init__(self, message: str, unexpected_fields: Optional[List[str]] = None):
        super().__init__(message)
        self.unexpected_fields = unexpected_fields or []


# ============================================================================
# Validation Enums (Phase 0 Commit 4)
# ============================================================================

class ValidationSeverity(str, Enum):
    """
    Validation severity levels per STD-parser-contracts v1.1 §8.3.
    
    Type-safe enum prevents string typos ("WARN" vs "warn").
    
    Severity Levels:
        BLOCK: Raise exception, stop processing (critical errors)
        WARN: Quarantine rows, continue processing (soft failures)
        INFO: Log only, continue (statistical anomalies)
    
    Examples:
        >>> severity = ValidationSeverity.WARN
        >>> assert severity == "WARN"  # String comparison still works
        >>> assert severity.value == "WARN"
    """
    BLOCK = "BLOCK"
    WARN = "WARN"
    INFO = "INFO"


class CategoricalRejectReason(str, Enum):
    """
    Machine-readable reason codes for categorical validation failures.
    
    Enables precise error tracking, automated remediation, and metrics.
    
    Reason Codes:
        UNKNOWN_VALUE: Value not in allowed enum list
        NULL_NOT_ALLOWED: Null value when nullable=false
        CASE_MISMATCH: Case differs from canonical (future)
        NORMALIZED: Value was normalized (future)
    
    Examples:
        >>> reason = CategoricalRejectReason.UNKNOWN_VALUE
        >>> assert reason == "CAT_UNKNOWN_VALUE"
    """
    UNKNOWN_VALUE = "CAT_UNKNOWN_VALUE"
    NULL_NOT_ALLOWED = "CAT_NULL_NOT_ALLOWED"
    CASE_MISMATCH = "CAT_CASE_MISMATCH"
    NORMALIZED = "CAT_NORMALIZED"


class ValidationResult(NamedTuple):
    """
    Categorical validation output per Phase 0 Commit 4.
    
    Structured result with full provenance and metrics.
    Replaces bare tuple return for type safety and clarity.
    
    Attributes:
        valid_df: DataFrame with valid rows (categorical dtypes applied)
        rejects_df: DataFrame with rejected rows (validation failures with metadata)
        metrics: Dict with validation metrics (reject_rate, columns, etc.)
    
    Examples:
        >>> result = enforce_categorical_dtypes(df, schema, ValidationSeverity.WARN)
        >>> print(f"Valid: {len(result.valid_df)}, Rejects: {len(result.rejects_df)}")
        >>> print(f"Reject rate: {result.metrics['reject_rate']:.2%}")
    """
    valid_df: pd.DataFrame
    rejects_df: pd.DataFrame
    metrics: Dict[str, Any]

# Rounding mode mapping
ROUNDING_MODES = {
    "HALF_UP": ROUND_HALF_UP,
    "HALF_EVEN": ROUND_HALF_EVEN,
}


class ParseResult(NamedTuple):
    """
    Structured parser output per STD-parser-contracts v1.1 §5.3
    
    Attributes:
        data: Canonical DataFrame (valid rows with metadata + row_content_hash)
        rejects: Rejected rows DataFrame (validation failures, empty if all valid)
        metrics: Parse metrics dict (total_rows, valid_rows, reject_rows, etc.)
    """
    data: pd.DataFrame
    rejects: pd.DataFrame
    metrics: Dict[str, Any]


def build_precision_map(schema: Dict[str, Any]) -> Dict[str, Tuple[int, str]]:
    """
    Extract precision and rounding mode from schema contract.
    
    Per STD-parser-contracts v1.1 §5.2, numeric columns have precision and
    rounding_mode fields for deterministic hash computation.
    
    Args:
        schema: Loaded schema contract (JSON dict)
        
    Returns:
        Dict mapping column name to (precision, rounding_mode)
        
    Example:
        >>> schema = json.load(open('cms_pprrvu_v1.1.json'))
        >>> precision_map = build_precision_map(schema)
        >>> precision_map['work_rvu']
        (2, 'HALF_UP')
    """
    precision_map = {}
    
    for col_name, col_def in schema.get('columns', {}).items():
        if col_def.get('type') in ['float64', 'number']:
            precision = col_def.get('precision', 6)  # Default 6dp if not specified
            rounding = col_def.get('rounding_mode', 'HALF_UP')  # Default HALF_UP
            precision_map[col_name] = (precision, rounding)
    
    return precision_map


def canonicalize_numeric_col(
    series: pd.Series, 
    precision: int, 
    rounding_mode: str
) -> pd.Series:
    """
    Canonicalize numeric column using Decimal for deterministic rounding.
    
    Uses Decimal arithmetic (not binary float) to ensure deterministic rounding
    across platforms and Python versions per STD-parser-contracts v1.1.
    
    Args:
        series: Numeric pandas Series
        precision: Decimal places for rounding
        rounding_mode: 'HALF_UP' or 'HALF_EVEN'
        
    Returns:
        Series of canonical strings (fixed decimal places)
        
    Example:
        >>> s = pd.Series([0.1234567, 1.005])
        >>> canonicalize_numeric_col(s, 2, 'HALF_UP')
        0    0.12
        1    1.01
        dtype: object
    """
    quantizer = Decimal("1." + "0" * precision)
    rounding = ROUNDING_MODES.get(rounding_mode, ROUND_HALF_UP)
    
    def format_decimal(x):
        if pd.isna(x):
            return ""
        # Use Decimal for deterministic rounding (not binary float)
        decimal_val = Decimal(str(x)).quantize(quantizer, rounding=rounding)
        return f"{decimal_val:.{precision}f}"
    
    return series.map(format_decimal)


def compute_row_hashes_vectorized(
    df: pd.DataFrame,
    column_order: List[str],
    schema: Dict[str, Any]
) -> pd.Series:
    """
    Compute row hashes vectorized (10-100x faster than row-wise apply).
    
    Per STD-parser-contracts v1.1 §5.2:
    - Uses column_order from schema for deterministic ordering
    - Reads precision/rounding per column from schema
    - Normalizes numerics with Decimal (not binary float)
    - Joins with \x1f (unit separator), SHA-256, full 64-char hex
    
    Args:
        df: DataFrame to hash
        column_order: Columns to include in hash (from schema, excludes metadata)
        schema: Schema contract dict (for precision/rounding)
        
    Returns:
        Series of 64-character SHA-256 hex hashes
        
    Performance: Target <100ms for 10K rows (vs 2-3s with apply)
    
    Example:
        >>> schema = json.load(open('cms_pprrvu_v1.1.json'))
        >>> hashes = compute_row_hashes_vectorized(df, schema['column_order'], schema)
        >>> assert all(len(h) == 64 for h in hashes)
    """
    # Build precision map from schema
    precision_map = build_precision_map(schema)
    
    # Pre-normalize each column to canonical strings (vectorized)
    normalized = []
    
    for col in column_order:
        if col not in df.columns:
            # Column missing from DataFrame - use empty strings
            normalized.append(pd.Series([''] * len(df), index=df.index))
            continue
        
        series = df[col]
        col_def = schema.get('columns', {}).get(col, {})
        col_type = col_def.get('type', 'str')
        
        # Vectorized normalization by type
        if col_type in ['float64', 'number'] or col in precision_map:
            precision, rounding_mode = precision_map.get(col, (6, 'HALF_UP'))
            canon_series = canonicalize_numeric_col(series, precision, rounding_mode)
            normalized.append(canon_series)
        
        elif col_type == 'boolean' or pd.api.types.is_bool_dtype(series):
            # Boolean → 'True'/'False' strings
            canon_series = series.fillna('').map(
                lambda v: 'True' if v else 'False' if v is not None else ''
            )
            normalized.append(canon_series)
        
        elif pd.api.types.is_datetime64_any_dtype(series):
            # Datetime → ISO-8601 UTC
            canon_series = series.fillna('').map(
                lambda v: v.strftime('%Y-%m-%dT%H:%M:%SZ') if pd.notna(v) else ''
            )
            normalized.append(canon_series)
        
        elif pd.api.types.is_categorical_dtype(series):
            # Categorical → string (avoid category code drift)
            canon_series = series.astype(str).fillna('').str.strip()
            normalized.append(canon_series)
        
        else:
            # String/other → trimmed string
            canon_series = series.fillna('').astype(str).str.strip()
            normalized.append(canon_series)
    
    # Vectorized join with \x1f (unit separator)
    if not normalized:
        # No columns to hash
        return pd.Series([''] * len(df), index=df.index)
    
    joined = normalized[0]
    for norm_series in normalized[1:]:
        joined = joined + '\x1f' + norm_series
    
    # Vectorized hash: Full 64-char SHA-256 hex digest
    def hash_content(content_str):
        return hashlib.sha256(content_str.encode('utf-8')).hexdigest()
    
    return joined.map(hash_content)


def compute_row_hash(row: pd.Series, schema_columns: List[str]) -> str:
    """
    DEPRECATED: Use compute_row_hashes_vectorized() for v1.1 compatibility.
    
    Legacy row-wise hash function (v1.0 - 16-char hash).
    Kept for backwards compatibility with old releases.
    
    For new parsers, use compute_row_hashes_vectorized() which:
    - Returns 64-char SHA-256 (not 16-char)
    - Uses schema-driven precision (not hardcoded 6dp)
    - Is 10-100x faster (vectorized)
    
    Args:
        row: DataFrame row (pd.Series)
        schema_columns: Column names in schema-declared order
        
    Returns:
        16-character hex hash (LEGACY)
    """
    logger.warning("Using deprecated compute_row_hash (v1.0). Use compute_row_hashes_vectorized for v1.1.")
    
    parts = []
    
    for col in schema_columns:
        if col not in row.index:
            parts.append("")
            continue
        
        val = row[col]
        
        if pd.isna(val):
            parts.append("")
        elif isinstance(val, (float, Decimal)):
            parts.append(f"{float(val):.6f}")
        elif isinstance(val, datetime):
            parts.append(val.strftime('%Y-%m-%dT%H:%M:%SZ'))
        elif isinstance(val, date):
            parts.append(val.isoformat())
        elif hasattr(val, 'categories'):
            parts.append(str(val).strip())
        elif isinstance(val, bool):
            parts.append('True' if val else 'False')
        else:
            parts.append(str(val).strip())
    
    content = '\x1f'.join(parts)
    hash_bytes = hashlib.sha256(content.encode('utf-8')).digest()
    
    # LEGACY: Return first 16 chars only
    return hash_bytes.hex()[:16]


def inject_metadata(
    df: pd.DataFrame,
    metadata: Dict[str, Any],
    filename: str
) -> pd.DataFrame:
    """
    Inject required metadata columns per STD-parser-contracts §6.4.
    
    Required metadata from ingestor:
    - release_id, vintage_date, product_year, quarter_vintage
    
    Columns added:
    - All metadata fields above
    - source_filename, source_file_sha256, source_uri
    - parsed_at (timestamp)
    
    Args:
        df: Parsed DataFrame
        metadata: Metadata dict from ingestor
        filename: Source filename
        
    Returns:
        DataFrame with metadata columns added
    """
    df = df.copy()
    
    # Three vintage fields (REQUIRED per DIS standards)
    df['vintage_date'] = metadata['vintage_date']
    df['product_year'] = metadata['product_year']
    df['quarter_vintage'] = metadata['quarter_vintage']
    
    # Core identity
    df['release_id'] = metadata['release_id']
    
    # Provenance
    df['source_filename'] = filename
    df['source_file_sha256'] = metadata.get('file_sha256', 'unknown')
    df['source_uri'] = metadata.get('source_uri', '')
    
    # Timestamp
    df['parsed_at'] = datetime.utcnow()
    
    return df


def finalize_parser_output(
    df: pd.DataFrame,
    natural_key_cols: List[str],
    schema: Dict[str, Any]
) -> pd.DataFrame:
    """
    Finalize parser output per STD-parser-contracts v1.1 §5.2.
    
    Steps:
    1. Sort by natural key (deterministic)
    2. Reset index to 0, 1, 2, ...
    3. Compute 64-char row_content_hash (vectorized, schema-driven precision)
    
    Args:
        df: Parsed DataFrame with metadata
        natural_key_cols: Columns for sorting (natural key from schema)
        schema: Schema contract dict (for column_order, precision, rounding)
        
    Returns:
        Finalized DataFrame with 64-char row_content_hash column
        
    Performance: Vectorized hashing is 10-100x faster than row-wise apply
    """
    # Sort by natural key (stable, deterministic)
    df = df.sort_values(
        by=natural_key_cols, 
        na_position='last'
    ).reset_index(drop=True)
    
    # Compute row content hash (VECTORIZED for performance)
    column_order = schema.get('column_order', [])
    df['row_content_hash'] = compute_row_hashes_vectorized(df, column_order, schema)
    
    return df


def detect_encoding(content: bytes) -> Tuple[str, bytes]:
    """
    Detect encoding and strip BOM per STD-parser-contracts §5.2.
    
    Supports:
    - UTF-8 (with BOM)
    - UTF-16 LE/BE
    - Latin-1 (fallback)
    - CP1252 (fallback)
    
    Args:
        content: File bytes
        
    Returns:
        (encoding, content_without_bom)
        
    Examples:
        >>> encoding, clean = detect_encoding(b'\\xef\\xbb\\xbfdata')
        >>> encoding
        'utf-8'
        >>> clean
        b'data'
    """
    # Check for BOM markers
    if content.startswith(b'\xef\xbb\xbf'):  # UTF-8 BOM
        logger.debug("Detected UTF-8 BOM, stripping")
        return 'utf-8', content[3:]
    
    elif content.startswith(b'\xff\xfe'):  # UTF-16 LE BOM
        logger.debug("Detected UTF-16 LE BOM, stripping")
        return 'utf-16-le', content[2:]
    
    elif content.startswith(b'\xfe\xff'):  # UTF-16 BE BOM
        logger.debug("Detected UTF-16 BE BOM, stripping")
        return 'utf-16-be', content[2:]
    
    # No BOM - try UTF-8 first
    try:
        content.decode('utf-8')
        logger.debug("Detected UTF-8 encoding (no BOM)")
        return 'utf-8', content
    except UnicodeDecodeError:
        pass
    
    # Try CP1252 (common in Windows files)
    try:
        content.decode('cp1252')
        logger.debug("Detected CP1252 encoding")
        return 'cp1252', content
    except UnicodeDecodeError:
        pass
    
    # Fallback to Latin-1 (never fails, maps bytes 1:1)
    logger.warning("Falling back to Latin-1 encoding")
    return 'latin-1', content


def is_fixed_width_format(content: bytes, filename: str) -> bool:
    """
    Detect if file is fixed-width format.
    
    Heuristic:
    - Filename ends with .txt
    - First line has no common delimiters (comma, tab, pipe)
    
    Args:
        content: File bytes
        filename: Filename hint
        
    Returns:
        True if fixed-width format detected
    """
    # TXT files are candidates for fixed-width
    if not filename.endswith('.txt'):
        return False
    
    # Decode first line
    encoding, clean_content = detect_encoding(content)
    
    try:
        text = clean_content.decode(encoding)
        first_line = text.split('\n')[0]
        
        # Fixed-width has no delimiters
        has_delimiters = (
            ',' in first_line or 
            '\t' in first_line or 
            '|' in first_line
        )
        
        return not has_delimiters
        
    except Exception as e:
        logger.warning(f"Failed to detect format: {e}")
        # Assume fixed-width for .txt files if detection fails
        return True


def create_quarantine_artifact(
    rejected_df: pd.DataFrame,
    release_id: str,
    dataset: str,
    reason: str,
    rule_id: str = None
) -> str:
    """
    Create quarantine artifact per STD-parser-contracts §8.4.
    
    v1.0: Helper function writes artifact
    v1.1: Will return rejected rows for ingestor to write
    
    Args:
        rejected_df: Rows to quarantine
        release_id: Release identifier
        dataset: Dataset name
        reason: Human-readable reason
        rule_id: Rule identifier (optional)
        
    Returns:
        Path to quarantine file
    """
    if len(rejected_df) == 0:
        return None
    
    from pathlib import Path
    
    quarantine_dir = Path(f"data/quarantine/{release_id}")
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    quarantine_path = quarantine_dir / f"{dataset}_{reason}.parquet"
    
    # Add quarantine metadata
    rejected_df = rejected_df.copy()
    rejected_df['quarantine_reason'] = reason
    rejected_df['quarantine_rule_id'] = rule_id or 'unknown'
    rejected_df['quarantined_at'] = datetime.utcnow()
    rejected_df['quarantine_dataset'] = dataset
    
    # Write to Parquet
    rejected_df.to_parquet(quarantine_path, index=False)
    
    logger.info(
        "Quarantine artifact created",
        path=str(quarantine_path),
        rows=len(rejected_df),
        reason=reason
    )
    
    return str(quarantine_path)


def build_parse_metrics(
    filename: str,
    dataset: str,
    schema_id: str,
    parser_version: str,
    layout_version: str,
    encoding: str,
    rows_in: int,
    rows_out: int,
    rejects_count: int,
    parse_duration_sec: float,
    **extra
) -> Dict[str, Any]:
    """
    Build standard parse metrics dict per STD-parser-contracts §10.1.
    
    Args:
        filename: Source filename
        dataset: Dataset name
        schema_id: Schema contract ID
        parser_version: Parser version
        layout_version: Layout version (for fixed-width)
        encoding: Detected encoding
        rows_in: Input row count
        rows_out: Output row count
        rejects_count: Rejected row count
        parse_duration_sec: Parse duration
        **extra: Additional metrics
        
    Returns:
        Metrics dictionary
    """
    return {
        'filename': filename,
        'dataset': dataset,
        'schema_id': schema_id,
        'parser_version': parser_version,
        'layout_version': layout_version,
        'encoding_detected': encoding,
        'rows_in': rows_in,
        'rows_out': rows_out,
        'rejects_count': rejects_count,
        'reject_rate': rejects_count / rows_in if rows_in > 0 else 0,
        'parse_seconds': round(parse_duration_sec, 2),
        **extra
    }


# Export public kit API
__all__ = [
    # v1.1 Production API
    'ParseResult',
    'compute_row_hashes_vectorized',
    'build_precision_map',
    'canonicalize_numeric_col',
    'inject_metadata',
    'enforce_categorical_dtypes',
    'finalize_parser_output',
    'detect_encoding',
    'is_fixed_width_format',
    'create_quarantine_artifact',
    'build_parse_metrics',
    # Legacy (backwards compat)
    'compute_row_hash',  # Deprecated - use compute_row_hashes_vectorized
]




def compute_row_id(row: pd.Series, natural_keys: List[str]) -> str:
    """
    Compute deterministic row_id from natural keys for deduplication and lineage.
    
    Per Phase 0 Commit 3: Schema-driven natural keys + row_id.
    
    Used for:
    - Uniqueness checking (duplicate detection)
    - Change detection (compare across vintages)
    - Lineage tracking (row-level provenance)
    
    Args:
        row: DataFrame row
        natural_keys: Columns forming natural key (from schema contract)
        
    Returns:
        64-char SHA-256 hex digest of natural key values
        
    Examples:
        >>> row_id = compute_row_id(row, ['hcpcs', 'modifier', 'effective_from'])
        >>> len(row_id)
        64
    """
    from datetime import datetime, date
    
    parts = []
    for col in natural_keys:
        val = row[col]
        if pd.isna(val):
            parts.append("")
        elif isinstance(val, datetime):
            parts.append(val.strftime('%Y-%m-%d'))
        elif isinstance(val, date):
            parts.append(val.isoformat())
        else:
            parts.append(str(val).strip())
    
    content = '\x1f'.join(parts)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def check_natural_key_uniqueness(
    df: pd.DataFrame,
    natural_keys: List[str],
    severity: ValidationSeverity = ValidationSeverity.WARN,
    schema_id: Optional[str] = None,
    release_id: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Check for duplicate natural keys and compute row_id.
    
    Per Phase 0 Commit 3 + Phase 1 Enhancement:
    - Computes row_id for all rows (SHA-256 of natural keys)
    - Detects duplicates (same natural key)
    - Returns (unique_df, duplicates_df) if severity=WARN
    - Raises DuplicateKeyError if severity=BLOCK
    
    Args:
        df: Input DataFrame
        natural_keys: Columns forming natural key
        severity: BLOCK (raise error) or WARN (return rejects) - default WARN
        schema_id: Schema ID for reject provenance (optional)
        release_id: Release ID for reject provenance (optional)
        
    Returns:
        (unique_df, duplicates_df) - Separated unique and duplicate rows
        
    Raises:
        DuplicateKeyError: If severity=BLOCK and duplicates found
        
    Examples:
        >>> # Soft failure (default):
        >>> unique_df, dupes_df = check_natural_key_uniqueness(df, ['hcpcs', 'modifier'])
        >>> print(f"Found {len(dupes_df)} duplicates")
        
        >>> # Hard failure:
        >>> unique_df, dupes_df = check_natural_key_uniqueness(
        ...     df, ['hcpcs', 'modifier'], 
        ...     severity=ValidationSeverity.BLOCK,
        ...     schema_id='cms_pprrvu_v1.1',
        ...     release_id='mpfs_2025_q4'
        ... )
        DuplicateKeyError: Duplicate natural keys detected: 5 duplicates
    """
    # Compute row_id for all rows
    df = df.copy()
    df['row_id'] = df.apply(lambda r: compute_row_id(r, natural_keys), axis=1)
    
    # Find duplicates
    duplicate_mask = df.duplicated(subset='row_id', keep=False)
    
    if duplicate_mask.any():
        duplicates = df[duplicate_mask].copy()
        duplicates['validation_error'] = 'Duplicate natural key'
        duplicates['validation_severity'] = severity.value
        duplicates['validation_rule_id'] = 'NATURAL_KEY_DUPLICATE'
        duplicates['validation_context'] = duplicates[natural_keys].astype(str).to_dict('records')
        
        if schema_id:
            duplicates['schema_id'] = schema_id
        if release_id:
            duplicates['release_id'] = release_id
        
        unique_df = df[~duplicate_mask].copy()
        
        logger.warning(
            f"Natural key duplicates found: {duplicate_mask.sum()} rows",
            natural_keys=natural_keys,
            duplicate_count=duplicate_mask.sum(),
            severity=severity.value
        )
        
        # If BLOCK severity, raise exception
        if severity == ValidationSeverity.BLOCK:
            duplicate_keys = duplicates[natural_keys].drop_duplicates().to_dict(orient='records')
            raise DuplicateKeyError(
                f"Duplicate natural keys detected: {duplicate_mask.sum()} duplicates",
                duplicates=duplicate_keys
            )
        
        return unique_df, duplicates
    
    return df, pd.DataFrame()


def get_categorical_columns(schema_contract: Dict) -> Dict[str, Dict[str, Any]]:
    """
    Extract categorical column specifications from schema contract.
    
    Per Phase 0 Commit 4: Schema-driven categorical validation.
    
    Args:
        schema_contract: Loaded JSON schema contract
        
    Returns:
        {column_name: {enum: [...], nullable: bool}}
        
    Examples:
        >>> schema = json.load(open('cms_pprrvu_v1.0.json'))
        >>> cats = get_categorical_columns(schema)
        >>> print(cats['modifier']['enum'])
        ['', '26', 'TC', '53', ...]
    """
    categorical = {}
    for col_name, col_spec in schema_contract.get("columns", {}).items():
        if col_spec.get("type") == "categorical":
            categorical[col_name] = {
                "enum": col_spec.get("enum", []),
                "nullable": col_spec.get("nullable", True)
            }
    return categorical


def enforce_categorical_dtypes(
    df: pd.DataFrame,
    schema_contract: Dict,
    natural_keys: List[str],
    schema_id: Optional[str] = None,
    release_id: Optional[str] = None,
    severity: ValidationSeverity = ValidationSeverity.WARN
) -> ValidationResult:
    """
    Enforce categorical dtypes with spec-driven validation.
    
    **NO SILENT NaN COERCION** per STD-parser-contracts v1.1 §8.2.1
    
    Per Phase 0 Commit 4 (Enhanced):
    - Reads enum/nullable from schema contract
    - Returns ValidationResult (not bare tuple)
    - Adds row_id, schema_id, release_id to rejects for provenance
    - Deterministic ordering (column, reason_code, row_id)
    - Comprehensive metrics
    
    Args:
        df: Input DataFrame
        schema_contract: Loaded schema contract (contains enum values, nullable)
        natural_keys: Natural key columns (for row_id computation)
        schema_id: Schema contract ID (for provenance)
        release_id: Release ID (for provenance)
        severity: BLOCK (raise) or WARN (quarantine) - uses enum!
        
    Returns:
        ValidationResult with:
            - valid_df: Valid rows with categorical dtypes applied
            - rejects_df: Rejected rows with full metadata
            - metrics: Validation metrics
        
    Raises:
        ValueError: If severity=BLOCK and invalid values found
        
    Rejects Schema:
        - row_id: SHA-256 of natural keys (for tracking)
        - schema_id: Schema contract identifier
        - release_id: Release identifier
        - validation_error: Human-readable message
        - validation_severity: BLOCK/WARN/INFO
        - validation_rule_id: Machine-readable reason code
        - validation_column: Column name
        - validation_context: Invalid value captured
        
    Examples:
        >>> schema = json.load(open('cms_pprrvu_v1.0.json'))
        >>> result = enforce_categorical_dtypes(
        ...     df, schema, ['hcpcs', 'modifier'],
        ...     schema_id='cms_pprrvu_v1.0',
        ...     release_id='mpfs_2025_q1',
        ...     severity=ValidationSeverity.WARN
        ... )
        >>> print(f"Valid: {len(result.valid_df)}, Rejects: {len(result.rejects_df)}")
        >>> print(result.rejects_df[['validation_rule_id', 'validation_context']])
    """
    rejects_list = []
    valid_df = df.copy()
    reject_counts_by_column = {}
    
    # Extract categorical columns from schema contract
    categorical_cols = get_categorical_columns(schema_contract)
    
    for col_name, col_spec in categorical_cols.items():
        if col_name not in valid_df.columns:
            continue
        
        allowed_values = col_spec["enum"]
        nullable = col_spec["nullable"]
        
        # Check 1: Null constraint
        if not nullable:
            null_mask = valid_df[col_name].isna()
            if null_mask.any():
                rejects = valid_df[null_mask].copy()
                rejects['validation_error'] = f"{col_name}: null not allowed"
                rejects['validation_severity'] = severity.value
                rejects['validation_rule_id'] = CategoricalRejectReason.NULL_NOT_ALLOWED.value
                rejects['validation_column'] = col_name
                rejects['validation_context'] = None  # No value to show
                rejects_list.append(rejects)
                valid_df = valid_df[~null_mask].copy()
                
                reject_counts_by_column[col_name] = reject_counts_by_column.get(col_name, 0) + null_mask.sum()
        
        # Check 2: Domain constraint
        invalid_mask = ~valid_df[col_name].isin(allowed_values + [None, pd.NA, ''])
        
        if invalid_mask.any():
            rejects = valid_df[invalid_mask].copy()
            rejects['validation_error'] = f"{col_name}: not in allowed values"
            rejects['validation_severity'] = severity.value
            rejects['validation_rule_id'] = CategoricalRejectReason.UNKNOWN_VALUE.value
            rejects['validation_column'] = col_name
            rejects['validation_context'] = rejects[col_name].astype(str)  # Capture invalid value
            rejects_list.append(rejects)
            valid_df = valid_df[~invalid_mask].copy()
            
            reject_counts_by_column[col_name] = reject_counts_by_column.get(col_name, 0) + invalid_mask.sum()
            
            # BLOCK severity raises immediately
            if severity == ValidationSeverity.BLOCK:
                raise ValueError(
                    f"Categorical validation failed on {col_name}: "
                    f"{invalid_mask.sum()} invalid values. "
                    f"Expected domain: {allowed_values[:10]}..."
                )
        
        # NOW safe to convert to categorical (after removing invalid rows)
        if len(allowed_values) > 0:  # Only if enum defined
            valid_df[col_name] = valid_df[col_name].astype(
                pd.CategoricalDtype(categories=allowed_values)
            )
    
    # Combine rejects
    if rejects_list:
        rejects_df = pd.concat(rejects_list, ignore_index=True)
        
        # Add provenance columns
        if natural_keys:
            rejects_df['row_id'] = rejects_df.apply(lambda r: compute_row_id(r, natural_keys), axis=1)
        if schema_id:
            rejects_df['schema_id'] = schema_id
        if release_id:
            rejects_df['release_id'] = release_id
        
        # Deterministic ordering: sort by (column, reason_code, row_id)
        sort_cols = ['validation_column', 'validation_rule_id']
        if 'row_id' in rejects_df.columns:
            sort_cols.append('row_id')
        rejects_df = rejects_df.sort_values(sort_cols).reset_index(drop=True)
    else:
        rejects_df = pd.DataFrame()
    
    # Build metrics
    total_rows = len(df)
    valid_rows = len(valid_df)
    reject_rows = len(rejects_df)
    
    metrics = {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "reject_rows": reject_rows,
        "reject_rate": reject_rows / total_rows if total_rows > 0 else 0.0,
        "columns_validated": len(categorical_cols),
        "reject_rate_by_column": {
            col: count / total_rows if total_rows > 0 else 0.0
            for col, count in reject_counts_by_column.items()
        }
    }
    
    # Emit metrics
    if reject_rows > 0:
        logger.warning(
            "Categorical validation rejects",
            total_rejects=reject_rows,
            columns=list(reject_counts_by_column.keys()),
            severity=severity.value,
            reject_rate=f"{metrics['reject_rate']:.2%}"
        )
    
    return ValidationResult(valid_df=valid_df, rejects_df=rejects_df, metrics=metrics)
