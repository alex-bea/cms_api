"""
OPPSCAP Parser - OPPS-based Payment Caps

Parses CMS OPPSCAP files (TXT/CSV) to canonical schema cms_oppscap_v1.1.

Per STD-parser-contracts v2.0 §2.1 (11-step template).

Supports:
- Fixed-width TXT via layout registry (OPPSCAP_2025D_LAYOUT v2025.4.0)
- CSV with header normalization (handles CMS typo: NON-FACILTY)

Schema: cms_oppscap_v1.1
Natural Keys: ['hcpcs', 'modifier', 'mac', 'locality_code']
Expected Rows: ~16,000 (varies by vintage)
"""

import hashlib
import time
import re
from typing import IO, Dict, Any, Tuple
from io import BytesIO
from datetime import datetime

import pandas as pd
import structlog

from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    detect_encoding,
    canonicalize_numeric_col,
    finalize_parser_output,
    normalize_string_columns,
    validate_required_metadata,
    check_natural_key_uniqueness,
    build_parser_metrics,
    ParseError,
)
from cms_pricing.ingestion.parsers.layout_registry import get_layout


logger = structlog.get_logger(__name__)


# ============================================================================
# Constants
# ============================================================================

PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_oppscap_v1.1"
NATURAL_KEYS = ["hcpcs", "modifier", "mac", "locality_code"]

# CSV header aliases (CMS variations + typos)
ALIAS_MAP = {
    # Standard CSV headers
    'hcpcs': 'hcpcs',
    'mod': 'modifier',
    'modifier': 'modifier',
    'procstat': 'status',
    'proc status': 'status',
    'carrier': 'mac',
    'mac': 'mac',
    'locality': 'locality_code',
    'locality code': 'locality_code',
    
    # Price columns (including CMS typo)
    'facility price': 'facility_price',
    'fac price': 'facility_price',
    'non-facilty price': 'nonfacility_price',  # CMS typo (missing I) - IMPORTANT!
    'non-facility price': 'nonfacility_price',  # Correct spelling
    'nonfac price': 'nonfacility_price',
    'nonfacility price': 'nonfacility_price',
}

# Validation rules (BLOCK severity)
VALIDATION_RULES = {
    'R-OPPSCAP-001': 'HCPCS must match ^[A-Z0-9]{5}$',
    'R-OPPSCAP-002': 'Modifier must match ^[A-Z0-9]{2}$ or be null',
    'R-OPPSCAP-003': 'MAC must be 5 digits (zero-padded)',
    'R-OPPSCAP-004': 'Locality must be 2 digits (zero-padded)',
    'R-OPPSCAP-005': 'Facility price must be >= 0',
    'R-OPPSCAP-006': 'Non-facility price must be >= 0',
    'R-OPPSCAP-007': 'Natural key (hcpcs, modifier, mac, locality) must be unique',
}


# ============================================================================
# Main Parser Function
# ============================================================================

def parse_oppscap(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any],
) -> ParseResult:
    """
    Parse OPPSCAP file (TXT or CSV) to canonical schema.
    
    Per STD-parser-contracts v2.0 §2.1 (11-step template).
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor
            - release_id, schema_id, product_year, quarter_vintage,
            - vintage_date, file_sha256, source_uri, source_release
        
    Returns:
        ParseResult with:
            - data: pandas DataFrame (canonical rows with metadata + row_content_hash)
            - rejects: pandas DataFrame (validation failures)
            - metrics: Dict (rows_parsed, rows_out, rejects_by_reason, parse_time_sec, etc.)
        
    Raises:
        ParseError: If parsing fails critically
        ValueError: If required metadata missing
    
    Schema: cms_oppscap_v1.1
    Natural Keys: ['hcpcs', 'modifier', 'mac', 'locality_code']
    Expected Rows: ~16,000
    """
    start_time = time.perf_counter()
    
    logger.info(
        "parse_oppscap_start",
        filename=filename,
        schema_id=metadata.get('schema_id'),
        parser_version=PARSER_VERSION
    )
    
    # Step 1: Validate required metadata
    validate_required_metadata(metadata, ['release_id', 'product_year', 'source_release'])
    
    # Step 2: Detect encoding (read first 8KB, then rewind)
    head = file_obj.read(8192)
    encoding, bom_detected = detect_encoding(head)
    file_obj.seek(0)
    
    logger.info("encoding_detected", encoding=encoding, bom=bom_detected, filename=filename)
    
    # Step 3: Detect format (TXT vs CSV)
    format_type = _detect_format(file_obj, filename)
    file_obj.seek(0)
    
    logger.info("format_detected", format=format_type, filename=filename)
    
    # Step 4: Parse based on format
    file_obj.seek(0)
    if format_type == 'TXT':
        df = _parse_txt_fixed_width(file_obj, encoding, metadata)
    elif format_type == 'CSV':
        df = _parse_csv(file_obj, encoding, metadata)
    else:
        raise ParseError(f"Unsupported format: {format_type}")
    
    logger.info("parse_raw_complete", rows=len(df), format=format_type)
    
    # Step 5: Normalize columns
    df = _normalize_columns(df)
    
    # Step 6: Type coercion + zero-padding
    df = _apply_type_coercion(df)
    
    # Step 7: Validation
    df, rejects = _validate_rules(df)
    
    logger.info(
        "validation_complete",
        rows_valid=len(df),
        rows_rejected=len(rejects)
    )
    
    # Step 8: Inject metadata
    df = _inject_metadata(df, metadata, filename)
    
    # Step 9: Sort by natural keys
    df = df.sort_values(by=NATURAL_KEYS).reset_index(drop=True)
    
    # Step 10: Compute row hashes
    df = _compute_row_hashes(df)
    
    # Step 11: Build metrics and return
    parse_time = time.perf_counter() - start_time
    
    metrics = {
        'rows_parsed': len(df) + len(rejects),
        'rows_out': len(df),
        'rows_rejected': len(rejects),
        'rejects_by_reason': rejects.groupby('reason').size().to_dict() if len(rejects) > 0 else {},
        'parse_time_sec': parse_time,
        'encoding_used': encoding,
        'bom_detected': bom_detected,
        'format_type': format_type,
    }
    
    logger.info(
        "parse_oppscap_complete",
        rows_parsed=metrics['rows_parsed'],
        rows_out=metrics['rows_out'],
        rows_rejected=metrics['rows_rejected'],
        parse_time_sec=parse_time,
    )
    
    return ParseResult(data=df, rejects=rejects, metrics=metrics)


# ============================================================================
# Helper Functions
# ============================================================================

def _detect_format(file_obj: IO[bytes], filename: str) -> str:
    """
    Detect file format (TXT or CSV) from filename and content.
    
    Args:
        file_obj: File stream (will be rewound after peek)
        filename: Source filename
        
    Returns:
        'TXT' or 'CSV'
    """
    # Filename-based detection
    filename_lower = filename.lower()
    if '.txt' in filename_lower:
        return 'TXT'
    elif '.csv' in filename_lower:
        return 'CSV'
    
    # Content-based detection (peek first line)
    try:
        first_line = file_obj.readline().decode('utf-8', errors='ignore').strip()
        file_obj.seek(0)
        
        # CSV has headers with commas
        if 'HCPCS' in first_line and ',' in first_line:
            return 'CSV'
        
        # TXT has no header, starts with HCPCS code (5 alphanumeric chars)
        if re.match(r'^[A-Z0-9]{5}', first_line):
            return 'TXT'
    except Exception as e:
        logger.warning("format_detection_failed", error=str(e), defaulting_to="TXT")
    
    # Default to TXT (authority format)
    logger.warning("format_detection_ambiguous", filename=filename, defaulting_to="TXT")
    return 'TXT'


def _parse_txt_fixed_width(
    file_obj: IO[bytes],
    encoding: str,
    metadata: Dict[str, Any],
) -> pd.DataFrame:
    """
    Parse fixed-width TXT using layout registry.
    
    Args:
        file_obj: File stream
        encoding: Detected encoding
        metadata: Metadata dict (for vintage lookup)
        
    Returns:
        DataFrame with layout-defined columns
    """
    # Get layout for this vintage
    product_year = str(metadata.get('product_year', '2025'))
    quarter_vintage = metadata.get('quarter_vintage', 'D')
    
    # get_layout signature: (product_year, quarter_vintage, dataset)
    layout = get_layout(product_year, quarter_vintage, 'oppscap')
    if not layout:
        raise ParseError(f"No layout found for OPPSCAP {product_year} Q{quarter_vintage}")
    
    # Extract column specs from layout
    columns = layout['columns']
    colspecs = []
    names = []
    
    for col_name, spec in columns.items():
        start = spec['start']
        end = spec['end']
        colspecs.append((start, end))
        names.append(col_name)
    
    # Parse fixed-width
    df = pd.read_fwf(
        file_obj,
        colspecs=colspecs,
        names=names,
        dtype=str,
        encoding=encoding,
    )
    
    # Strip whitespace from all string columns
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    # Layout probe logging (first 3 rows for verification)
    if len(df) >= 3:
        logger.debug(
            "layout_probe_txt",
            sample_hcpcs=df['hcpcs'].head(3).tolist(),
            sample_mac=df['mac'].head(3).tolist(),
            sample_locality=df['locality_code'].head(3).tolist(),
            sample_fac_price=df['facility_price'].head(3).tolist(),
        )
    
    return df


def _parse_csv(
    file_obj: IO[bytes],
    encoding: str,
    metadata: Dict[str, Any],
) -> pd.DataFrame:
    """
    Parse CSV with header normalization.
    
    Args:
        file_obj: File stream
        encoding: Detected encoding
        metadata: Metadata dict
        
    Returns:
        DataFrame with normalized column names
    """
    # Read CSV
    df = pd.read_csv(file_obj, dtype=str, encoding=encoding)
    
    # Normalize headers (case-insensitive alias matching)
    lower_alias_map = {k.lower(): v for k, v in ALIAS_MAP.items()}
    
    new_columns = {}
    unmapped = []
    
    for col in df.columns:
        col_lower = col.strip().lower()
        if col_lower in lower_alias_map:
            new_columns[col] = lower_alias_map[col_lower]
        else:
            unmapped.append(col)
            new_columns[col] = col.strip().lower()
    
    if unmapped:
        logger.warning("unmapped_csv_columns", columns=unmapped, filename=metadata.get('source_filename'))
    
    df = df.rename(columns=new_columns)
    
    # Strip whitespace
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names and values to match schema.
    
    Args:
        df: Raw DataFrame
        
    Returns:
        DataFrame with normalized columns
    """
    # Uppercase string identifiers
    if 'hcpcs' in df.columns:
        df['hcpcs'] = df['hcpcs'].str.upper().str.strip()
    
    if 'modifier' in df.columns:
        df['modifier'] = df['modifier'].str.upper().str.strip()
        # Convert empty string to None (nullable field)
        df['modifier'] = df['modifier'].replace('', None)
        df['modifier'] = df['modifier'].replace('nan', None)
    
    if 'status' in df.columns:
        df['status'] = df['status'].str.upper().str.strip()
    
    return df


def _apply_type_coercion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply type coercion per schema contract.
    
    Args:
        df: DataFrame with normalized columns
        
    Returns:
        DataFrame with coerced types
    """
    # Zero-padding for code fields
    if 'mac' in df.columns:
        df['mac'] = df['mac'].astype(str).str.zfill(5)
    
    if 'locality_code' in df.columns:
        df['locality_code'] = df['locality_code'].astype(str).str.zfill(2)
    
    # Canonicalize prices to Decimal(10, 2) strings
    # This ensures hash stability and exact precision
    if 'facility_price' in df.columns:
        df['facility_price'] = canonicalize_numeric_col(
            df['facility_price'],
            precision=2,
            rounding_mode='HALF_UP'
        )
    
    if 'nonfacility_price' in df.columns:
        df['nonfacility_price'] = canonicalize_numeric_col(
            df['nonfacility_price'],
            precision=2,
            rounding_mode='HALF_UP'
        )
    
    return df


def _validate_rules(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate data against schema rules (R-OPPSCAP-001 to R-OPPSCAP-007).
    
    Args:
        df: DataFrame to validate
        
    Returns:
        (valid_df, rejects_df)
    """
    reject_rows = []
    
    # R-001: HCPCS pattern
    if 'hcpcs' in df.columns:
        invalid_hcpcs = ~df['hcpcs'].str.match(r'^[A-Z0-9]{5}$', na=False)
        if invalid_hcpcs.any():
            rejects = df[invalid_hcpcs].copy()
            rejects['reason'] = 'invalid_hcpcs'
            rejects['rule_id'] = 'R-OPPSCAP-001'
            rejects['validation_error'] = 'HCPCS must be 5 alphanumeric characters'
            rejects['validation_severity'] = 'BLOCK'
            reject_rows.append(rejects)
            df = df[~invalid_hcpcs]
            
            logger.warning(
                "validation_failed_hcpcs",
                count=invalid_hcpcs.sum(),
                examples=rejects['hcpcs'].head(3).tolist()
            )
    
    # R-002: Modifier pattern (nullable)
    if 'modifier' in df.columns:
        # Modifier must be 2 chars OR null
        invalid_mod = df['modifier'].notna() & ~df['modifier'].str.match(r'^[A-Z0-9]{2}$', na=False)
        if invalid_mod.any():
            rejects = df[invalid_mod].copy()
            rejects['reason'] = 'invalid_modifier'
            rejects['rule_id'] = 'R-OPPSCAP-002'
            rejects['validation_error'] = 'Modifier must be 2 alphanumeric characters or null'
            rejects['validation_severity'] = 'BLOCK'
            reject_rows.append(rejects)
            df = df[~invalid_mod]
            
            logger.warning(
                "validation_failed_modifier",
                count=invalid_mod.sum(),
                examples=rejects['modifier'].head(3).tolist()
            )
    
    # R-003: MAC 5 digits
    if 'mac' in df.columns:
        invalid_mac = ~df['mac'].str.match(r'^[0-9]{5}$', na=False)
        if invalid_mac.any():
            rejects = df[invalid_mac].copy()
            rejects['reason'] = 'invalid_mac'
            rejects['rule_id'] = 'R-OPPSCAP-003'
            rejects['validation_error'] = 'MAC must be 5 digits'
            rejects['validation_severity'] = 'BLOCK'
            reject_rows.append(rejects)
            df = df[~invalid_mac]
            
            logger.warning(
                "validation_failed_mac",
                count=invalid_mac.sum(),
                examples=rejects['mac'].head(3).tolist()
            )
    
    # R-004: Locality 2 digits
    if 'locality_code' in df.columns:
        invalid_loc = ~df['locality_code'].str.match(r'^[0-9]{2}$', na=False)
        if invalid_loc.any():
            rejects = df[invalid_loc].copy()
            rejects['reason'] = 'invalid_locality'
            rejects['rule_id'] = 'R-OPPSCAP-004'
            rejects['validation_error'] = 'Locality must be 2 digits'
            rejects['validation_severity'] = 'BLOCK'
            reject_rows.append(rejects)
            df = df[~invalid_loc]
            
            logger.warning(
                "validation_failed_locality",
                count=invalid_loc.sum(),
                examples=rejects['locality_code'].head(3).tolist()
            )
    
    # R-005 & R-006: Prices non-negative
    for price_col, rule_id in [('facility_price', 'R-OPPSCAP-005'), ('nonfacility_price', 'R-OPPSCAP-006')]:
        if price_col in df.columns:
            # Convert to numeric for validation
            price_numeric = pd.to_numeric(df[price_col], errors='coerce')
            invalid_price = (price_numeric < 0) | price_numeric.isna()
            
            if invalid_price.any():
                rejects = df[invalid_price].copy()
                rejects['reason'] = f'invalid_{price_col}'
                rejects['rule_id'] = rule_id
                rejects['validation_error'] = f'{price_col} must be non-negative'
                rejects['validation_severity'] = 'BLOCK'
                reject_rows.append(rejects)
                df = df[~invalid_price]
                
                logger.warning(
                    f"validation_failed_{price_col}",
                    count=invalid_price.sum(),
                    examples=df[invalid_price][price_col].head(3).tolist() if invalid_price.any() else []
                )
    
    # R-007: Natural key uniqueness
    duplicates = df[df.duplicated(subset=NATURAL_KEYS, keep=False)]
    if len(duplicates) > 0:
        duplicates = duplicates.copy()
        duplicates['reason'] = 'duplicate_natural_key'
        duplicates['rule_id'] = 'R-OPPSCAP-007'
        duplicates['validation_error'] = f'Duplicate NK: {NATURAL_KEYS}'
        duplicates['validation_severity'] = 'BLOCK'
        reject_rows.append(duplicates)
        df = df.drop_duplicates(subset=NATURAL_KEYS, keep='first')
        
        logger.error(
            "duplicate_natural_keys",
            count=len(duplicates),
            natural_keys=NATURAL_KEYS,
            examples=duplicates[NATURAL_KEYS].head(3).to_dict('records')
        )
    
    # Combine rejects
    if reject_rows:
        rejects_df = pd.concat(reject_rows, ignore_index=True)
    else:
        rejects_df = pd.DataFrame()
    
    return df, rejects_df


def _inject_metadata(
    df: pd.DataFrame,
    metadata: Dict[str, Any],
    filename: str
) -> pd.DataFrame:
    """
    Inject required metadata columns.
    
    Args:
        df: DataFrame to augment
        metadata: Metadata dict from ingestor
        filename: Source filename
        
    Returns:
        DataFrame with metadata columns
    """
    df['release_id'] = metadata['release_id']
    df['product_year'] = str(metadata['product_year'])
    df['quarter_vintage'] = metadata.get('quarter_vintage', 'D')
    df['vintage_date'] = metadata.get('vintage_date', f"{metadata['product_year']}-10-01")
    df['source_filename'] = filename
    df['source_file_sha256'] = metadata.get('file_sha256', '')
    df['source_uri'] = metadata.get('source_uri', '')
    df['source_release'] = metadata.get('source_release', f"RVU{metadata['product_year']}{metadata.get('quarter_vintage', 'D')}")
    df['parsed_at'] = datetime.utcnow().isoformat()
    
    return df


def _compute_row_hashes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute row_content_hash per schema contract.
    
    Uses SHA-256 of row values in schema order (excluding metadata).
    
    Args:
        df: DataFrame with all columns
        
    Returns:
        DataFrame with row_content_hash column
    """
    # Columns to hash (in schema order, excluding metadata)
    hash_cols = ['hcpcs', 'modifier', 'status', 'mac', 'locality_code', 'facility_price', 'nonfacility_price']
    
    def hash_row(row):
        # Concatenate values in schema order
        values = [str(row[col]) if pd.notna(row[col]) else '' for col in hash_cols if col in row.index]
        content = '|'.join(values)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    df['row_content_hash'] = df.apply(hash_row, axis=1)
    
    # Log first 3 hashes for determinism verification
    if len(df) >= 3:
        logger.debug(
            "row_hashes_sample",
            hash_1=df['row_content_hash'].iloc[0],
            hash_2=df['row_content_hash'].iloc[1],
            hash_3=df['row_content_hash'].iloc[2],
        )
    
    return df

