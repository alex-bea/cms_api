"""
Locality-County Crosswalk Parser (Raw)

Parses CMS 25LOCCO files (TXT, CSV, XLSX) to raw schema (no FIPS derivation).
Per STD-parser-contracts v1.10 §21.1 (11-step template).

Two-stage architecture:
- Stage 1 (this parser): Layout-faithful parsing (state/county NAMES)
- Stage 2 (enricher): FIPS derivation + county explosion

Reference: planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md

Formats Supported:
- TXT: Fixed-width (LOCCO_2025D_LAYOUT)
- CSV: Header auto-detection, alias mapping
- XLSX: Auto-sheet selection, dtype control

Schema: cms_locality_raw_v1.0 (CMS-native naming)
Natural Keys: ['mac', 'locality_code']
Expected Rows: 100-150 (~109 unique after dedup)
"""

import hashlib
import time
import re
from typing import IO, Dict, Any, BinaryIO, Tuple, Optional
from io import BytesIO
import pandas as pd
import structlog

from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    detect_encoding,
    finalize_parser_output,
    normalize_string_columns,
    validate_required_metadata,
    check_natural_key_uniqueness,
    build_parser_metrics,
    ParseError,
)
from cms_pricing.ingestion.parsers.layout_registry import LOCCO_2025D_LAYOUT

logger = structlog.get_logger(__name__)


# ============================================================================
# Header Normalization Helper
# ============================================================================

def _normalize_header(col: str) -> str:
    """
    Normalize column header for robust aliasing.
    
    - Strip leading/trailing whitespace
    - Condense multiple spaces to single space  
    - Lowercase for case-insensitive matching
    
    Examples:
        "State " → "state"
        "Fee Schedule  Area" → "fee schedule area"
        "Locality Number" → "locality number"
        
    Per STD-parser-contracts v1.10 §5.2.3 (Alias Map Best Practices)
    """
    return re.sub(r'\s+', ' ', str(col or '').strip()).lower()


# ============================================================================
# Constants
# ============================================================================

PARSER_VERSION = "v1.1.0"  # Phase 2: Added CSV/XLSX support
SCHEMA_ID = "cms_locality_raw_v1.0"
NATURAL_KEYS = ["mac", "locality_code"]

# Canonical alias map (normalized: lowercase, single-spaced)
# Handles CMS CSV quirks: typo ("Adminstrative"), trailing spaces
CANONICAL_ALIAS_MAP = {
    # MAC variations (includes CSV typo)
    'medicare adminstrative contractor': 'mac',   # CSV has typo
    'medicare administrative contractor': 'mac',  # Corrected spelling
    'medicare admin': 'mac',
    'mac': 'mac',
    
    # Locality code variations
    'locality number': 'locality_code',  # CSV name
    'locality code': 'locality_code',
    'locality': 'locality_code',
    
    # State variations
    'state': 'state_name',
    'state name': 'state_name',
    
    # Fee area variations  
    'fee schedule area': 'fee_area',
    'fee area': 'fee_area',
    'locality name': 'fee_area',
    
    # Counties variations
    'counties': 'county_names',
    'county names': 'county_names',
    'county': 'county_names',
}


# ============================================================================
# Main Parser Function
# ============================================================================

def parse_locality_raw(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse Locality-County crosswalk file (layout-faithful, no FIPS derivation).
    
    Per STD-parser-contracts v1.9 §21.1 (11-step template).
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor (see IMPLEMENTATION.md §3)
            - release_id, schema_id, product_year, quarter_vintage,
            - vintage_date, file_sha256, source_uri
        
    Returns:
        ParseResult with raw data (state/county NAMES, not FIPS codes)
        
    Examples:
        >>> with open('25LOCCO.txt', 'rb') as f:
        ...     result = parse_locality_raw(f, '25LOCCO.txt', metadata)
        >>> len(result.data)  # ~115 localities
        115
        >>> result.data.columns  # Raw columns (NAMES not FIPS)
        ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    """
    
    start_time = time.time()
    
    logger.info(
        "parse_locality_start",
        filename=filename,
        schema_id=SCHEMA_ID,
        parser_version=PARSER_VERSION
    )
    
    # Step 1: Validate metadata
    validate_required_metadata(metadata, [
        'release_id', 'schema_id', 'product_year', 'quarter_vintage',
        'vintage_date', 'file_sha256', 'source_uri'
    ])
    
    # Step 2: Detect format and route to appropriate parser
    filename_lower = filename.lower()
    
    if filename_lower.endswith('.csv'):
        # CSV format: Dynamic header detection, alias mapping
        # _parse_csv handles encoding detection internally
        df = _parse_csv(file_obj, metadata)
        detected_encoding = 'utf-8'  # CSV parser handles this, placeholder for metrics
        
    elif filename_lower.endswith(('.xlsx', '.xls')):
        # XLSX format: Auto-sheet selection, dtype control
        df = _parse_xlsx(file_obj, metadata)
        detected_encoding = 'excel'  # XLSX is binary, not text-encoded
        
    elif filename_lower.endswith('.txt'):
        # TXT fixed-width format
        raw_bytes = file_obj.read()
        detected_encoding, confidence = detect_encoding(raw_bytes)
        logger.info(
            "encoding_detected",
            filename=filename,
            encoding=detected_encoding,
            confidence=confidence
        )
        text_content = raw_bytes.decode(detected_encoding, errors='replace')
        df = _parse_txt_fixed_width(text_content, metadata)
        
    else:
        raise ParseError(f"Unsupported format: {filename}. Expected: .txt, .csv, .xlsx")
    
    # Step 6: Normalize string columns (trim, uppercase)
    string_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = normalize_string_columns(df, string_cols)
    
    # Step 7: Check natural key uniqueness
    duplicates = check_natural_key_uniqueness(df, NATURAL_KEYS)
    if duplicates:
        logger.warning(
            "duplicate_natural_keys_found",
            count=len(duplicates),
            action="dropping_duplicates",
            sample=duplicates[:5]  # Log first 5
        )
        # Drop duplicates, keep first occurrence (CMS files may have exact duplicates)
        df = df.drop_duplicates(subset=NATURAL_KEYS, keep='first').reset_index(drop=True)
        logger.info(
            "duplicates_removed",
            original_count=len(duplicates) + len(df),
            final_count=len(df),
            removed_count=len(duplicates)
        )
    
    # Step 8: Build metrics
    parse_duration_sec = time.time() - start_time
    metrics = build_parser_metrics(
        total_rows=len(df),
        valid_rows=len(df),
        reject_rows=0,  # Phase 1: no rejects (layout-faithful)
        encoding_detected=detected_encoding,
        parse_duration_sec=parse_duration_sec,
        parser_version=PARSER_VERSION,
        schema_id=SCHEMA_ID
    )
    
    logger.info(
        "parse_locality_complete",
        row_count=len(df),
        parse_time_sec=parse_duration_sec,
        encoding=detected_encoding
    )
    
    # Step 9: Finalize output (sort by natural keys, add row_content_hash)
    # Note: finalize_parser_output needs a schema dict, but we don't have one loaded yet
    # For now, just sort by natural keys for deterministic output
    df = df.sort_values(by=NATURAL_KEYS).reset_index(drop=True)
    
    # Step 10: Return ParseResult
    return ParseResult(
        data=df,
        rejects=[],  # Phase 1: layout-faithful, no rejects
        metrics=metrics
    )


# ============================================================================
# Format-Specific Parsers
# ============================================================================

def _find_header_row_csv(file_obj: BinaryIO, encoding: str) -> int:
    """
    Find CSV header row with specific column names (not title rows).
    
    Looks for column names like "Locality Number" or "Locality Code" 
    AND "Contractor" to distinguish from title rows.
    
    Scans first 15 rows to find headers (don't hardcode skiprows).
    
    Returns:
        Row index (0-based)
        
    Raises:
        ParseError: If header row not found
    """
    file_obj.seek(0)
    lines = file_obj.read().decode(encoding, errors='ignore').splitlines()
    
    for idx, line in enumerate(lines[:15]):
        line_lower = line.lower()
        # Look for specific column names, not generic keywords
        has_locality_col = ('locality number' in line_lower or 
                           'locality code' in line_lower or
                           'locality' in line_lower)
        has_contractor = 'contractor' in line_lower
        has_county_col = ('counties' in line_lower or 'county' in line_lower)
        
        # Must have locality column + contractor (avoids matching title rows)
        if has_locality_col and has_contractor and has_county_col:
            logger.info("csv_header_detected", row_index=idx, header_preview=line[:80])
            return idx
    
    raise ParseError("CSV header row not found (expected row with column names like 'Locality Number', 'Contractor', 'Counties')")


def _parse_csv(file_obj: BinaryIO, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse CSV format with dynamic header detection and alias mapping.
    
    Features:
    - Auto-detects header row (no hardcoded skiprows)
    - Handles CSV quirks: typos, trailing spaces
    - Zero-pads locality_code for format consistency
    - Encoding/BOM detection
    
    Args:
        file_obj: File object
        metadata: Metadata dict
        
    Returns:
        DataFrame with canonical columns
    """
    
    # Read bytes and detect encoding
    raw_bytes = file_obj.read()
    detected_encoding = detect_encoding(raw_bytes)[0]
    
    # Find header row dynamically
    file_obj_temp = BytesIO(raw_bytes)
    header_row = _find_header_row_csv(file_obj_temp, detected_encoding)
    
    # Reset for pandas read
    file_obj_temp = BytesIO(raw_bytes)
    
    logger.info(
        "parse_csv_start",
        encoding=detected_encoding,
        header_row=header_row
    )
    
    # Read CSV
    df = pd.read_csv(
        file_obj_temp,
        encoding=detected_encoding,
        header=header_row,
        dtype=str,  # All columns as strings
        skipinitialspace=True,
        skip_blank_lines=True,
    )
    
    # Normalize headers (lowercase, condense spaces, strip)
    df.columns = [_normalize_header(c) for c in df.columns]
    
    # Apply canonical alias map
    df = df.rename(columns=CANONICAL_ALIAS_MAP)
    
    # Verify expected columns present
    expected = {'mac', 'locality_code', 'state_name', 'fee_area', 'county_names'}
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ParseError(f"Missing columns in CSV: {missing}. Found: {list(df.columns)}")
    
    # Select canonical columns in deterministic order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Drop blank rows FIRST (before normalization)
    df = df[df['mac'].notna() & (df['mac'] != '') & (df['mac'] != 'nan')].copy()
    df = df[df['locality_code'].notna() & (df['locality_code'] != '') & (df['locality_code'] != 'nan')].copy()
    
    # Format normalization (non-semantic, for format consistency)
    df['mac'] = df['mac'].str.strip().str.zfill(5)  # Zero-pad MAC: "1112" → "01112"
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)  # Zero-pad: "0" → "00"
    df['state_name'] = df['state_name'].str.strip()
    df['fee_area'] = df['fee_area'].str.strip()
    df['county_names'] = df['county_names'].str.strip()
    
    logger.info(
        "csv_parsed",
        rows_read=len(df),
        encoding=detected_encoding,
        header_row=header_row
    )
    
    return df


def _find_data_sheet_xlsx(file_obj: BinaryIO) -> Tuple[str, int]:
    """
    Find XLSX sheet and header row with specific column names (not title rows).
    
    Looks for column names like "Locality Number" AND "Contractor"
    to distinguish from title rows.
    
    Auto-selects correct sheet (handles multi-sheet workbooks).
    
    Returns:
        (sheet_name, header_row_index)
        
    Raises:
        ParseError: If header not found in any sheet
    """
    xlsx = pd.ExcelFile(file_obj)
    
    for sheet_name in xlsx.sheet_names:
        # Read first 15 rows to find headers
        df_preview = pd.read_excel(
            file_obj,
            sheet_name=sheet_name,
            header=None,
            nrows=15
        )
        
        for idx, row in df_preview.iterrows():
            row_text = ' '.join(str(v) for v in row if pd.notna(v)).lower()
            # Look for specific column names, not generic keywords
            has_locality_col = ('locality number' in row_text or 
                               'locality code' in row_text or
                               'locality' in row_text)
            has_contractor = 'contractor' in row_text
            has_county_col = ('counties' in row_text or 'county' in row_text)
            
            # Must have locality column + contractor (avoids matching title rows)
            if has_locality_col and has_contractor and has_county_col:
                logger.info(
                    "xlsx_header_detected",
                    sheet=sheet_name,
                    row_index=idx,
                    header_preview=row_text[:80]
                )
                return sheet_name, int(idx)
    
    raise ParseError("XLSX header row not found in any sheet (expected row with column names like 'Locality Number', 'Contractor', 'Counties')")


def _parse_xlsx(file_obj: BinaryIO, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse XLSX format with auto-sheet selection and dtype control.
    
    Features:
    - Auto-selects correct sheet
    - Prevents Excel float coercion (0.0 → "0")
    - Zero-pads locality_code for format consistency
    
    Args:
        file_obj: File object
        metadata: Metadata dict
        
    Returns:
        DataFrame with canonical columns
    """
    
    # Find data sheet and header row
    sheet_name, header_row = _find_data_sheet_xlsx(file_obj)
    file_obj.seek(0)
    
    logger.info(
        "parse_xlsx_start",
        sheet_name=sheet_name,
        header_row=header_row
    )
    
    # Read with dtype control (prevent Excel float coercion)
    df = pd.read_excel(
        file_obj,
        sheet_name=sheet_name,
        header=header_row,
        dtype=str,
        converters={
            # Strip '.0' from Excel integers
            'Locality Number': lambda v: str(v).rstrip('.0') if v else '',
            'Locality Code': lambda v: str(v).rstrip('.0') if v else '',
        }
    )
    
    # Normalize headers (lowercase, condense spaces, strip)
    df.columns = [_normalize_header(c) for c in df.columns]
    
    # Apply canonical alias map
    df = df.rename(columns=CANONICAL_ALIAS_MAP)
    
    # Verify expected columns present
    expected = {'mac', 'locality_code', 'state_name', 'fee_area', 'county_names'}
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ParseError(f"Missing columns in XLSX: {missing}. Found: {list(df.columns)}")
    
    # Select canonical columns in deterministic order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Drop blank rows FIRST (before normalization)
    df = df[df['mac'].notna() & (df['mac'] != '') & (df['mac'] != 'nan')].copy()
    df = df[df['locality_code'].notna() & (df['locality_code'] != '') & (df['locality_code'] != 'nan')].copy()
    
    # Format normalization (non-semantic, for format consistency)
    df['mac'] = df['mac'].str.strip().str.zfill(5)  # Zero-pad MAC: "1112" → "01112"
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)  # Zero-pad: "0" → "00"
    df['state_name'] = df['state_name'].str.strip()
    df['fee_area'] = df['fee_area'].str.strip()
    df['county_names'] = df['county_names'].str.strip()
    
    logger.info(
        "xlsx_parsed",
        rows_read=len(df),
        sheet_name=sheet_name,
        header_row=header_row
    )
    
    return df


def _parse_txt_fixed_width(text_content: str, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse fixed-width TXT using layout registry.
    
    Handles:
    - Header skipping (rows with "Medicare Admi" or "Locality")
    - Blank line skipping
    - State name forward-fill (continuation rows)
    - Fixed-width column extraction per LOCCO_2025D_LAYOUT
    
    Args:
        text_content: Decoded text file content
        metadata: Metadata dict (for product_year)
        
    Returns:
        DataFrame with raw columns (mac, locality_code, state_name, fee_area, county_names)
    """
    
    layout = LOCCO_2025D_LAYOUT
    
    logger.info(
        "parse_txt_fixed_width",
        layout_version=layout['version'],
        min_line_length=layout['min_line_length']
    )
    
    rows = []
    skipped_header_count = 0
    skipped_blank_count = 0
    forward_filled_count = 0
    
    for line_no, line in enumerate(text_content.splitlines(), start=1):
        # Skip blank lines
        if not line.strip():
            skipped_blank_count += 1
            continue
        
        # Skip header lines (contain column titles)
        line_lower = line.strip().lower()
        if ('medicare' in line_lower and 'locality' in line_lower) or line_lower.startswith('mac'):
            skipped_header_count += 1
            logger.debug("skipped_header_line", line_no=line_no, preview=line[:50])
            continue
        
        # Check minimum line length
        if len(line) < layout['min_line_length']:
            logger.debug(
                "skipped_short_line",
                line_no=line_no,
                length=len(line),
                min_required=layout['min_line_length']
            )
            continue
        
        # Extract columns using fixed-width positions
        row = {}
        for col_name, col_spec in layout['columns'].items():
            start = col_spec['start']
            end = col_spec['end']
            
            # Extract substring
            if end is None:
                # Rest of line
                value = line[start:].strip() if start < len(line) else ""
            else:
                value = line[start:end].strip() if start < len(line) else ""
            
            row[col_name] = value
        
        # Forward-fill mac, locality_code, and state_name from previous row if blank (continuation rows)
        filled_fields = []
        if rows:  # Only forward-fill if we have a previous row
            if row.get("mac", "") == "":
                row["mac"] = rows[-1].get("mac", "")
                filled_fields.append("mac")
            if row.get("locality_code", "") == "":
                row["locality_code"] = rows[-1].get("locality_code", "")
                filled_fields.append("locality_code")
            if row.get("state_name", "") == "":
                row["state_name"] = rows[-1].get("state_name", "")
                filled_fields.append("state_name")
            
            if filled_fields:
                forward_filled_count += 1
                logger.debug(
                    "forward_filled",
                    line_no=line_no,
                    fields=filled_fields
                )
        
        rows.append(row)
    
    logger.info(
        "txt_parse_complete",
        total_rows=len(rows),
        skipped_headers=skipped_header_count,
        skipped_blank=skipped_blank_count,
        forward_filled=forward_filled_count
    )
    
    if not rows:
        raise ParseError("No data rows found after skipping headers/blanks")
    
    df = pd.DataFrame(rows)
    
    # Format normalization (for consistency with CSV/XLSX)
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)  # Zero-pad: "0" → "00"
    
    return df


# ============================================================================
# Helper Functions (for future CSV/XLSX support in Phase 2)
# ============================================================================

def _normalize_header(header: str) -> str:
    """
    Normalize CSV/XLSX header to canonical column name.
    
    Args:
        header: Raw header string
        
    Returns:
        Canonical column name or original if no alias
    """
    normalized = header.strip().lower()
    return CANONICAL_ALIAS_MAP.get(normalized, header)

