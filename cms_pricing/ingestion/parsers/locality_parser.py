"""
Locality-County Crosswalk Parser (Raw)

Parses CMS 25LOCCO.txt files to raw schema (no FIPS derivation).
Per STD-parser-contracts v1.9 §21.1 (11-step template).

Two-stage architecture:
- Stage 1 (this parser): Layout-faithful parsing (state/county NAMES)
- Stage 2 (enricher): FIPS derivation + county explosion

Reference: planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md

Schema: cms_locality_raw_v1.0 (CMS-native naming)
Natural Keys: ['mac', 'locality_code']
Expected Rows: 100-150 (~115 locality entries)
"""

import hashlib
import time
from typing import IO, Dict, Any
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
# Constants
# ============================================================================

PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_locality_raw_v1.0"
NATURAL_KEYS = ["mac", "locality_code"]

# CSV/XLSX header aliases (for future Phase 2)
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'medicare administrative contractor': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'locality code': 'locality_code',
    'state': 'state_name',
    'state name': 'state_name',
    'fee schedule area': 'fee_area',
    'locality name': 'fee_area',
    'county': 'county_names',
    'counties': 'county_names',
    'county names': 'county_names',
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
    
    # Step 2: Read file content
    raw_bytes = file_obj.read()
    if isinstance(file_obj, BytesIO):
        file_obj.seek(0)  # Reset for potential re-reads
    
    # Step 3: Detect encoding
    detected_encoding, confidence = detect_encoding(raw_bytes)
    logger.info(
        "encoding_detected",
        filename=filename,
        encoding=detected_encoding,
        confidence=confidence
    )
    
    # Step 4: Decode to text
    try:
        text_content = raw_bytes.decode(detected_encoding)
    except UnicodeDecodeError as e:
        raise ParseError(f"Encoding failed with {detected_encoding}: {e}")
    
    # Step 5: Parse with layout (TXT only for Phase 1)
    if filename.endswith('.txt'):
        df = _parse_txt_fixed_width(text_content, metadata)
    else:
        raise ParseError(f"Unsupported format: {filename} (Phase 1 = TXT only)")
    
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
    
    return pd.DataFrame(rows)


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
    return ALIAS_MAP.get(normalized, header)

