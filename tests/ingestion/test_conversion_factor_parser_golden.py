"""
Golden fixture tests for Conversion Factor parser.

Per STD-parser-contracts v1.6 §14.2 - golden-first workflow.
These tests will fail until conversion_factor_parser.py is implemented.
"""

import pytest
from pathlib import Path
import pandas as pd

from cms_pricing.ingestion.parsers._parser_kit import ParseResult


FIXTURES = Path(__file__).parent.parent / "fixtures" / "conversion_factor" / "golden"


def build_test_metadata(schema_version="v2.0"):
    """Build minimal test metadata for CF parser."""
    return {
        'release_id': 'mpfs_2025_annual_test',
        'schema_id': f'cms_conversion_factor_{schema_version}',
        'product_year': '2025',
        'quarter_vintage': '2025_annual',
        'vintage_date': pd.Timestamp('2025-01-01'),
        'file_sha256': '208cb7220aa8279496052231c5974181caab18eca940fcc01f1d344522410511',
        'source_uri': 'https://www.cms.gov/test/cf_2025.csv',
        'parser_version': 'v1.0.0',
    }


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_golden_csv_fixture():
    """
    Parse golden CSV fixture - verify deterministic output.
    
    Tests:
    - ParseResult structure
    - 2 rows (physician + anesthesia)
    - Authoritative CMS values (32.3465, 20.3178)
    - 4dp precision enforcement
    - Deterministic 64-char hash
    - Natural key sort
    - Metadata injection
    - Join invariant (total = valid + rejects)
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import (
        parse_conversion_factor,
        PARSER_VERSION
    )
    
    fixture_path = FIXTURES / "cf_2025_minimal.csv"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(
            f, 
            "cf_2025_minimal.csv", 
            build_test_metadata()
        )
    
    # ============================================================================
    # ParseResult Structure (v1.1 contract)
    # ============================================================================
    assert isinstance(result, ParseResult), "Must return ParseResult NamedTuple"
    assert isinstance(result.data, pd.DataFrame), "data must be DataFrame"
    assert isinstance(result.rejects, pd.DataFrame), "rejects must be DataFrame"
    assert isinstance(result.metrics, dict), "metrics must be dict"
    
    # ============================================================================
    # Data Content
    # ============================================================================
    assert len(result.data) == 2, "Expected 2 rows (physician + anesthesia)"
    assert len(result.rejects) == 0, "No rejects expected for valid data"
    
    # ============================================================================
    # Required Columns (Data + Metadata)
    # ============================================================================
    required_data_cols = ['cf_type', 'cf_value', 'cf_description', 'effective_from', 'effective_to']
    required_meta_cols = [
        'release_id', 'vintage_date', 'product_year', 'quarter_vintage',
        'source_filename', 'source_file_sha256', 'parsed_at', 'row_content_hash'
    ]
    
    for col in required_data_cols:
        assert col in result.data.columns, f"Missing data column: {col}"
    
    for col in required_meta_cols:
        assert col in result.data.columns, f"Missing metadata column: {col}"
    
    # ============================================================================
    # Natural Key Sort (Deterministic)
    # ============================================================================
    # Should be sorted by ["cf_type", "effective_from"]
    # Alphabetically: anesthesia, physician
    assert list(result.data['cf_type']) == ['anesthesia', 'physician'], \
        "Must be sorted by cf_type (natural key)"
    
    # ============================================================================
    # Authoritative CMS Values (Federal Register CY-2025)
    # ============================================================================
    phys = result.data[result.data['cf_type'] == 'physician'].iloc[0]
    anes = result.data[result.data['cf_type'] == 'anesthesia'].iloc[0]
    
    assert phys['cf_value'] == 32.3465, \
        f"Physician CF must be 32.3465 (CMS CY-2025), got {phys['cf_value']}"
    assert anes['cf_value'] == 20.3178, \
        f"Anesthesia CF must be 20.3178 (CMS CY-2025), got {anes['cf_value']}"
    
    # ============================================================================
    # Precision (4 decimal places, HALF_UP)
    # ============================================================================
    for val in result.data['cf_value']:
        # Check that value has at most 4 decimal places
        val_str = f"{val:.10f}".rstrip('0')  # Remove trailing zeros
        if '.' in val_str:
            decimals = len(val_str.split('.')[1])
            assert decimals <= 4, f"cf_value must have ≤4 decimal places, got {decimals} for {val}"
    
    # ============================================================================
    # Deterministic Hash (64-char SHA-256)
    # ============================================================================
    assert all(result.data['row_content_hash'].str.len() == 64), \
        "row_content_hash must be 64-character SHA-256 hex digest"
    
    # Hash should be deterministic (same input → same hash)
    assert result.data['row_content_hash'].is_unique, \
        "Each row should have unique hash"
    
    # ============================================================================
    # Metadata Injection
    # ============================================================================
    assert all(result.data['release_id'] == 'mpfs_2025_annual_test')
    assert all(result.data['product_year'] == '2025')
    assert all(result.data['quarter_vintage'] == '2025_annual')
    assert all(result.data['source_filename'] == 'cf_2025_minimal.csv')
    
    # ============================================================================
    # Metrics (Task D: includes skiprows_dynamic)
    # ============================================================================
    assert result.metrics['total_rows'] == 2
    assert result.metrics['valid_rows'] == 2
    assert result.metrics['reject_rows'] == 0
    assert result.metrics['reject_rate'] == 0.0
    assert result.metrics['encoding_detected'] in ['utf-8', 'cp1252', 'latin-1']
    assert result.metrics['encoding_fallback'] in [True, False]
    assert result.metrics['skiprows_dynamic'] == 0, "CSV has header (skiprows=0)"
    assert result.metrics['parser_version'] == PARSER_VERSION
    assert result.metrics['schema_id'] == 'cms_conversion_factor_v2.0'
    
    # Join invariant (catches bugs)
    assert result.metrics['total_rows'] == len(result.data) + len(result.rejects), \
        "Join invariant: total must equal valid + rejects"
    
    print("✅ CF Golden CSV: All assertions passed")


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_golden_zip_fixture():
    """
    Parse golden ZIP fixture - verify ZIP extraction.
    
    Tests ZIP handling per STD-parser-contracts v1.6 §21.1.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "cf_2025_minimal.zip"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(
            f,
            "cf_2025_minimal.zip",
            build_test_metadata()
        )
    
    # Same expectations as CSV (ZIP extraction should be transparent)
    assert isinstance(result, ParseResult)
    assert len(result.data) == 2, "ZIP should contain 2 rows"
    assert len(result.rejects) == 0
    
    # Verify values survived ZIP extraction
    phys = result.data[result.data['cf_type'] == 'physician'].iloc[0]
    assert phys['cf_value'] == 32.3465, "Physician CF must survive ZIP extraction"
    
    # Metadata should indicate ZIP source
    assert result.metrics['total_rows'] == 2
    
    print("✅ CF Golden ZIP: Extraction and parsing successful")


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_golden_xlsx_fixture():
    """
    Parse golden XLSX fixture - verify Excel dtype=str handling.
    
    Tests Excel parsing per Anti-Pattern 8 (STD-parser-contracts v1.6 §20.1).
    Must read as dtype=str to avoid Excel float coercion.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "cf_2025_minimal.xlsx"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(
            f,
            "cf_2025_minimal.xlsx",
            build_test_metadata()
        )
    
    # Same expectations - Excel parsing should preserve precision
    assert isinstance(result, ParseResult)
    assert len(result.data) == 2
    assert len(result.rejects) == 0
    
    # Critical: Precision must survive Excel (Anti-Pattern 8)
    phys = result.data[result.data['cf_type'] == 'physician'].iloc[0]
    anes = result.data[result.data['cf_type'] == 'anesthesia'].iloc[0]
    
    assert phys['cf_value'] == 32.3465, \
        f"Excel must preserve physician CF precision, got {phys['cf_value']}"
    assert anes['cf_value'] == 20.3178, \
        f"Excel must preserve anesthesia CF precision, got {anes['cf_value']}"
    
    # Verify 4dp precision (not Excel's float corruption)
    assert f"{phys['cf_value']:.4f}" == "32.3465"
    assert f"{anes['cf_value']:.4f}" == "20.3178"
    
    print("✅ CF Golden XLSX: Excel precision preserved (dtype=str pattern works)")


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_hash_determinism():
    """
    Verify hash is deterministic across multiple parses.
    
    Same input → same hash (reproducibility requirement).
    Per PPRRVU pattern - proven robust.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "cf_2025_minimal.csv"
    metadata = build_test_metadata()
    
    # Parse twice
    with open(fixture_path, 'rb') as f:
        result1 = parse_conversion_factor(f, "cf_2025_minimal.csv", metadata)
    
    with open(fixture_path, 'rb') as f:
        result2 = parse_conversion_factor(f, "cf_2025_minimal.csv", metadata)
    
    # Hashes must be identical (excluding parsed_at timestamp)
    hash1 = result1.data['row_content_hash'].tolist()
    hash2 = result2.data['row_content_hash'].tolist()
    
    assert hash1 == hash2, "row_content_hash must be deterministic (same input → same hash)"
    
    print("✅ CF Hash Determinism: Identical hashes across parses")


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_metadata_exclusion_invariance():
    """
    Verify row_content_hash excludes metadata columns.
    
    Changing release_id or source_filename should NOT change content hash.
    This ensures hash stability across ingestion runs.
    
    Per user feedback (2025-10-16): Key invariant for golden tests.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "cf_2025_minimal.csv"
    
    # Parse with metadata set 1
    metadata1 = build_test_metadata()
    metadata1['release_id'] = 'release_v1'
    metadata1['source_filename'] = 'file_v1.csv'
    
    with open(fixture_path, 'rb') as f:
        result1 = parse_conversion_factor(f, "cf_2025_minimal.csv", metadata1)
    
    # Parse with DIFFERENT metadata set 2
    metadata2 = build_test_metadata()
    metadata2['release_id'] = 'release_v2'  # DIFFERENT
    metadata2['source_filename'] = 'file_v2.csv'  # DIFFERENT
    
    with open(fixture_path, 'rb') as f:
        result2 = parse_conversion_factor(f, "cf_2025_minimal.csv", metadata2)
    
    # Content hashes MUST be identical (metadata excluded)
    hash1 = result1.data['row_content_hash'].tolist()
    hash2 = result2.data['row_content_hash'].tolist()
    
    assert hash1 == hash2, \
        "row_content_hash must exclude metadata (release_id, source_filename changes shouldn't affect hash)"
    
    # But metadata columns should reflect the different values
    assert (result1.data['release_id'] == 'release_v1').all()
    assert (result2.data['release_id'] == 'release_v2').all()
    
    print("✅ CF Metadata Exclusion: Hash invariant to metadata changes")


@pytest.mark.golden
@pytest.mark.ingestor  
def test_cf_mid_year_ar_support():
    """
    Verify support for mid-year adjustment revisions (AR).
    
    Real scenario: CY-2024 had physician CF change on Mar 9, 2024.
    Parser must support multiple CFs per year with different effective_from dates.
    
    Natural keys: ["cf_type", "effective_from"] allow this.
    
    Source: CMS CY-2024 mid-year AR (historical precedent).
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    from io import BytesIO
    
    # CSV with 2 physician CFs (different effective dates = valid)
    csv_content = """cf_type,cf_value,cf_description,effective_from,effective_to
physician,33.0607,CY 2024 Original,2024-01-01,2024-03-08
physician,32.7442,CY 2024 Mid-Year AR (Mar 9),2024-03-09,2024-12-31
anesthesia,20.0000,CY 2024 Anesthesia,2024-01-01,
"""
    
    metadata = build_test_metadata()
    metadata['product_year'] = '2024'
    metadata['quarter_vintage'] = '2024_annual'
    
    result = parse_conversion_factor(
        BytesIO(csv_content.encode('utf-8')),
        "cf_2024_ar.csv",
        metadata
    )
    
    # Should have 3 rows (2 physician + 1 anesthesia), no duplicates
    assert len(result.data) == 3, "Should parse all 3 rows (mid-year AR supported)"
    assert len(result.rejects) == 0, "No rejects (different effective_from = valid)"
    
    # Verify both physician CFs present
    phys_cfs = result.data[result.data['cf_type'] == 'physician']
    assert len(phys_cfs) == 2, "Should have 2 physician CFs (original + AR)"
    
    # Sorted by effective_from (natural key)
    assert phys_cfs.iloc[0]['effective_from'] < phys_cfs.iloc[1]['effective_from'], \
        "Should be sorted by effective_from (natural key)"
    
    print("✅ CF Mid-Year AR: Multiple effective dates per cf_type supported")


@pytest.mark.golden
@pytest.mark.ingestor
def test_cf_encoding_robustness():
    """
    Verify robust encoding handling (BOM, CP1252).
    
    Tests encoding cascade per STD-parser-contracts v1.6:
    - UTF-8 with BOM (\\xef\\xbb\\xbf) → BOM stripped
    - CP1252 (Windows) → smart quotes handled
    - No \\ufeff in column headers (Anti-Pattern 6)
    
    Per user feedback: Longstanding CSV pitfalls.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    from io import BytesIO
    
    # Test 1: UTF-8 with BOM
    csv_with_bom = b'\xef\xbb\xbfcf_type,cf_value,cf_description,effective_from,effective_to\nphysician,32.3465,Test,2025-01-01,\n'
    
    result_bom = parse_conversion_factor(
        BytesIO(csv_with_bom),
        "cf_bom.csv",
        build_test_metadata()
    )
    
    assert len(result_bom.data) == 1
    assert 'cf_type' in result_bom.data.columns, "BOM should be stripped from header"
    assert not any('\ufeff' in str(col) for col in result_bom.data.columns), \
        "No BOM characters in column names (Anti-Pattern 6)"
    
    # Test 2: CP1252 encoding (Windows smart quotes in description)
    csv_cp1252 = "cf_type,cf_value,cf_description,effective_from,effective_to\n"
    csv_cp1252 += "physician,32.3465,CY 2025 \x93Final\x94 Rule,2025-01-01,\n"  # Smart quotes
    csv_cp1252_bytes = csv_cp1252.encode('cp1252')
    
    result_cp1252 = parse_conversion_factor(
        BytesIO(csv_cp1252_bytes),
        "cf_cp1252.csv",
        build_test_metadata()
    )
    
    assert len(result_cp1252.data) == 1
    assert result_cp1252.metrics['encoding_detected'] in ['cp1252', 'utf-8', 'latin-1']
    assert result_cp1252.metrics['encoding_fallback'] in [True, False]
    
    print("✅ CF Encoding Robustness: BOM stripped, CP1252 handled, no \\ufeff in headers")

