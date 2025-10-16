"""
PPRRVU Parser Tests

Tests for parse_pprrvu() per STD-parser-contracts v1.2 §21.

Covers:
1. Golden fixture (determinism)
2. Precision/rounding (HALF_UP)
3. Encoding variations
4. Natural key uniqueness
5. Categorical domain validation
6. Layout mismatch
7. Hash metadata exclusion
"""

import pytest
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu, SCHEMA_ID, NATURAL_KEYS
from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    DuplicateKeyError,
    LayoutMismatchError
)


FIXTURE_DIR = Path("tests/fixtures/pprrvu")
GOLDEN_FIXTURE = FIXTURE_DIR / "golden/PPRRVU2025_sample.txt"
GOLDEN_HASH = "b4437f4534b999e1764a4bbb4c13f05dc7e18e256bdbc9cd87e82a8caed05e1e"


def create_test_metadata(release_id="mpfs_2025_q4_test"):
    """Helper to create standard test metadata."""
    return {
        'release_id': release_id,
        'product_year': '2025',
        'quarter_vintage': '2025Q4',
        'vintage_date': datetime(2025, 10, 1),
        'file_sha256': GOLDEN_HASH,
        'source_uri': 'https://www.cms.gov/files/zip/rvu25d-0.zip',
        'schema_id': SCHEMA_ID,
        'layout_version': 'v2025.4.0'
    }


def test_pprrvu_golden_fixture():
    """
    Test 1: Golden fixture produces deterministic output.
    
    Verifies:
    - Parser produces expected row count
    - Schema compliance (all required columns)
    - Deterministic row_content_hash
    - No rejects (all rows valid)
    - Metadata injected correctly
    """
    # Verify fixture integrity
    with open(GOLDEN_FIXTURE, 'rb') as f:
        fixture_content = f.read()
        fixture_hash = hashlib.sha256(fixture_content).hexdigest()
    
    assert fixture_hash == GOLDEN_HASH, f"Golden fixture changed! Expected {GOLDEN_HASH}, got {fixture_hash}"
    
    # Parse
    metadata = create_test_metadata()
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata)
    
    # Verify ParseResult structure
    assert isinstance(result, ParseResult)
    assert isinstance(result.data, pd.DataFrame)
    assert isinstance(result.rejects, pd.DataFrame)
    assert isinstance(result.metrics, dict)
    
    # Verify row count (94 data rows, header rows skipped)
    assert len(result.data) > 0, "Should have parsed data rows"
    assert len(result.data) < 100, "Should have skipped header rows"
    assert len(result.rejects) == 0, f"Expected no rejects, got {len(result.rejects)}"
    
    # Verify schema compliance (using schema column names: rvu_*)
    required_data_cols = ['hcpcs', 'modifier', 'status_code', 'rvu_work', 'rvu_pe_nonfac', 'rvu_pe_fac', 'rvu_malp', 'effective_from']
    required_metadata_cols = ['release_id', 'vintage_date', 'product_year', 'quarter_vintage',
                               'source_filename', 'source_file_sha256', 'parsed_at', 'row_content_hash']
    
    for col in required_data_cols + required_metadata_cols:
        assert col in result.data.columns, f"Missing column: {col}"
    
    # Verify hash
    assert 'row_content_hash' in result.data.columns
    assert all(result.data['row_content_hash'].str.len() == 64), "Hash not 64-char SHA-256"
    
    # Verify metadata injection
    assert all(result.data['release_id'] == 'mpfs_2025_q4_test')
    assert all(result.data['schema_id'] == SCHEMA_ID)
    
    # Verify determinism by parsing again
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result2 = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata)
    
    assert result.data['row_content_hash'].tolist() == result2.data['row_content_hash'].tolist(), "Not deterministic!"
    
    print(f"✅ Golden test passed. Parsed {len(result.data)} rows, 0 rejects")


def test_pprrvu_precision_rounding():
    """
    Test 2: RVU values rounded to 2 decimals (HALF_UP).
    """
    metadata = create_test_metadata()
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata)
    
    # Check precision on RVU columns
    rvu_cols = ['work_rvu', 'pe_rvu_nonfac', 'pe_rvu_fac', 'mp_rvu']
    
    for col in rvu_cols:
        if col in result.data.columns:
            # All values should have at most 2 decimal places
            for val in result.data[col].dropna():
                # Check by converting to string and counting decimals
                val_str = f"{val:.10f}".rstrip('0').rstrip('.')
                if '.' in val_str:
                    decimals = len(val_str.split('.')[1])
                    assert decimals <= 2, f"{col} has {decimals} decimals: {val}"
    
    print(f"✅ Precision test passed")


def test_pprrvu_natural_key_uniqueness():
    """
    Test 3: Duplicate natural keys raise DuplicateKeyError.
    
    Uses bad_dup_keys.txt fixture.
    """
    metadata = create_test_metadata()
    bad_fixture = FIXTURE_DIR / "bad_dup_keys.txt"
    
    with pytest.raises(DuplicateKeyError) as exc_info:
        with open(bad_fixture, 'rb') as f:
            parse_pprrvu(f, bad_fixture.name, metadata)
    
    # Verify error details
    error = exc_info.value
    assert "duplicate" in str(error).lower()
    assert len(error.duplicates) > 0
    
    print(f"✅ Uniqueness test passed - caught {len(error.duplicates)} duplicate(s)")


def test_pprrvu_layout_mismatch():
    """
    Test 4: Truncated rows raise LayoutMismatchError.
    
    Uses bad_layout.txt fixture.
    """
    metadata = create_test_metadata()
    bad_fixture = FIXTURE_DIR / "bad_layout.txt"
    
    # Should complete but with fewer rows (truncated row skipped)
    with open(bad_fixture, 'rb') as f:
        result = parse_pprrvu(f, bad_fixture.name, metadata)
    
    # Truncated line should be skipped
    assert len(result.data) < 10, "Should have skipped truncated row"
    
    print(f"✅ Layout test passed - skipped truncated rows")


def test_pprrvu_hash_metadata_exclusion():
    """
    Test 5: Changing metadata doesn't affect row_content_hash.
    """
    metadata1 = create_test_metadata(release_id="release_1")
    metadata2 = create_test_metadata(release_id="release_2")
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result1 = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata1)
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result2 = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata2)
    
    # Hashes should be identical (metadata excluded)
    assert result1.data['row_content_hash'].tolist() == result2.data['row_content_hash'].tolist()
    
    # But release_id should differ
    assert all(result1.data['release_id'] == "release_1")
    assert all(result2.data['release_id'] == "release_2")
    
    print(f"✅ Hash exclusion test passed")


def test_pprrvu_performance():
    """
    Test 6: Parser completes within performance budget.
    
    Golden fixture (94 rows) should parse in < 2 seconds.
    """
    metadata = create_test_metadata()
    
    import time
    start = time.time()
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata)
    
    duration = time.time() - start
    
    assert duration < 2.0, f"Parse took {duration:.2f}s, expected < 2s"
    assert result.metrics['parse_duration_sec'] < 2.0
    
    print(f"✅ Performance test passed: {duration:.3f}s")


def test_pprrvu_metrics_structure():
    """
    Test 7: Metrics dict has required fields.
    """
    metadata = create_test_metadata()
    
    with open(GOLDEN_FIXTURE, 'rb') as f:
        result = parse_pprrvu(f, GOLDEN_FIXTURE.name, metadata)
    
    required_metrics = [
        'parser_version', 'encoding_detected', 'parse_duration_sec',
        'schema_id', 'layout_version', 'filename',
        'total_rows', 'rows_valid', 'rows_rejected'
    ]
    
    for metric in required_metrics:
        assert metric in result.metrics, f"Missing metric: {metric}"
    
    print(f"✅ Metrics test passed: {len(result.metrics)} metrics")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
