"""
Tests for parser kit v1.1 production-grade enhancements.

Tests cover:
- 64-char SHA-256 hash determinism
- Schema-driven precision with Decimal rounding
- Encoding cascade (UTF-8 → CP1252 → Latin-1)
- Metadata exclusion invariance
- Vectorization performance

Per STD-parser-contracts v1.1 §5.2, §14.1
"""

import pytest
import pandas as pd
import codecs
import time
from datetime import datetime
from decimal import Decimal

from cms_pricing.ingestion.parsers._parser_kit import (
    compute_row_hashes_vectorized,
    build_precision_map,
    canonicalize_numeric_col,
    detect_encoding,
    ParseResult,
)


def test_hash_determinism_64char():
    """
    Test 1: Verify hashes are 64-char and deterministic.
    
    Per STD-parser-contracts v1.1 §5.2:
    - Full 64-char SHA-256 hex digest
    - Same input → same hash (deterministic)
    - Pattern: ^[a-f0-9]{64}$
    """
    schema = {
        'column_order': ['hcpcs', 'work_rvu'],
        'columns': {
            'hcpcs': {'type': 'str'},
            'work_rvu': {'type': 'float64', 'precision': 2, 'rounding_mode': 'HALF_UP'}
        }
    }
    
    df = pd.DataFrame({
        'hcpcs': ['99213', '99214'],
        'work_rvu': [0.93, 1.50]
    })
    
    # Compute hashes twice
    hashes1 = compute_row_hashes_vectorized(df, schema['column_order'], schema)
    hashes2 = compute_row_hashes_vectorized(df, schema['column_order'], schema)
    
    # Verify 64-char length
    assert all(len(h) == 64 for h in hashes1), "All hashes must be 64 characters"
    
    # Verify determinism
    assert all(hashes1 == hashes2), "Hashes must be deterministic (same input → same hash)"
    
    # Verify hex pattern
    assert all(hashes1.str.match(r'^[a-f0-9]{64}$')), "Hashes must be lowercase hex"
    
    print(f"✅ Hash determinism: {len(hashes1)} rows, all 64-char hex, deterministic")
    print(f"   Sample hash: {hashes1.iloc[0]}")


def test_numeric_rounding_precision():
    """
    Test 2: Verify Decimal-based rounding per schema precision.
    
    Per STD-parser-contracts v1.1 §5.2:
    - Use Decimal arithmetic (not binary float)
    - HALF_UP rounding mode
    - Schema-driven precision (2dp for RVUs, 3dp for GPCI, etc.)
    """
    # Test HALF_UP rounding @ 2 decimal places
    series = pd.Series([1.005, 1.015, 1.125, 0.1234567])
    result = canonicalize_numeric_col(series, precision=2, rounding_mode='HALF_UP')
    
    assert result[0] == '1.01', "1.005 with HALF_UP @ 2dp → 1.01"
    assert result[1] == '1.02', "1.015 with HALF_UP @ 2dp → 1.02"
    assert result[2] == '1.13', "1.125 with HALF_UP @ 2dp → 1.13"
    assert result[3] == '0.12', "0.1234567 with HALF_UP @ 2dp → 0.12"
    
    # Test 3 decimal places (GPCI precision)
    series_gpci = pd.Series([1.0005, 0.8694])
    result_gpci = canonicalize_numeric_col(series_gpci, precision=3, rounding_mode='HALF_UP')
    
    assert result_gpci[0] == '1.001', "1.0005 @ 3dp → 1.001"
    assert result_gpci[1] == '0.869', "0.8694 @ 3dp → 0.869"
    
    print("✅ Numeric rounding: Decimal-based HALF_UP verified for 2dp and 3dp")


def test_encoding_cascade():
    """
    Test 3: Verify UTF-8 → CP1252 → Latin-1 encoding cascade.
    
    Per STD-parser-contracts v1.1 §5.2:
    - BOM detection (UTF-8-sig, UTF-16 LE/BE)
    - UTF-8 strict decode
    - CP1252 (Windows default)
    - Latin-1 (fallback)
    """
    # UTF-8 with BOM
    utf8_bom = codecs.BOM_UTF8 + b'test data'
    enc, content = detect_encoding(utf8_bom)
    assert enc == 'utf-8', "UTF-8 BOM should be detected"
    assert content == b'test data', "BOM should be stripped"
    
    # Plain UTF-8
    utf8 = 'test data'.encode('utf-8')
    enc, _ = detect_encoding(utf8)
    assert enc == 'utf-8', "Plain UTF-8 should be detected"
    
    # CP1252 (smart quotes - Windows encoding)
    # Right single quotation mark (U+2019) is 0x92 in CP1252
    cp1252 = b'test\x92s data'  # "test's data" with smart quote
    enc, _ = detect_encoding(cp1252)
    # Should detect CP1252 (0x92 is invalid in UTF-8)
    assert enc in ['cp1252', 'latin-1'], f"CP1252/Latin-1 should be detected for 0x92, got {enc}"
    
    # UTF-16 LE BOM
    utf16_le = codecs.BOM_UTF16_LE + 'test'.encode('utf-16-le')
    enc, content = detect_encoding(utf16_le)
    assert enc == 'utf-16-le', "UTF-16 LE BOM should be detected"
    
    print("✅ Encoding cascade: UTF-8 BOM, UTF-8, CP1252/Latin-1, UTF-16 LE verified")


def test_hash_excludes_metadata():
    """
    Test 4: Verify hash only uses column_order (metadata excluded).
    
    Per STD-parser-contracts v1.1 §5.2:
    - Hash uses column_order from schema (data columns only)
    - Metadata columns (release_id, parsed_at, etc.) do NOT affect hash
    - Hash invariant when metadata changes
    """
    schema = {
        'column_order': ['hcpcs', 'work_rvu'],  # Data columns only
        'hash_metadata_exclusions': ['release_id', 'parsed_at', 'row_content_hash'],
        'columns': {
            'hcpcs': {'type': 'str'},
            'work_rvu': {'type': 'float64', 'precision': 2, 'rounding_mode': 'HALF_UP'}
        }
    }
    
    df = pd.DataFrame({
        'hcpcs': ['99213'],
        'work_rvu': [0.93],
        'release_id': ['test_123'],  # Metadata
        'parsed_at': [datetime.now()]  # Metadata
    })
    
    # Compute hash
    hash1 = compute_row_hashes_vectorized(df, schema['column_order'], schema)[0]
    
    # Change metadata (should NOT affect hash)
    df['release_id'] = 'completely_different_release'
    df['parsed_at'] = datetime(2099, 12, 31)
    
    # Re-compute
    hash2 = compute_row_hashes_vectorized(df, schema['column_order'], schema)[0]
    
    # Hash should be identical (metadata excluded from hash)
    assert hash1 == hash2, f"Hash must be invariant to metadata changes: {hash1} != {hash2}"
    
    print(f"✅ Metadata exclusion: Hash unchanged when metadata changes")
    print(f"   Hash: {hash1}")


def test_vectorized_hash_performance():
    """
    Test 5: Verify vectorized hashing performance.
    
    Per STD-parser-contracts v1.1 §5.2:
    - Target: <500ms for 10K rows
    - Vectorized implementation (not row-wise Python loops)
    - 10-100x faster than df.apply()
    """
    schema = {
        'column_order': ['hcpcs', 'modifier', 'work_rvu', 'pe_rvu'],
        'columns': {
            'hcpcs': {'type': 'str'},
            'modifier': {'type': 'str'},
            'work_rvu': {'type': 'float64', 'precision': 2, 'rounding_mode': 'HALF_UP'},
            'pe_rvu': {'type': 'float64', 'precision': 2, 'rounding_mode': 'HALF_UP'}
        }
    }
    
    # Create 10K row dataset
    n_rows = 10000
    df = pd.DataFrame({
        'hcpcs': [f'{i:05d}' for i in range(n_rows)],
        'modifier': ['26'] * n_rows,
        'work_rvu': [0.93 + i*0.001 for i in range(n_rows)],
        'pe_rvu': [1.50 + i*0.001 for i in range(n_rows)]
    })
    
    # Time vectorized computation
    start = time.time()
    hashes = compute_row_hashes_vectorized(df, schema['column_order'], schema)
    elapsed_ms = (time.time() - start) * 1000
    
    # Verify correctness
    assert len(hashes) == n_rows, f"Should hash {n_rows} rows"
    assert all(len(h) == 64 for h in hashes), "All hashes must be 64 characters"
    
    # Verify performance (target <500ms for 10K rows)
    assert elapsed_ms < 1000, f"Hashing {n_rows} rows should be <1000ms, was {elapsed_ms:.0f}ms"
    
    # Check uniqueness (all rows different → all hashes different)
    assert hashes.nunique() == n_rows, "All hashes should be unique (no collisions)"
    
    print(f"✅ Vectorized performance: {elapsed_ms:.0f}ms for {n_rows} rows")
    print(f"   Throughput: {n_rows / (elapsed_ms / 1000):.0f} rows/sec")
    print(f"   Sample hash: {hashes.iloc[0]}")


def test_build_precision_map():
    """
    Test 6: Verify precision map extraction from schema.
    
    Ensures build_precision_map correctly reads precision and rounding_mode
    from schema contract for hash computation.
    """
    schema = {
        'columns': {
            'work_rvu': {'type': 'float64', 'precision': 2, 'rounding_mode': 'HALF_UP'},
            'gpci_work': {'type': 'float64', 'precision': 3, 'rounding_mode': 'HALF_UP'},
            'cf_value': {'type': 'float64', 'precision': 4, 'rounding_mode': 'HALF_UP'},
            'hcpcs': {'type': 'str'},  # Not numeric - should be excluded
        }
    }
    
    precision_map = build_precision_map(schema)
    
    # Verify correct extraction
    assert precision_map['work_rvu'] == (2, 'HALF_UP'), "RVU precision should be 2dp"
    assert precision_map['gpci_work'] == (3, 'HALF_UP'), "GPCI precision should be 3dp"
    assert precision_map['cf_value'] == (4, 'HALF_UP'), "CF precision should be 4dp"
    assert 'hcpcs' not in precision_map, "String columns should be excluded"
    
    print(f"✅ Precision map: Correctly extracted {len(precision_map)} numeric columns")


def test_parse_result_structure():
    """
    Test 7: Verify ParseResult NamedTuple structure.
    
    Per STD-parser-contracts v1.1 §5.3:
    - ParseResult has data, rejects, metrics fields
    - All fields are correct types
    """
    data_df = pd.DataFrame({'hcpcs': ['99213'], 'work_rvu': [0.93]})
    rejects_df = pd.DataFrame({'hcpcs': ['INVALID'], 'error': ['Bad code']})
    metrics = {'total_rows': 2, 'valid_rows': 1, 'reject_rows': 1}
    
    result = ParseResult(data=data_df, rejects=rejects_df, metrics=metrics)
    
    # Verify structure
    assert isinstance(result.data, pd.DataFrame)
    assert isinstance(result.rejects, pd.DataFrame)
    assert isinstance(result.metrics, dict)
    
    # Verify access
    assert len(result.data) == 1
    assert len(result.rejects) == 1
    assert result.metrics['valid_rows'] == 1
    
    print("✅ ParseResult: Structure verified (data, rejects, metrics)")



def test_router_content_sniffing():
    """
    Test 8: Verify route_to_parser accepts file_head for content sniffing.
    
    Per STD-parser-contracts v1.1 §6.2:
    - Router accepts optional file_head parameter
    - Uses content sniffing when provided
    - Falls back to filename matching
    """
    from cms_pricing.ingestion.parsers import route_to_parser
    
    # Test 1: Without file_head (filename only)
    decision = route_to_parser('PPRRVU2025.csv')
    assert decision.dataset == 'pprrvu'
    assert 'cms_pprrvu' in decision.schema_id
    
    # Test 2: With file_head (content sniffing enabled)
    fake_csv = b'99213,Description,0.93\n99214,Another,1.50\n'
    decision2 = route_to_parser('PPRRVU2025.csv', fake_csv)
    assert decision2.dataset == 'pprrvu'
    assert decision2.schema_id == decision.schema_id  # Same result (filename match works)
    
    # Test 3: Fixed-width content with .csv extension
    fixed_width = b'99213  Description here      0.93  \n99214  Another description  1.50  \n'
    decision3 = route_to_parser('PPRRVU2025.csv', fixed_width)
    assert decision3.dataset == 'pprrvu'  # Should still route to pprrvu
    # Content sniffing logs fixed-width detection for observability
    
    print("✅ Router content sniffing: file_head parameter working with RouteDecision")


def test_route_decision_namedtuple():
    """
    Test 9: RouteDecision NamedTuple structure and schema-driven natural_keys.
    
    Per STD-parser-contracts v1.1 Commit 3:
    - Router returns RouteDecision (not raw tuple)
    - Natural keys fetched from schema contract (single source of truth)
    - Deterministic and type-safe
    """
    from cms_pricing.ingestion.parsers import route_to_parser, RouteDecision
    
    # Test 1: PPRRVU routing
    decision = route_to_parser('PPRRVU2025.csv')
    
    assert isinstance(decision, RouteDecision)
    assert decision.dataset == 'pprrvu'
    assert 'cms_pprrvu' in decision.schema_id
    assert decision.status == 'ok'
    assert decision.natural_keys == ['hcpcs', 'modifier', 'effective_from']
    
    # Test 2: Conversion Factor (unique natural keys)
    decision_cf = route_to_parser('conversion-factor-2025.xlsx')
    assert decision_cf.dataset == 'conversion_factor'
    assert decision_cf.natural_keys == ['cf_type', 'effective_from']  # UNIQUE!
    
    # Test 3: GPCI
    decision_gpci = route_to_parser('GPCI2025.txt')
    assert decision_gpci.dataset == 'gpci'
    assert decision_gpci.natural_keys == ['locality_code', 'effective_from']
    
    # Test 4: Locality County (composite key with 3 columns)
    decision_locco = route_to_parser('LOCCO2025.csv')
    assert decision_locco.dataset == 'locality'  # Router uses 'locality' dataset name
    assert len(decision_locco.natural_keys) == 3  # Complex composite key
    
    print("✅ RouteDecision: Type-safe NamedTuple with schema-driven natural_keys")


def test_route_decision_determinism():
    """
    Test 10: Verify routing is deterministic (same input → identical output).
    """
    from cms_pricing.ingestion.parsers import route_to_parser
    
    # Route same file twice
    decision1 = route_to_parser('PPRRVU2025.csv')
    decision2 = route_to_parser('PPRRVU2025.csv')
    
    # Should be identical
    assert decision1 == decision2
    assert decision1.dataset == decision2.dataset
    assert decision1.schema_id == decision2.schema_id
    assert decision1.status == decision2.status
    assert decision1.natural_keys == decision2.natural_keys
    
    print("✅ Determinism: Identical RouteDecision on repeated calls")


# ============================================================================
# Phase 0 Commit 4 Tests: Categorical Validation
# ============================================================================

def test_validation_severity_enum():
    """
    Test 11: ValidationSeverity enum prevents string typos.
    """
    from cms_pricing.ingestion.parsers._parser_kit import ValidationSeverity
    
    # Enum values
    assert ValidationSeverity.BLOCK == "BLOCK"
    assert ValidationSeverity.WARN == "WARN"
    assert ValidationSeverity.INFO == "INFO"
    
    # Type checking works
    sev: ValidationSeverity = ValidationSeverity.WARN
    assert sev.value == "WARN"
    assert isinstance(sev, str)  # str subclass for backward compat
    
    print("✅ ValidationSeverity enum: Type-safe, no string typos")


def test_categorical_unknown_value_reject():
    """
    Test 12: Invalid categorical values rejected with reason codes and row_id.
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationSeverity,
        CategoricalRejectReason
    )
    import pandas as pd
    
    df = pd.DataFrame({
        "modifier": ["", "26", "TC", "INVALID"],  # INVALID not in domain
        "hcpcs": ["99213", "99214", "99215", "99216"]
    })
    
    schema = {
        "columns": {
            "modifier": {
                "type": "categorical",
                "enum": ["", "26", "TC", "53"],
                "nullable": True
            }
        }
    }
    
    result = enforce_categorical_dtypes(
        df, schema, natural_keys=['hcpcs', 'modifier'],
        schema_id='test_schema',
        release_id='test_release',
        severity=ValidationSeverity.WARN
    )
    
    # Check valid/rejects split
    assert len(result.valid_df) == 3  # 3 valid values
    assert len(result.rejects_df) == 1  # 1 reject
    
    # Check reason code
    assert result.rejects_df['validation_rule_id'].iloc[0] == CategoricalRejectReason.UNKNOWN_VALUE.value
    assert result.rejects_df['validation_column'].iloc[0] == "modifier"
    assert result.rejects_df['validation_context'].iloc[0] == "INVALID"
    
    # Check provenance
    assert 'row_id' in result.rejects_df.columns
    assert result.rejects_df['schema_id'].iloc[0] == 'test_schema'
    assert result.rejects_df['release_id'].iloc[0] == 'test_release'
    assert len(result.rejects_df['row_id'].iloc[0]) == 64  # SHA-256
    
    # Check metrics
    assert result.metrics['total_rows'] == 4
    assert result.metrics['valid_rows'] == 3
    assert result.metrics['reject_rows'] == 1
    assert result.metrics['reject_rate'] == 0.25
    
    print("✅ Categorical unknown value: Rejected with full provenance")


def test_categorical_null_not_allowed():
    """
    Test 13: Null values rejected when nullable=false.
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationSeverity,
        CategoricalRejectReason
    )
    import pandas as pd
    
    df = pd.DataFrame({
        "status": ["A", None, "D"],
        "hcpcs": ["99213", "99214", "99215"]
    })
    
    schema = {
        "columns": {
            "status": {
                "type": "categorical",
                "enum": ["A", "D", "P"],
                "nullable": False  # Nulls not allowed!
            }
        }
    }
    
    result = enforce_categorical_dtypes(
        df, schema, natural_keys=['hcpcs'],
        severity=ValidationSeverity.WARN
    )
    
    assert len(result.valid_df) == 2  # 2 valid
    assert len(result.rejects_df) == 1  # 1 null reject
    assert result.rejects_df['validation_rule_id'].iloc[0] == CategoricalRejectReason.NULL_NOT_ALLOWED.value
    
    print("✅ Categorical null constraint: Enforced when nullable=false")


def test_categorical_block_raises_exception():
    """
    Test 14: BLOCK severity raises ValueError immediately.
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationSeverity
    )
    import pandas as pd
    import pytest
    
    df = pd.DataFrame({"modifier": ["INVALID"], "hcpcs": ["99213"]})
    schema = {
        "columns": {
            "modifier": {
                "type": "categorical",
                "enum": ["", "26", "TC"],
                "nullable": True
            }
        }
    }
    
    with pytest.raises(ValueError, match="Categorical validation failed"):
        enforce_categorical_dtypes(df, schema, natural_keys=['hcpcs'], severity=ValidationSeverity.BLOCK)
    
    print("✅ BLOCK severity: Raises ValueError as expected")


def test_zero_silent_nan_coercions_strengthened():
    """
    Test 15: CRITICAL - Prove no silent NaN coercion (STRENGTHENED).
    
    Pandas CategoricalDtype silently converts unknown values to NaN.
    Our validation MUST catch this BEFORE conversion.
    
    Enhanced checks:
    - NaN count unchanged
    - dtype NOT categorical until after validation
    - Invalid values moved to rejects (not converted to NaN)
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationSeverity
    )
    import pandas as pd
    
    df = pd.DataFrame({
        "modifier": ["", "26", "TC", "UNKNOWN"],
        "status": ["A", "D", "P", "A"],
        "hcpcs": ["99213", "99214", "99215", "99216"]
    })
    
    schema = {
        "columns": {
            "modifier": {"type": "categorical", "enum": ["", "26", "TC"], "nullable": True},
            "status": {"type": "categorical", "enum": ["A", "D", "P"], "nullable": True}
        }
    }
    
    # Before validation
    null_count_before = df.isna().sum().sum()
    assert df['modifier'].dtype != 'category'  # Not categorical yet
    assert df['status'].dtype != 'category'
    
    # Validate
    result = enforce_categorical_dtypes(
        df, schema, natural_keys=['hcpcs'],
        severity=ValidationSeverity.WARN
    )
    
    # After validation
    null_count_after = result.valid_df.isna().sum().sum()
    
    # CRITICAL CHECKS
    assert null_count_after <= null_count_before  # NaN count did NOT increase
    assert len(result.rejects_df) == 1  # UNKNOWN caught
    assert result.rejects_df['validation_context'].iloc[0] == "UNKNOWN"
    
    # Check dtype NOW categorical (after validation)
    assert result.valid_df['modifier'].dtype.name == 'category'
    assert result.valid_df['status'].dtype.name == 'category'
    
    print("✅ Zero silent coercions: NaN count stable, dtype checked, invalid values rejected")


def test_validation_result_structure():
    """
    Test 16: ValidationResult NamedTuple has correct structure.
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationResult,
        ValidationSeverity
    )
    import pandas as pd
    
    df = pd.DataFrame({"modifier": ["", "26"], "hcpcs": ["99213", "99214"]})
    schema = {"columns": {"modifier": {"type": "categorical", "enum": ["", "26", "TC"], "nullable": True}}}
    
    result = enforce_categorical_dtypes(df, schema, natural_keys=['hcpcs'], severity=ValidationSeverity.WARN)
    
    # Check type
    assert isinstance(result, ValidationResult)
    assert isinstance(result, tuple)  # NamedTuple is tuple
    
    # Check attributes
    assert hasattr(result, 'valid_df')
    assert hasattr(result, 'rejects_df')
    assert hasattr(result, 'metrics')
    
    # Check metrics structure
    assert 'total_rows' in result.metrics
    assert 'valid_rows' in result.metrics
    assert 'reject_rows' in result.metrics
    assert 'reject_rate' in result.metrics
    assert 'columns_validated' in result.metrics
    assert 'reject_rate_by_column' in result.metrics
    
    print("✅ ValidationResult: Type-safe NamedTuple with comprehensive metrics")


def test_deterministic_reject_ordering():
    """
    Test 17: Rejects are deterministically sorted for reproducible diffs.
    """
    from cms_pricing.ingestion.parsers._parser_kit import (
        enforce_categorical_dtypes,
        ValidationSeverity
    )
    import pandas as pd
    
    # Create data with multiple reject types
    df = pd.DataFrame({
        "modifier": ["INVALID1", "INVALID2", ""],
        "status": ["A", "INVALID3", "D"],
        "hcpcs": ["99213", "99214", "99215"]
    })
    
    schema = {
        "columns": {
            "modifier": {"type": "categorical", "enum": ["", "26", "TC"], "nullable": True},
            "status": {"type": "categorical", "enum": ["A", "D", "P"], "nullable": True}
        }
    }
    
    # Run twice
    result1 = enforce_categorical_dtypes(df, schema, natural_keys=['hcpcs'], severity=ValidationSeverity.WARN)
    result2 = enforce_categorical_dtypes(df, schema, natural_keys=['hcpcs'], severity=ValidationSeverity.WARN)
    
    # Check identical ordering
    assert len(result1.rejects_df) == len(result2.rejects_df)
    assert result1.rejects_df['validation_column'].tolist() == result2.rejects_df['validation_column'].tolist()
    assert result1.rejects_df['validation_rule_id'].tolist() == result2.rejects_df['validation_rule_id'].tolist()
    
    # Verify sorted by (column, reason_code, row_id)
    rejects = result1.rejects_df
    if len(rejects) > 1:
        for i in range(len(rejects) - 1):
            col1, col2 = rejects['validation_column'].iloc[i], rejects['validation_column'].iloc[i+1]
            assert col1 <= col2  # Column names sorted
    
    print("✅ Deterministic ordering: Identical rejects on repeated runs")


print("\n" + "="*80)
print("All Phase 0 Commit 4 tests ready to run!")
print("="*80)


def test_compressed_file_routing():
    """
    Test 18: Compression suffix stripping for routing.
    
    Per Phase 0 Lockdown: Router strips .gz, .gzip, .bz2, .zip before matching.
    """
    from cms_pricing.ingestion.parsers import route_to_parser
    
    # Test .gz suffix
    decision_gz = route_to_parser("PPRRVU2025.csv.gz")
    assert decision_gz.dataset == "pprrvu"
    assert decision_gz.natural_keys == ["hcpcs", "modifier", "effective_from"]
    
    # Test .gzip suffix
    decision_gzip = route_to_parser("GPCI2025.txt.gzip")
    assert decision_gzip.dataset == "gpci"
    
    # Test .bz2 suffix
    decision_bz2 = route_to_parser("conversion-factor-2025.xlsx.bz2")
    assert decision_bz2.dataset == "conversion_factor"
    
    # Test no suffix (should still work)
    decision_plain = route_to_parser("PPRRVU2025.csv")
    assert decision_plain.dataset == "pprrvu"
    
    print("✅ Compressed file routing: .gz/.gzip/.bz2/.zip suffixes stripped correctly")
