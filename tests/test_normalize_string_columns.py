"""
Tests for normalize_string_columns utility in parser kit.

Per DIS §3.4 Normalize Stage and STD-parser-contracts v1.7 Anti-Pattern 9.
"""
import pandas as pd
import pytest

from cms_pricing.ingestion.parsers._parser_kit import normalize_string_columns


def test_normalize_strips_leading_trailing_whitespace():
    """Strip whitespace from string columns."""
    df = pd.DataFrame({
        'cf_type': ['physician ', ' anesthesia', '  physician  '],
        'description': ['Test ', ' Data', 'Clean']
    })
    
    result = normalize_string_columns(df)
    
    assert result['cf_type'].tolist() == ['physician', 'anesthesia', 'physician']
    assert result['description'].tolist() == ['Test', 'Data', 'Clean']


def test_normalize_handles_nbsp():
    """Replace non-breaking spaces (\\xa0) with regular spaces."""
    df = pd.DataFrame({
        'cf_type': ['physician\xa0', '\xa0anesthesia', 'physician\u00a0']
    })
    
    result = normalize_string_columns(df)
    
    # NBSP replaced with space, then stripped
    assert result['cf_type'].tolist() == ['physician', 'anesthesia', 'physician']


def test_normalize_specific_columns_only():
    """Normalize only specified columns."""
    df = pd.DataFrame({
        'cf_type': [' physician '],
        'description': [' Keep Whitespace ']
    })
    
    result = normalize_string_columns(df, columns=['cf_type'])
    
    assert result['cf_type'].iloc[0] == 'physician'
    assert result['description'].iloc[0] == ' Keep Whitespace '  # Unchanged


def test_normalize_empty_to_null():
    """Convert empty strings to None when enabled."""
    df = pd.DataFrame({
        'cf_type': ['physician', '', '   ', 'anesthesia']
    })
    
    result = normalize_string_columns(df, empty_to_null=True)
    
    assert result['cf_type'].tolist()[0] == 'physician'
    assert pd.isna(result['cf_type'].iloc[1])  # Empty string → None
    # Note: '   ' strips to '' then becomes None
    assert result['cf_type'].tolist()[3] == 'anesthesia'


def test_normalize_skips_non_string_columns():
    """Don't modify numeric or other non-string columns."""
    df = pd.DataFrame({
        'cf_type': [' physician '],
        'cf_value': [32.3465],
        'year': [2025]
    })
    
    result = normalize_string_columns(df)
    
    assert result['cf_type'].iloc[0] == 'physician'
    assert result['cf_value'].iloc[0] == 32.3465  # Unchanged
    assert result['year'].iloc[0] == 2025  # Unchanged


def test_normalize_auto_detects_string_columns():
    """When columns=None, auto-detect and normalize all string columns."""
    df = pd.DataFrame({
        'cf_type': [' physician '],
        'description': [' Test '],
        'cf_value': [32.3465]
    })
    
    result = normalize_string_columns(df)  # No columns specified
    
    assert result['cf_type'].iloc[0] == 'physician'
    assert result['description'].iloc[0] == 'Test'
    assert result['cf_value'].iloc[0] == 32.3465


def test_normalize_preserves_original_dataframe():
    """normalize_string_columns returns a copy, doesn't modify original."""
    df_original = pd.DataFrame({
        'cf_type': [' physician ']
    })
    
    df_normalized = normalize_string_columns(df_original)
    
    # Original unchanged
    assert df_original['cf_type'].iloc[0] == ' physician '
    # Result is cleaned
    assert df_normalized['cf_type'].iloc[0] == 'physician'


def test_normalize_mixed_whitespace_issues():
    """Handle combination of issues: leading, trailing, NBSP, tabs."""
    df = pd.DataFrame({
        'code': [' \t CODE1 \xa0', '\xa0CODE2\t ', '  CODE3  ']
    })
    
    result = normalize_string_columns(df)
    
    assert result['code'].tolist() == ['CODE1', 'CODE2', 'CODE3']


def test_normalize_empty_dataframe():
    """Handle empty DataFrame gracefully."""
    df = pd.DataFrame()
    
    result = normalize_string_columns(df)
    
    assert len(result) == 0


def test_normalize_missing_column():
    """Gracefully skip if specified column doesn't exist."""
    df = pd.DataFrame({'cf_type': [' physician ']})
    
    # Specify a column that doesn't exist
    result = normalize_string_columns(df, columns=['cf_type', 'nonexistent'])
    
    assert result['cf_type'].iloc[0] == 'physician'
    assert 'nonexistent' not in result.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

