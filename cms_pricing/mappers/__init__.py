"""
Column Mappers - Schema ↔ API Transformations

Per STD-parser-contracts: Parsers produce schema columns (DB format).
API layer transforms to presentation format.

Naming Convention:
  - Schema (DB): rvu_work, rvu_pe_nonfac, rvu_pe_fac, rvu_malp (prefixed, grouped)
  - API (presentation): work_rvu, pe_rvu_nonfac, pe_rvu_fac, mp_rvu (suffixed, intuitive)
  
Rationale:
  - Schema uses prefixes for logical grouping in database
  - API uses suffixes for intuitive field names
  - Transformation happens at serialization boundary
"""

# PPRRVU Schema ↔ API Mapping
PPRRVU_SCHEMA_TO_API = {
    # Schema (DB canonical) → API (presentation)
    'rvu_work': 'work_rvu',
    'rvu_pe_nonfac': 'pe_rvu_nonfac',
    'rvu_pe_fac': 'pe_rvu_fac',
    'rvu_malp': 'mp_rvu',
}

PPRRVU_API_TO_SCHEMA = {v: k for k, v in PPRRVU_SCHEMA_TO_API.items()}


def schema_to_api(df, mapping=None):
    """
    Transform schema columns to API presentation format.
    
    Args:
        df: DataFrame with schema column names
        mapping: Custom mapping dict (default: PPRRVU_SCHEMA_TO_API)
        
    Returns:
        DataFrame with API column names
        
    Example:
        >>> df_api = schema_to_api(df_schema)
        >>> # rvu_work → work_rvu
    """
    if mapping is None:
        mapping = PPRRVU_SCHEMA_TO_API
    return df.rename(columns=mapping)


def api_to_schema(df, mapping=None):
    """
    Transform API columns to schema format (for writes).
    
    Args:
        df: DataFrame with API column names
        mapping: Custom mapping dict (default: PPRRVU_API_TO_SCHEMA)
        
    Returns:
        DataFrame with schema column names
        
    Example:
        >>> df_schema = api_to_schema(df_api)
        >>> # work_rvu → rvu_work
    """
    if mapping is None:
        mapping = PPRRVU_API_TO_SCHEMA
    return df.rename(columns=mapping)
