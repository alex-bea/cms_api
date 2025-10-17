"""
Helpers for real-source variance testing per QTS §5.1.3.

Provides canonicalization and diff artifact generation for threshold-based
parity tests when using authentic CMS source files.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import json
from datetime import datetime


def canon_locality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonicalize Locality DataFrame for format comparison.
    
    Applies format normalization per Locality parser conventions:
    - Zero-pad MAC to 5 digits
    - Zero-pad locality_code to 2 digits
    - Strip whitespace from all string columns
    - Deterministic column order
    - Drop duplicates on natural keys
    - Sort by natural keys
    
    Args:
        df: Raw parsed DataFrame
        
    Returns:
        Canonicalized DataFrame ready for comparison
    """
    df = df.copy()
    
    # Format normalization (non-semantic)
    df['mac'] = df['mac'].astype(str).str.strip().str.zfill(5)
    df['locality_code'] = df['locality_code'].astype(str).str.strip().str.zfill(2)
    df['state_name'] = df['state_name'].astype(str).str.strip()
    df['fee_area'] = df['fee_area'].astype(str).str.strip()
    df['county_names'] = df['county_names'].astype(str).str.strip()
    
    # Canonical column order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Dedup and sort for comparison
    df = df.drop_duplicates(subset=['mac', 'locality_code'])
    df = df.sort_values(['mac', 'locality_code']).reset_index(drop=True)
    
    return df


def write_variance_artifacts(
    format_name: str,
    authority_df: pd.DataFrame,
    secondary_df: pd.DataFrame,
    parser_name: str,
    output_dir: Path = None
) -> Dict[str, Any]:
    """
    Generate variance report artifacts for real-source parity testing.
    
    Produces:
    - missing_in_<format>.csv: Natural keys in authority, missing in secondary
    - extra_in_<format>.csv: Natural keys in secondary, not in authority
    - parity_summary.json: Metrics, thresholds, counts
    
    Args:
        format_name: Secondary format name (e.g., "CSV", "XLSX")
        authority_df: Canonicalized authority DataFrame
        secondary_df: Canonicalized secondary DataFrame
        parser_name: Parser name for artifact naming (e.g., "locality")
        output_dir: Output directory (default: tests/artifacts/variance/)
        
    Returns:
        Summary dict with metrics
    """
    if output_dir is None:
        output_dir = Path("tests/artifacts/variance")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Natural key sets
    nk_auth = set(zip(authority_df['mac'], authority_df['locality_code']))
    nk_sec = set(zip(secondary_df['mac'], secondary_df['locality_code']))
    
    # Compute metrics
    missing = nk_auth - nk_sec
    extra = nk_sec - nk_auth
    overlap_count = len(nk_auth & nk_sec)
    overlap_pct = overlap_count / max(1, len(nk_auth))
    row_var_abs = abs(len(secondary_df) - len(authority_df))
    row_var_pct = row_var_abs / max(1, len(authority_df))
    
    # Write missing keys
    if missing:
        missing_df = authority_df[
            authority_df.apply(lambda r: (r['mac'], r['locality_code']) in missing, axis=1)
        ]
        missing_path = output_dir / f"{parser_name}_parity_missing_in_{format_name.lower()}.csv"
        missing_df.to_csv(missing_path, index=False)
    
    # Write extra keys
    if extra:
        extra_df = secondary_df[
            secondary_df.apply(lambda r: (r['mac'], r['locality_code']) in extra, axis=1)
        ]
        extra_path = output_dir / f"{parser_name}_parity_extra_in_{format_name.lower()}.csv"
        extra_df.to_csv(extra_path, index=False)
    
    # Summary
    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "parser": parser_name,
        "authority_format": "TXT",  # Hardcoded for Locality 2025D
        "secondary_format": format_name,
        "thresholds": {
            "nk_overlap_min": 0.98,
            "row_variance_pct_max": 0.01,
            "row_variance_abs_max": 2
        },
        "metrics": {
            "authority_row_count": len(authority_df),
            "secondary_row_count": len(secondary_df),
            "nk_overlap_count": overlap_count,
            "nk_overlap_pct": overlap_pct,
            "nk_missing_count": len(missing),
            "nk_extra_count": len(extra),
            "row_variance_abs": row_var_abs,
            "row_variance_pct": row_var_pct
        },
        "pass": {
            "nk_overlap": overlap_pct >= 0.98,
            "row_variance": (row_var_pct <= 0.01) or (row_var_abs <= 2)
        },
        "artifacts": {
            "missing": f"{parser_name}_parity_missing_in_{format_name.lower()}.csv" if missing else None,
            "extra": f"{parser_name}_parity_extra_in_{format_name.lower()}.csv" if extra else None
        }
    }
    
    summary_path = output_dir / f"{parser_name}_parity_summary_{format_name.lower()}.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary

