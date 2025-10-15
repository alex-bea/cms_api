"""
Shared metadata extraction utilities.

Extracts vintage metadata from manifests, filenames, or release IDs.
Used by MPFS, RVU, and other ingestors to avoid duplication.

Follows DIS v1.0 standards with three vintage fields:
- vintage_date: When data was published (timestamp)
- product_year: Valuation year (e.g., "2025")
- quarter_vintage: Quarter identifier (e.g., "2025Q4", "2025_annual")
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import structlog

logger = structlog.get_logger()


def extract_vintage_metadata(
    manifest_path: Optional[Path] = None,
    release_id: Optional[str] = None,
    filename: Optional[str] = None,
    discovered_at: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Extract vintage metadata from available sources.
    
    Priority: manifest > release_id > filename
    
    Called once in ingestor __init__ to avoid duplication.
    
    Args:
        manifest_path: Path to discovery manifest JSON
        release_id: Release identifier (e.g., "mpfs_2025_q4_20251015")
        filename: Filename to parse if no manifest/release_id
        discovered_at: Override discovered timestamp
    
    Returns:
        Dictionary with:
            - vintage_date: datetime (when data was published)
            - product_year: str (e.g., "2025")
            - quarter_vintage: str (e.g., "2025Q4", "2025_annual")
            - revision: str or None (e.g., "A", "B", "C", "D")
            
    Examples:
        >>> extract_vintage_metadata(release_id="mpfs_2025_q4_20251015")
        {
            'vintage_date': datetime(2025, 10, 15, ...),
            'product_year': '2025',
            'quarter_vintage': '2025Q4',
            'revision': None
        }
        
        >>> extract_vintage_metadata(filename="PPRRVU2025D.txt")
        {
            'vintage_date': datetime.now(),
            'product_year': '2025',
            'quarter_vintage': '2025Q4',
            'revision': 'D'
        }
    """
    logger.debug(
        "Extracting vintage metadata",
        manifest_path=str(manifest_path) if manifest_path else None,
        release_id=release_id,
        filename=filename
    )
    
    # Try manifest first (highest priority)
    if manifest_path and manifest_path.exists():
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            
            vintage_date = datetime.fromisoformat(
                manifest.get('discovered_at', datetime.utcnow().isoformat())
            )
            release_id = manifest.get('release_id', release_id)
            
            logger.debug("Extracted vintage from manifest", vintage_date=vintage_date)
        except Exception as e:
            logger.warning("Failed to read manifest", error=str(e))
            vintage_date = discovered_at or datetime.utcnow()
    else:
        vintage_date = discovered_at or datetime.utcnow()
    
    # Parse release_id or filename for year/quarter/revision
    source = release_id or filename or ""
    
    # Extract product year: look for 4-digit year starting with 20
    year_match = re.search(r'(20\d{2})', source)
    product_year = year_match.group(1) if year_match else "2025"
    
    # Extract quarter/revision
    revision = None
    quarter = None
    
    # Try explicit quarter notation: Q1, Q2, Q3, Q4
    quarter_match = re.search(r'[Qq]([1-4])', source)
    if quarter_match:
        quarter = f"Q{quarter_match.group(1)}"
    else:
        # Try revision letter: A, B, C, D (map to quarters)
        revision_match = re.search(r'([ABCD])(?:\.|_|$|\s)', source, re.I)
        if revision_match:
            revision = revision_match.group(1).upper()
            quarter_map = {'A': 'Q1', 'B': 'Q2', 'C': 'Q3', 'D': 'Q4'}
            quarter = quarter_map[revision]
    
    # Build quarter_vintage
    if quarter:
        quarter_vintage = f"{product_year}{quarter}"
    else:
        quarter_vintage = f"{product_year}_annual"
    
    result = {
        'vintage_date': vintage_date,
        'product_year': product_year,
        'quarter_vintage': quarter_vintage,
        'revision': revision
    }
    
    logger.info(
        "Vintage metadata extracted",
        product_year=product_year,
        quarter_vintage=quarter_vintage,
        revision=revision
    )
    
    return result


def extract_year_from_filename(filename: str) -> Optional[str]:
    """
    Extract 4-digit year from filename.
    
    Args:
        filename: Filename to parse
        
    Returns:
        Year string (e.g., "2025") or None
    """
    match = re.search(r'(20\d{2})', filename)
    return match.group(1) if match else None


def extract_quarter_from_filename(filename: str) -> Optional[str]:
    """
    Extract quarter from filename.
    
    Handles: Q1-Q4, A-D notation
    
    Args:
        filename: Filename to parse
        
    Returns:
        Quarter string (e.g., "Q4") or None
    """
    # Try explicit quarter
    quarter_match = re.search(r'[Qq]([1-4])', filename)
    if quarter_match:
        return f"Q{quarter_match.group(1)}"
    
    # Try revision letter
    revision_match = re.search(r'([ABCD])(?:\.|_|$|\s)', filename, re.I)
    if revision_match:
        quarter_map = {'A': 'Q1', 'B': 'Q2', 'C': 'Q3', 'D': 'Q4'}
        return quarter_map[revision_match.group(1).upper()]
    
    return None


def build_release_id(
    dataset_name: str,
    product_year: str,
    quarter_vintage: str,
    timestamp: Optional[datetime] = None
) -> str:
    """
    Build standardized release ID.
    
    Format: {dataset}_{year}_{quarter}_{timestamp}
    
    Args:
        dataset_name: Dataset name (e.g., "mpfs", "rvu")
        product_year: Product year (e.g., "2025")
        quarter_vintage: Quarter vintage (e.g., "2025Q4", "2025_annual")
        timestamp: Optional timestamp (defaults to now)
    
    Returns:
        Release ID string
        
    Examples:
        >>> build_release_id("mpfs", "2025", "2025Q4")
        'mpfs_2025_q4_20251015_143022'
    """
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    # Extract quarter part
    if '_annual' in quarter_vintage:
        quarter_part = 'annual'
    else:
        quarter_match = re.search(r'Q(\d)', quarter_vintage)
        quarter_part = f"q{quarter_match.group(1)}" if quarter_match else 'annual'
    
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    return f"{dataset_name}_{product_year}_{quarter_part}_{timestamp_str}"


# Validation helpers

def validate_vintage_metadata(metadata: Dict[str, Any]) -> list:
    """
    Validate vintage metadata has required fields.
    
    Args:
        metadata: Vintage metadata dict
        
    Returns:
        List of validation errors (empty if valid)
    """
    required_fields = ['vintage_date', 'product_year', 'quarter_vintage']
    errors = []
    
    for field in required_fields:
        if field not in metadata:
            errors.append(f"Missing required field: {field}")
        elif metadata[field] is None:
            errors.append(f"Field is None: {field}")
    
    # Validate product_year format
    if 'product_year' in metadata:
        year = metadata['product_year']
        if not re.match(r'^20\d{2}$', str(year)):
            errors.append(f"Invalid product_year format: {year} (expected 20XX)")
    
    # Validate quarter_vintage format
    if 'quarter_vintage' in metadata:
        qv = metadata['quarter_vintage']
        if not re.match(r'^20\d{2}(Q[1-4]|_annual)$', str(qv)):
            errors.append(f"Invalid quarter_vintage format: {qv}")
    
    return errors

