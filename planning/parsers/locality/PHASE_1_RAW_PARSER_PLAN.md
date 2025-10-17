# Phase 1: Raw Locality Parser Implementation Plan

**Target Time:** 60 minutes  
**Status:** Ready to start  
**Pattern:** Follow GPCI parser (proven, QTS-compliant)  
**Goal:** Layout-faithful parser for 25LOCCO.txt (NO FIPS derivation yet)

---

## Overview

**What we're building:**
- Raw parser that reads CMS 25LOCCO.txt exactly as-is
- Parses state NAMES and county NAMES (not FIPS codes)
- Follows STD-parser-contracts v1.9 §21.1 (11-step template)
- QTS-compliant golden tests
- Single format first (TXT fixed-width)

**What we're NOT doing (yet):**
- ❌ FIPS code derivation (that's Phase 2 - enrich stage)
- ❌ County name splitting/explosion
- ❌ Reference data joins
- ❌ CSV/XLSX formats (Phase 2)

---

## File Structure Reference

**Source File:** `sample_data/rvu25d_0/25LOCCO.txt` or `data/cms_raw/2025_D_rvu_data/25LOCCO.txt`

**Expected Layout (Fixed-Width):**
```
Columns:
1-2:   MAC (Medicare Administrative Contractor)
4-5:   Locality Code
7-30:  State Name
32-?:  County Names (comma-delimited list)
```

**Example Row:**
```
01 01 ALABAMA            AUTAUGA, BALDWIN, BARBOUR
```

---

## Implementation Breakdown (60 min)

### Step 1: Create Layout Registry Entry (10 min)

**File:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Add:**
```python
"LOCCO_2025D": {
    "version": "2025.4.1",
    "source": "CMS PFS Relative Value Files 2025D",
    "url": "https://www.cms.gov/...",
    "effective_from": "2025-01-01",
    "columns": [
        {"name": "mac", "start": 0, "end": 2, "dtype": "str"},
        {"name": "locality_code", "start": 3, "end": 5, "dtype": "str"},
        {"name": "state_name", "start": 6, "end": 30, "dtype": "str"},
        {"name": "county_names", "start": 31, "end": None, "dtype": "str"},  # Rest of line
    ],
    "schema_id": "cms_locality_raw_v1.0",
    "natural_keys": ["mac", "locality_code"],
}
```

**Verification:**
- Layout registered (GitHub task #361)
- Column positions match actual file (task #362)
- Test with sample row (task #368)

---

### Step 2: Create Parser Module (25 min)

**File:** `cms_pricing/ingestion/parsers/locality_parser.py`

**Structure (follow GPCI pattern):**
```python
"""
Locality-County Crosswalk Parser (Raw)

Parses CMS 25LOCCO.txt files to raw schema (no FIPS derivation).
Per STD-parser-contracts v1.9 §21.1 (11-step template).

Two-stage architecture:
- Stage 1 (this parser): Layout-faithful parsing (state/county NAMES)
- Stage 2 (enricher): FIPS derivation + county explosion

Reference: planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md
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
from cms_pricing.ingestion.parsers.layout_registry import get_layout

logger = structlog.get_logger(__name__)

# Constants
PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_locality_raw_v1.0"
NATURAL_KEYS = ["mac", "locality_code"]

# Alias map (if CSV/XLSX support added later)
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'state': 'state_name',
    'county': 'county_names',
    'counties': 'county_names',
}


def parse_locality_raw(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse Locality-County file (layout-faithful, no FIPS derivation).
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required from ingestor (release_id, schema_id, etc.)
    
    Returns:
        ParseResult with raw data (state/county NAMES, not FIPS)
    """
    
    start_time = time.time()
    
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
    df = normalize_string_columns(df, ['mac', 'locality_code', 'state_name', 'county_names'])
    
    # Step 7: Check natural key uniqueness
    duplicates = check_natural_key_uniqueness(df, NATURAL_KEYS)
    if duplicates:
        logger.warning("duplicate_natural_keys", count=len(duplicates))
    
    # Step 8: Build metrics
    parse_time_ms = (time.time() - start_time) * 1000
    metrics = build_parser_metrics(
        row_count=len(df),
        reject_count=0,  # Phase 1: no rejects (layout-faithful)
        parse_time_ms=parse_time_ms,
        encoding=detected_encoding,
        schema_id=SCHEMA_ID,
        parser_version=PARSER_VERSION
    )
    
    # Step 9: Finalize output
    return finalize_parser_output(
        data=df,
        rejects=[],
        metrics=metrics,
        metadata=metadata,
        natural_keys=NATURAL_KEYS,
        schema_id=SCHEMA_ID
    )


def _parse_txt_fixed_width(text_content: str, metadata: Dict[str, Any]) -> pd.DataFrame:
    """Parse fixed-width TXT using layout registry."""
    
    layout_id = "LOCCO_2025D"
    layout = get_layout(layout_id, metadata.get('product_year', 2025))
    
    # Read fixed-width
    rows = []
    for line_no, line in enumerate(text_content.splitlines(), start=1):
        if not line.strip():
            continue
        
        row = {}
        for col in layout['columns']:
            start = col['start']
            end = col['end']
            value = line[start:end].strip() if end else line[start:].strip()
            row[col['name']] = value
        
        rows.append(row)
    
    return pd.DataFrame(rows)
```

**Key Points:**
- Layout-faithful (parses NAMES, not FIPS) — tracked via GitHub task #361
- `_parser_kit` utilities reused (no duplication) — task #362
- Natural keys: `(mac, locality_code)`
- No transformation (leaves county names as comma-delimited string)
- Single format (TXT) for Phase 1

---

### Step 3: Create Golden Test Fixture (10 min)

**File:** `tests/fixtures/locality/25LOCCO_golden.txt`

**Create minimal golden fixture (5-10 rows):**
```
01 01 ALABAMA            AUTAUGA, BALDWIN
01 99 ALABAMA            REST OF ALABAMA
02 01 ALASKA             ANCHORAGE
06 05 CALIFORNIA         LOS ANGELES
```

**Manifest:** `tests/fixtures/locality/manifest.json`
```json
{
  "dataset": "locality",
  "source": "cms_pfs_2025d",
  "files": [
    {
      "filename": "25LOCCO_golden.txt",
      "format": "txt_fixed_width",
      "rows": 10,
      "schema_id": "cms_locality_raw_v1.0",
      "purpose": "golden_happy_path",
      "notes": "Clean fixture, 0 rejects, representative localities"
    }
  ]
}
```

---

### Step 4: Create Golden Test (10 min)

**File:** `tests/parsers/test_locality_parser.py`

```python
"""
Tests for Locality Parser (Raw - Phase 1)

Per STD-qa-testing v1.3 (Golden Test Pattern).
"""

import pytest
from pathlib import Path
from io import BytesIO

from cms_pricing.ingestion.parsers.locality_parser import parse_locality_raw


@pytest.mark.golden
def test_locality_raw_txt_golden():
    """
    Golden test: Parse 25LOCCO.txt with clean fixture.
    
    Expectations:
    - Exact row count
    - 0 rejects
    - Natural keys unique
    - Columns: mac, locality_code, state_name, county_names (NAMES, not FIPS)
    """
    
    fixture_path = Path("tests/fixtures/locality/25LOCCO_golden.txt")
    
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://fixture'
    }
    
    with open(fixture_path, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO_golden.txt', metadata)
    
    # Assertions
    assert len(result.data) == 10, "Expected 10 rows in golden fixture"
    assert len(result.rejects) == 0, "Golden fixture should have 0 rejects"
    
    # Schema check (raw - NAMES not FIPS)
    expected_cols = {'mac', 'locality_code', 'state_name', 'county_names'}
    assert set(result.data.columns) >= expected_cols
    
    # Natural key uniqueness
    duplicates = result.data.duplicated(subset=['mac', 'locality_code'])
    assert not duplicates.any(), "Natural keys must be unique"
    
    # Verify NAMES (not FIPS codes)
    assert 'ALABAMA' in result.data['state_name'].values
    assert 'AUTAUGA' in result.data['county_names'].iloc[0]  # First row
    
    # Metrics
    assert result.metrics['row_count'] == 10
    assert result.metrics['reject_count'] == 0
    assert result.metrics['schema_id'] == 'cms_locality_raw_v1.0'


def test_locality_natural_key_uniqueness():
    """Test natural key uniqueness check."""
    # Create fixture with duplicate (mac, locality_code)
    # Expect warning in logs
    pass  # Implement if needed
```

---

### Step 5: Run and Debug (5 min)

```bash
# Run golden test
cd /Users/alexanderbea/Cursor/cms-api
export REF_MODE=inline  # Use inline dict for dev
pytest tests/parsers/test_locality_parser.py::test_locality_raw_txt_golden -v

# Expected output:
# ✅ PASSED (10 rows, 0 rejects)
```

**Debug checklist:**
- Confirm layout column positions (Project task #362)
- Verify encoding detection against sample data
- Check string normalization (trim/uppercase)
- Confirm natural key uniqueness checks
- Ensure parser metrics populated

---

## Deliverables Checklist

### Code
- Layout registry entry `LOCCO_2025D` committed
- Parser module `locality_parser.py` implemented (target 250-300 lines)
- Reuse `_parser_kit` utilities (no duplication)

### Tests
- Golden fixture `25LOCCO_golden.txt` (~10 rows) prepared
- Fixture manifest `manifest.json` updated
- Golden test `test_locality_raw_txt_golden()` added
- Golden test passes (10 rows, 0 rejects)

### Documentation
- Parser docstring references two-stage architecture doc
- Inline comments explain "layout-faithful, no FIPS" constraint
- Natural key assumptions documented

---

## Success Criteria

**Must Pass:**
- ✅ Golden test: 100% pass rate
- ✅ Exact row count (10 expected)
- ✅ 0 rejects (clean golden fixture per QTS §5.1.1)
- ✅ Natural keys unique
- ✅ Parse time < 50ms (small fixture)
- ✅ Encoding detection works (UTF-8/CP1252)

**Schema Contract:**
```python
{
    "schema_id": "cms_locality_raw_v1.0",
    "columns": {
        "mac": "str",               # As-is from layout
        "locality_code": "str",      # As-is from layout
        "state_name": "str",         # State NAME (not FIPS)
        "county_names": "str"        # Comma-delimited NAMES (not FIPS)
    },
    "natural_keys": ["mac", "locality_code"],
    "row_hash_excludes": ["ingestion_metadata"],
    "note": "Raw schema - no FIPS codes. Enrich stage derives FIPS."
}
```

---

## What's Next (Phase 2)

After Phase 1 complete:
- Add CSV format support (header normalization)
- Add consistency tests (TXT vs CSV)
- Create FIPS enricher (separate module)
- County name explosion (1 row → N counties)

---

## Time Breakdown

| Step | Task | Time | Cumulative |
|------|------|------|------------|
| 1 | Layout registry entry | 10 min | 10 min |
| 2 | Parser module | 25 min | 35 min |
| 3 | Golden fixture | 10 min | 45 min |
| 4 | Golden test | 10 min | 55 min |
| 5 | Run & debug | 5 min | **60 min** |

**Target:** 60 minutes  
**Buffer:** None (GPCI pattern proven, low risk)

---

## References

**PRDs:**
- `STD-parser-contracts-prd-v1.0.md` v1.9 §21.1 (11-step template)
- `STD-qa-testing-prd-v1.0.md` v1.3 §5.1 (Golden test pattern)
- `STD-data-architecture-impl-v1.0.md` §1.3 (Transformation boundaries)

**Planning:**
- `planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md` - Overall architecture
- `planning/parsers/locality/PRE_IMPLEMENTATION_VERIFICATION.md` - Format verification

**Code Patterns:**
- `cms_pricing/ingestion/parsers/gpci_parser.py` - Proven pattern to follow
- `cms_pricing/ingestion/parsers/_parser_kit.py` - Shared utilities

**Test Patterns:**
- `tests/parsers/test_gpci_parser.py` - Golden test reference
