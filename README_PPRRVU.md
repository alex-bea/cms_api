# PPRRVU Parser

**Parser for CMS Medicare Physician Fee Schedule (MPFS) Physician/Practitioner Relative Value Units**

## Overview

The PPRRVU parser processes CMS MPFS RVU data files (fixed-width TXT, CSV, XLSX) into a canonical, validated schema with deterministic hashing and comprehensive metadata.

**Contract:** `STD-parser-contracts v1.2 §21`  
**Schema:** `cms_pprrvu_v1.1`  
**Parser Version:** `v1.0.0`

## Features

✅ **Multi-format support:** TXT (fixed-width), CSV, XLSX  
✅ **Encoding detection:** UTF-8 → CP1252 → Latin-1 cascade  
✅ **Schema-driven precision:** 2 decimals, HALF_UP rounding  
✅ **Categorical validation:** Pre-cast domain checks  
✅ **Natural key uniqueness:** BLOCK severity (hard-fail on duplicates)  
✅ **Deterministic hashing:** 64-char SHA-256 row hashes  
✅ **Comprehensive metadata:** 9 metadata columns injected  
✅ **Performance:** < 2s for 10K rows  

## Usage

```python
from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu
from datetime import datetime

# Prepare metadata
metadata = {
    'release_id': 'mpfs_2025_q4',
    'product_year': '2025',
    'quarter_vintage': '2025Q4',
    'vintage_date': datetime(2025, 10, 1),
    'file_sha256': 'abc123...',
    'source_uri': 'https://www.cms.gov/files/zip/rvu25d-0.zip',
    'schema_id': 'cms_pprrvu_v1.1',
    'layout_version': 'v2025.4.0'
}

# Parse file
with open('PPRRVU2025_Oct.txt', 'rb') as f:
    result = parse_pprrvu(f, 'PPRRVU2025_Oct.txt', metadata)

# Access results
print(f"Valid rows: {len(result.data)}")
print(f"Rejected rows: {len(result.rejects)}")
print(f"Duration: {result.metrics['parse_duration_sec']:.3f}s")
print(f"Encoding: {result.metrics['encoding_detected']}")

# Save to database or parquet
result.data.to_parquet('pprrvu_2025_q4.parquet', index=False)
```

## Natural Keys

**Columns:** `['hcpcs', 'modifier', 'status_code', 'effective_from']`

⚠️ **Critical:** Includes `status_code` to prevent collisions across A/B/C/I/J/T codes.

Duplicates raise `DuplicateKeyError` (severity=BLOCK).

## Data Columns

| Column | Type | Precision | Description |
|--------|------|-----------|-------------|
| `hcpcs` | str (categorical) | - | HCPCS/CPT code |
| `modifier` | str (categorical, nullable) | - | Procedure modifier |
| `status_code` | str (categorical) | - | Status code (A/B/C/I/J/T/X) |
| `work_rvu` | float64 | 2 | Work RVU |
| `pe_rvu_nonfac` | float64 | 2 | PE RVU non-facility |
| `pe_rvu_fac` | float64 | 2 | PE RVU facility |
| `mp_rvu` | float64 | 2 | Malpractice RVU |
| `global_days` | Int64 | - | Global days |
| `na_indicator` | str | - | Not applicable indicator |
| `opps_cap_applicable` | str/bool | - | OPPS cap applicable |
| `effective_from` | date | - | Effective date |

## Metadata Columns

All rows include 9 metadata columns:

- `release_id` - Ingest release identifier
- `vintage_date` - Data vintage date
- `product_year` - Product year (e.g., "2025")
- `quarter_vintage` - Quarter vintage (e.g., "2025Q4")
- `source_filename` - Original filename
- `source_file_sha256` - SHA-256 of source file
- `source_uri` - Source URL
- `parsed_at` - Parse timestamp (UTC)
- `row_content_hash` - 64-char SHA-256 of row content

## Error Handling

### DuplicateKeyError

Raised when duplicate natural keys detected:

```python
from cms_pricing.ingestion.parsers._parser_kit import DuplicateKeyError

try:
    result = parse_pprrvu(f, filename, metadata)
except DuplicateKeyError as e:
    print(f"Found {len(e.duplicates)} duplicate natural keys:")
    for dup in e.duplicates:
        print(f"  {dup}")
```

### LayoutMismatchError

Raised when fixed-width parsing fails:

```python
from cms_pricing.ingestion.parsers._parser_kit import LayoutMismatchError

try:
    result = parse_pprrvu(f, filename, metadata)
except LayoutMismatchError as e:
    print(f"Layout error: {e}")
```

### CategoryValidationError

Raised when invalid categorical values found (status_code domain).

## Testing

Run comprehensive test suite:

```bash
pytest tests/ingestion/test_pprrvu_parser.py -v
```

Tests cover:
1. Golden fixture (determinism)
2. Precision/rounding (HALF_UP)
3. Natural key uniqueness
4. Layout mismatch handling
5. Hash metadata exclusion
6. Performance (< 2s for 10K rows)
7. Metrics structure

## Fixtures

**Golden:** `tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt` (94 rows)  
**Negative:**
- `bad_layout.txt` - Truncated rows
- `bad_dup_keys.txt` - Duplicate natural keys
- `bad_category.txt` - Invalid status code
- `bad_schema_regression.csv` - Banned column

## Performance

| Rows | Duration | Throughput |
|------|----------|------------|
| 100 | < 100ms | 1K rows/s |
| 1K | < 500ms | 2K rows/s |
| 10K | < 2s | 5K rows/s |

## Changelog

**v1.0.0** (2025-10-16)
- Initial implementation
- Multi-format support (TXT, CSV, XLSX)
- Schema-driven validation
- Deterministic hashing
- Natural key uniqueness enforcement (BLOCK severity)
- 7 comprehensive tests

---

**Maintainer:** CMS Pricing API Team  
**Contract:** STD-parser-contracts v1.2 §21  
**Last Updated:** 2025-10-16
