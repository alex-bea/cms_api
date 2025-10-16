# PPRRVU Golden Fixtures

## PPRRVU2025_sample.txt

**Source:** `sample_data/rvu25d_0/PPRRVU2025_Oct.txt` (lines 1-101)  
**SHA-256:** `b4437f4534b999e1764a4bbb4c13f05dc7e18e256bdbc9cd87e82a8caed05e1e`  
**Format:** Fixed-width TXT  
**Schema:** `cms_pprrvu_v1.1`  
**Layout:** `v2025.4.0` (from layout_registry)  
**Product Year:** 2025  
**Quarter:** Q4 (October release)  

### File Structure

- **Header rows:** Lines 1-7 (copyright notices, start with "HDR")
- **Data rows:** Lines 8-101 (94 actual data rows)
- **Columns:** 20 fixed-width columns (see layout_registry for positions)

### Expected Output

When parsed with `parse_pprrvu()`:

- **Rows:** 94 valid data rows (header rows skipped)
- **Columns:** 11 data columns + 9 metadata columns = 20 total
- **Natural Keys:** `['hcpcs', 'modifier', 'status_code', 'effective_from']`
- **Rejects:** 0 (all rows valid)

### Data Columns

1. `hcpcs` (str, categorical) - HCPCS/CPT code
2. `modifier` (str, categorical, nullable) - Procedure modifier
3. `status_code` (str, categorical) - Status code (A/B/C/I/J/T/X)
4. `work_rvu` (float64, precision=2) - Work RVU
5. `pe_rvu_nonfac` (float64, precision=2) - PE RVU non-facility
6. `pe_rvu_fac` (float64, precision=2) - PE RVU facility
7. `mp_rvu` (float64, precision=2) - Malpractice RVU
8. `global_days` (int) - Global days
9. `na_indicator` (str) - Not applicable indicator
10. `opps_cap_applicable` (str or bool) - OPPS cap applicable
11. `effective_from` (date) - Effective date

### Metadata Columns

- `release_id` (str) - Ingest release identifier
- `vintage_date` (datetime) - Data vintage date
- `product_year` (str) - Product year (e.g., "2025")
- `quarter_vintage` (str) - Quarter vintage (e.g., "2025Q4")
- `source_filename` (str) - Original filename
- `source_file_sha256` (str) - SHA-256 of source file
- `source_uri` (str) - Source URL
- `parsed_at` (datetime) - Parse timestamp (UTC)
- `row_content_hash` (str) - 64-char SHA-256 of row content

### Sample Data

First 3 data rows (truncated):

```
0001F  Heart failure composite            I   0.00   0.00     0.00    0.00
0005F  Osteoarthritis composite           I   0.00   0.00     0.00    0.00
00100  Anesth salivary gland              J   0.00   0.00     0.00    0.00
```

### Usage

```python
from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu
from pathlib import Path
import hashlib

fixture_path = Path("tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt")

# Verify fixture integrity
with open(fixture_path, 'rb') as f:
    assert hashlib.sha256(f.read()).hexdigest() == "b4437f4534b999e1764a4bbb4c13f05dc7e18e256bdbc9cd87e82a8caed05e1e"

# Parse
metadata = {
    'release_id': 'mpfs_2025_q4_test',
    'product_year': '2025',
    'quarter_vintage': '2025Q4',
    'vintage_date': datetime(2025, 10, 1),
    'file_sha256': 'b4437f4534b999e1764a4bbb4c13f05dc7e18e256bdbc9cd87e82a8caed05e1e',
    'source_uri': 'https://www.cms.gov/files/zip/rvu25d-0.zip',
    'schema_id': 'cms_pprrvu_v1.1',
    'layout_version': 'v2025.4.0'
}

with open(fixture_path, 'rb') as f:
    result = parse_pprrvu(f, fixture_path.name, metadata)

print(f"Parsed {len(result.data)} rows, {len(result.rejects)} rejects")
# Output: Parsed 94 rows, 0 rejects
```

### Validation Expectations

1. **Hash Determinism:** Parsing twice produces identical `row_content_hash` values
2. **Precision:** All RVU values rounded to 2 decimals (HALF_UP)
3. **Natural Keys:** All rows have unique `['hcpcs', 'modifier', 'status_code', 'effective_from']`
4. **Categorical:** All `status_code` values in allowed domain
5. **Metadata:** All metadata columns populated correctly
6. **Sorting:** Output sorted by natural keys (deterministic order)
7. **Performance:** Parse completes in < 100ms

### Known Characteristics

- **Status codes present:** I, J (inactive/Informational codes)
- **RVU values:** Many 0.00 for inactive codes
- **Modifiers:** Mostly empty (no modifiers for these codes)
- **Global days:** 0 for most codes
- **OPPS cap:** Varied (9XXX patterns seen)

---

**Generated:** Phase 1 PPRRVU parser implementation  
**Contract:** STD-parser-contracts v1.2 §21  
**Last updated:** 2025-10-16

