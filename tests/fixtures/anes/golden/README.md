# ANES Golden Test Fixtures

**Purpose:** Clean, valid ANES data for happy-path testing

## Files

- `ANES2025_sample.txt` - Fixed-width TXT format (20 rows)
- `ANES2025_sample.csv` - CSV format (20 rows + header)

## Data Source

Extracted from `sample_data/rvu25d_0/ANES2025.txt` (first 20 rows)

## Format Details

### TXT (Fixed-Width)
- Layout: ANES_2025D_LAYOUT v2025.4.0
- Columns: MAC (0-5), Locality (12-14), Name (17-57), CF (70-74)
- Values: Integer cents (1931, 2786, etc.)

### CSV
- Header: "Contractor, Locality, Locality Name, National Anes CF..."
- Values: USD decimals (19.31, 27.86, etc.) - already scaled
- Note: CSV from CMS is pre-scaled, TXT is raw cents

## Expected Parsing Result

- **Rows:** 20 valid
- **Rejects:** 0
- **Natural Key:** `['mac', 'locality_code', 'effective_from']`
- **Scaling:** TXT: 1931 cents → $19.31 USD
- **Effective Dates:** 
  - `effective_from`: 2025-01-01 (from filename)
  - `effective_to`: 2025-12-31 or None

## Test Coverage

Per STD-qa-testing-prd-v1.0 §5.1.1: Golden fixtures must be clean, realistic data.

- Format support: TXT, CSV, XLSX (convert from CSV), ZIP (compress TXT)
- Units scaling: Cents → USD
- Date derivation: Filename → effective dates
- Natural key uniqueness: No duplicates
- Row hashing: Deterministic SHA-256

