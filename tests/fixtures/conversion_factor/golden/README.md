# Conversion Factor Golden Fixtures

## Source

**CMS Federal Register - CY 2025 Physician Fee Schedule Final Rule**

Authoritative values from CMS Federal Register:
- **Physician CF:** 32.3465 USD (exact, 4 decimal places)
- **Anesthesia CF:** 20.3178 USD (national base unit, 4 decimal places)

**References:**
- [CMS Final Rule](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures)
- Federal Register CY 2025 Physician Fee Schedule

---

## Fixtures

### 1. `cf_2025_minimal.csv` - Base CSV Format

**Content:** 2 rows (physician + anesthesia national rates)

**SHA-256:** `208cb7220aa8279496052231c5974181caab18eca940fcc01f1d344522410511`

**Purpose:** Test CSV parsing, precision enforcement, categorical validation

---

### 2. `cf_2025_minimal.zip` - ZIP Archive

**Content:** ZIP wrapper containing `cf_2025_minimal.csv`

**SHA-256:** `eded1a47cf4289e8b9198d21b490bbe3a6681f2e1bd5df4aff029e2009656147`

**Purpose:** Test ZIP extraction and member routing per STD-parser-contracts v1.6 §21.1

---

### 3. `cf_2025_minimal.xlsx` - Excel Workbook

**Content:** Same 2 rows in Excel format

**SHA-256:** `252a71741dc06db9eac3b114324247ec24fc4238edd3282841366bc93e588d5a`

**Purpose:** Test Excel parsing with `dtype=str` (Anti-Pattern 8)

---

## Expected Parser Output

**Rows:** 2 (physician + anesthesia)

**Columns:**
- Data: `cf_type`, `cf_value`, `cf_description`, `effective_from`, `effective_to`
- Metadata: `release_id`, `vintage_date`, `product_year`, `quarter_vintage`, `source_filename`, `source_file_sha256`, `parsed_at`, `row_content_hash`

**Sort Order:** By natural key `["cf_type", "effective_from"]`
- Row 1: anesthesia (alphabetically first)
- Row 2: physician

**Precision:** 4 decimal places, HALF_UP rounding

**Hash:** 64-character SHA-256 row_content_hash (deterministic)

---

## Validation Rules

**BLOCK (Critical):**
- `cf_type` must be in domain: `['physician', 'anesthesia']`
- `cf_value` must be > 0
- `effective_from` required
- No duplicate natural keys

**WARN (Soft):**
- Physician CF deviation from 32.3465 for CY-2025
- Anesthesia CF deviation from 20.3178 for CY-2025

---

## Notes

- These are **synthetic fixtures** with CMS-authoritative values
- Use for golden/determinism testing, not format validation
- Real CMS CF files (ZIP/XLSX) should be added when scraper discovers them
- Mid-year AR support: multiple rows per year allowed (different `effective_from`)

---

**Created:** 2025-10-16  
**Schema:** cms_conversion_factor_v2.0  
**Parser:** conversion_factor_parser.py v1.0.0

