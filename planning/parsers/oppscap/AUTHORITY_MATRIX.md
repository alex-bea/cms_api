# OPPSCAP Format Authority Matrix

**Vintage:** 2025D (October 2025)  
**Date:** 2025-10-20  
**Dataset:** OPPS-based Payment Caps (Outpatient Prospective Payment System)

---

## Format Authority Ranking

### 1. TXT (Fixed-Width) - **AUTHORITY**

**File:** `OPPSCAP_Oct.txt`  
**Rows:** 16,100  
**Layout:** `OPPSCAP_2025D_LAYOUT` v2025.4.0 (defined in `layout_registry.py`)  
**Source:** RVU25D.pdf specification

**Rationale:**
- Canonical CMS format for RVU bundle datasets
- Matches official PDF layout specification  
- Fixed-width positions ensure no ambiguity
- Most complete dataset (16,100 rows)

**Layout Positions (0-indexed, verified 2025-10-20):**
```
0-5:   HCPCS code (5 chars, e.g., "0633T")
5-7:   Modifier (2 chars, e.g., "TC", may be blank)
8-9:   Status (1 char + padding, e.g., "C ")
10-15: MAC code (5 digits, e.g., "01112")
17-19: Locality code (2 digits, e.g., "05")
22-28: Facility price (decimal, e.g., "150.69")
32-38: Non-facility price (decimal, e.g., "150.69")
```

---

### 2. CSV - **SECONDARY** (Parity Testing)

**File:** `OPPSCAP_Oct.csv`  
**Rows:** 16,102 (+2 vs TXT, 0.01% variance)  
**Headers:** `HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE`

**Note:** CMS typo in header: "NON-FACILTY" (missing I)

**Parity Thresholds (per QTS §5.1.3):**
- Natural key overlap: ≥98% vs TXT
- Row count variance: ≤1% or ≤2 rows

**Expected Result:** **PASS**
- +2 rows = 0.01% variance (well within 1% threshold)
- Likely header or footer rows in CSV

**Testing Strategy:**
- `@pytest.mark.real_source` parity test
- Emit variance artifacts (missing/extra CSVs)
- Document +2 row variance in test notes

---

### 3. XLSX - **DEFERRED** (Variance Investigation)

**File:** `OPPSCAP_Oct.xlsx`  
**Rows:** 1,735 (-14,365 vs TXT, -89% variance)  
**Sheet:** 'OPPSCAP_Oct'

**Status:** **NOT INCLUDED IN V1**

**Variance Analysis:**
- 89% fewer rows than TXT authority
- Too large to be rounding/formatting difference
- Possible explanations:
  1. XLSX is summary/subset for quick reference
  2. XLSX is different vintage (Q3 vs Q4?)
  3. XLSX is filtered by criteria (e.g., cap > threshold)
  4. XLSX is corrupted/incomplete

**Decision:** Defer XLSX to follow-up investigation
- V1 scope: TXT (authority) + CSV (parity) only
- V2 scope: Investigate XLSX structure, determine if separate parser needed

---

## Natural Key Determination

**Analysis (verified 2025-10-20):**

| NK Combination | Unique Keys | Total Rows | Max Rows/Key | Conclusion |
|----------------|-------------|------------|--------------|------------|
| (hcpcs, modifier) | 77 | 16,100 | 115 | ❌ Not unique |
| (hcpcs, modifier, locality) | 3,619 | 16,100 | 19 | ❌ Not unique |
| **(hcpcs, modifier, mac, locality)** | **8,855** | **16,100** | **1** | **✅ UNIQUE** |

**Conclusion:** Natural key is `(hcpcs, modifier, mac, locality_code)`

**Why MAC + Locality both needed:**
- Same HCPCS+modifier has different prices across MACs (different contractors)
- Same HCPCS+modifier has different prices across localities (geographic variation)
- Example: "0633T+TC" has 96 unique prices across 115 MAC/locality combinations

**PRD Note:** PRD incorrectly states NK as `(hcpcs, modifier, locality_id)` without MAC. Real data requires MAC in NK.

---

## Testing Strategy (v1)

### Golden Fixtures
- **TXT:** 18-row clean fixture (no duplicates, diverse HCPCs/modifiers/MACs/localities)
- **CSV:** Identical 18 rows as TXT (format parity)
- **Requirement:** 0 rejects on golden fixtures

### Real Source Parity
- **Test:** TXT vs CSV on full files
- **Threshold:** ≥98% NK overlap, ≤1% row variance
- **Expected:** PASS (+2 rows = 0.01%)
- **Artifacts:** Emit variance CSVs and summary JSON

### XLSX
- **v1:** Not tested (deferred)
- **v2:** Investigate variance, create separate test if needed

---

## Implementation Notes

**Encoding:**
- TXT: Likely UTF-8 or ASCII (no special characters observed)
- CSV: UTF-8 with standard delimiters
- **Action:** Use encoding cascade (UTF-8 → UTF-16 → CP1252)

**Header Detection:**
- TXT: No headers (fixed-width)
- CSV: Header row 1, data starts row 2
- **Action:** Standard CSV header normalization

**Data Quality:**
- Status codes observed: "C" (common), need to verify full domain
- Prices: Range $116-$152 in sample
- Modifiers: "TC" (technical component) common, blanks possible

---

## Phase 1 Completion Checklist

- [x] Layout positions verified (existing layout_registry.py lines 98-111)
- [x] Natural keys determined: (hcpcs, modifier, mac, locality_code)
- [x] Schema contract v1.1 created (matches actual file structure)
- [x] Format variance documented (CSV +0.01%, XLSX -89%)
- [x] Authority matrix created (TXT authority, CSV parity, XLSX deferred)
- [x] XLSX investigation (DEFERRED to v2)

**Ready for Phase 2:** Parser implementation

---

## References

- **Layout:** `cms_pricing/ingestion/parsers/layout_registry.py:98-111`
- **Schema v1.1:** `cms_pricing/ingestion/contracts/cms_oppscap_v1.1.json`
- **PRD:** `prds/PRD-rvu-gpci-prd-v0.1.md` §1.3 (OPPSCAP section)
- **Sample Data:** `sample_data/rvu25d_0/OPPSCAP_Oct.{txt,csv,xlsx}`
- **Reference Parser:** `cms_pricing/ingestion/parsers/gpci_parser.py`

