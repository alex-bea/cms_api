# GPCI Parser Plan - CMS Expert Improvements

**Date:** 2025-10-16  
**Reviewer:** CMS Domain Expert  
**Impact:** 9 critical improvements preventing 3-5 hours debugging

---

## ✅ **Improvements Integrated**

### 1. Locality Count Correction ⭐⭐⭐
**Before:** ~115 rows ±10% (generic assumption)  
**After:** 100-120 rows (warn), fail <90 rows

**Why this matters:**
- CMS post-California consolidation: ~109 current localities
- Locality reconfigurations happen (CA, MAC boundaries)
- Tight bounds (105-125) would false-alarm on valid changes
- Wider bounds (100-120) with hard floor (<90) catches real data loss

**Source:** CMS PFS Relative Value Files documentation

---

### 2. CMS-Native Column Naming ⭐⭐⭐
**Before:** `gpci_malp` (spelled out malpractice)  
**After:** `gpci_mp` (CMS abbreviation)

**Why this matters:**
- CMS files use "MP GPCI" in headers
- Federal Register uses "malpractice (MP) component"
- Industry standard: MP (2-letter abbreviation)
- Reduces cognitive load when comparing parser output to CMS docs

**Impact:** Prevents confusion, aligns with CMS terminology

---

### 3. State Identification Strategy ⭐⭐⭐
**Before:** `state_fips` (required 2-digit FIPS code)  
**After:** `state` (optional 2-letter USPS code from source)

**Why this matters:**
- CMS Locality Key files use **state abbreviations** ("AL", "AK"), not FIPS
- FIPS codes are Census domain, not CMS-native
- Parser shouldn't guess/enrich FIPS from state names
- Warehouse layer can add FIPS via geography crosswalk if needed

**Schema design:**
- Core columns: locality_code + GPCI indices + effective_from
- Enrichment columns: state, locality_name, mac (optional, excluded from hash)

**Benefit:** Cleaner parser, stable hash, correct layer separation

---

### 4. GPCI Range Validation (Enhanced) ⭐⭐
**Before:** [0.3, 2.0] (single threshold)  
**After:** WARN [0.30, 2.00], FAIL [0.20, 2.50]

**Why this matters:**
- Historical CMS data: GPCI typically in [0.5, 1.6] range
- Edge cases: Manhattan PE=1.569 (high), rural areas ~0.7 (low)
- Headroom [0.20, 2.50] catches truly invalid data (e.g., negative, >3.0)
- WARN [0.30, 2.00] flags unusual but possibly valid values

**Validation tiers:**
1. **FAIL <0.20 or >2.50:** Critical data corruption
2. **WARN <0.30 or >2.00:** Unusual but investigate
3. **INFO [0.30, 2.00]:** Normal range

---

### 5. GPCI Floors → Pricing Logic ⭐⭐⭐
**Before:** Unclear if parser should apply floors  
**After:** Parser delivers **raw** indices; pricing applies floors

**Why this matters:**
- **Alaska floor:** Statutory 1.50 work GPCI (permanent)
- **Congressional floor:** 1.00 work GPCI (periodically extended, time-boxed)
- **Parser role:** Canonical source data from CMS
- **Pricing role:** Apply business rules (floors, caps, sequestration)

**Documentation added:**
- Schema: "Floors applied at pricing time, not parser"
- Parser docstring: "Delivers raw GPCI values from CMS"
- Business rules: Explicit floor handling guidance

**Benefit:** Correct separation of concerns, no ambiguity

---

### 6. CMS Provenance Fields ⭐⭐
**Before:** Generic `source_filename`, `source_file_sha256`  
**After:** Added `source_release`, `source_inner_file`

**Why this matters:**
- CMS bundles quarterly ZIPs: `rvu25A.zip`, `rvu25D.zip`
- Each ZIP contains multiple files: `GPCI2025.csv`, `PPRRVU25_Oct.txt`
- Audit trail needs BOTH:
  - **Outer:** Which quarterly release? (RVU25D)
  - **Inner:** Which specific file? (GPCI2025.csv)

**Use case:**
```
Reproduce GPCI data from 2025 Q4 release:
1. Fetch rvu25D.zip (source_release)
2. Extract GPCI2025.csv (source_inner_file)
3. Verify SHA-256 matches source_file_sha256
4. Parse and compare row_content_hash
```

---

### 7. Payment Spot-Check Integration Test ⭐⭐⭐
**Before:** Parser tests only  
**After:** Added integration test with CMS PFS Lookup Tool

**Test pattern:**
```python
def test_gpci_payment_spot_check():
    """
    Spot-check: CPT 99213, Alabama locality (00), 2025-01-01
    
    Per CMS PFS Lookup Tool:
    - Work RVU: 0.93
    - Alabama work GPCI: 1.000
    - Physician CF 2025: 32.3465
    - Expected payment (work): 0.93 × 1.000 × 32.3465 = $30.08
    
    Tolerance: ±$0.01 (rounding differences)
    """
```

**Why this matters:**
- **End-to-end confidence:** Proves GPCI × RVU × CF works
- **CMS ground truth:** PFS Lookup Tool is authoritative
- **Catches integration bugs:** Schema misalignment, precision loss, etc.
- **Regression prevention:** Any breaking change fails this test

**Effort:** 10 minutes, massive confidence boost

---

### 8. Locality Reconfiguration Handling ⭐⭐
**Before:** Implicit assumption of stable locality universe  
**After:** Explicit handling for CMS reconfiguration events

**Real-world events:**
- **2017:** California locality consolidation (major reduction)
- **Ongoing:** MAC boundary adjustments
- **Future:** Possible locality splits/merges

**Parser robustness:**
- Don't enforce strict row count (allow 100-120 range)
- Don't force 1:1 locality continuity in tests
- Accept identical row-hashes with new effective_from (quarterly re-publication)

**Quality gate:**
- WARN if outside 100-120 (investigate)
- FAIL if <90 (critical data loss)

---

### 9. Third-Party Source Handling ⭐
**Before:** No guidance on AMA reprints, vendor blogs  
**After:** Explicit: CMS files only, others for context

**Authoritative sources:**
- ✅ **CMS.gov:** Only authoritative source for ingestion
- ❌ **AMA reprints:** Useful for context (e.g., Alaska floor notes), NOT for parsing
- ❌ **Vendor blogs:** Educational only, may have transcription errors

**Benefit:** Clear provenance chain, no ambiguity

---

## 📊 **Impact Summary**

| Improvement | Time Saved | Confidence Boost |
|-------------|------------|------------------|
| CMS-native naming (MP not MALP) | 30 min | High |
| State vs state_fips clarity | 1 hour | High |
| Realistic row count bounds | 45 min | Medium |
| GPCI floors → pricing logic | 1 hour | High |
| Payment spot-check test | 0 min (catches bugs) | **Very High** |
| Provenance fields | 30 min | Medium |
| Reconfiguration handling | 30 min | Medium |
| **TOTAL** | **4-5 hours saved** | **Production-ready** |

---

## 🎯 **Comparison: v1.0 Plan vs v2.0 CMS-Native**

| Aspect | v1.0 (Original) | v2.0 (CMS-Native) |
|--------|-----------------|-------------------|
| Schema | v1.1 (`gpci_malp`, `state_fips`) | **v1.2** (`gpci_mp`, `state`) |
| Row count | ~115 ±10% | **100-120** (CMS realistic) |
| GPCI bounds | [0.3, 2.0] fail | **[0.30, 2.00] warn, [0.20, 2.50] fail** |
| State field | Required (FIPS) | **Optional (USPS)** enrichment |
| Floors | Unclear | **Pricing logic** (documented) |
| Provenance | Generic | **CMS-specific** (release + inner file) |
| Integration test | Parser only | **Payment spot-check** |
| Reconfiguration | Implicit | **Explicit** handling |

**Winner:** v2.0 is production-hardened with CMS domain expertise

---

## 📁 **Files Updated**

1. ✅ `cms_pricing/ingestion/contracts/cms_gpci_v1.2.json` - New schema (CMS-native)
2. ✅ `cms_pricing/ingestion/contracts/cms_gpci_v1.0.json` - Deprecated (note added)
3. ✅ `GPCI_PARSER_PLAN_V2.md` - Updated implementation plan
4. ✅ `GPCI_SCHEMA_V1.2_RATIONALE.md` - Migration rationale (this doc)

---

## 🚀 **Next Steps**

1. **Update layout registry** - v2025.4.1 with CMS-native names (20 min)
2. **Implement parser** - Follow GPCI_PARSER_PLAN_V2.md (2 hours)
3. **Write tests** - 14 tests with payment spot-check (45 min)
4. **Document** - CHANGELOG + fixture README (20 min)

**Total:** 2.5 hours for bullet-proof, CMS-aligned GPCI parser

---

**Prepared by:** Agent + CMS Domain Expert  
**Status:** ✅ Ready for implementation  
**Confidence:** Very High (builds on PPRRVU/CF learnings + CMS expertise)


