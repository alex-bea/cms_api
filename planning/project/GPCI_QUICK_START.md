# GPCI Parser - Quick Start Guide

**Last Updated:** 2025-10-16  
**Time to Implement:** 2.5 hours  
**Status:** ✅ Ready to code

---

## 🎯 **TL;DR**

Implement GPCI parser using:
- ✅ Schema: `cms_gpci_v1.2.json` (CMS-native naming)
- ✅ Template: STD-parser-contracts v1.7 §21.1
- ✅ References: PPRRVU + CF parsers
- ✅ Plan: `GPCI_PARSER_PLAN_V2.md`

**Key improvements from CMS expert:**
- `gpci_mp` (not `gpci_malp`) - CMS terminology
- 100-120 rows expected (~109 localities)
- Payment spot-check integration test
- Floors → pricing logic (not parser)

---

## 🚀 **Implementation Sequence**

### 1️⃣ Fix Layout (20 min)
```bash
# Edit: cms_pricing/ingestion/parsers/layout_registry.py
# Line 75: GPCI_2025D_LAYOUT

# Changes:
locality_id → locality_code
work_gpci → gpci_work
pe_gpci → gpci_pe
mp_gpci → gpci_mp
state → state (keep, mark as enrichment)
version: v2025.4.0 → v2025.4.1
min_line_length: 150 → 100 (measure from actual data)
```

### 2️⃣ Create Parser (75 min)
```bash
# Copy template from conversion_factor_parser.py
# File: cms_pricing/ingestion/parsers/gpci_parser.py

# Key constants:
PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_gpci_v1.2"
NATURAL_KEYS = ["locality_code", "effective_from"]

# Follow 9-step template (§21.1)
```

### 3️⃣ Write Tests (45 min)
```bash
# Golden tests: tests/ingestion/test_gpci_parser_golden.py
# - test_gpci_golden_csv()
# - test_gpci_golden_txt()
# - test_gpci_golden_xlsx()
# - test_gpci_golden_zip()
# - test_gpci_determinism()

# Negative tests: tests/ingestion/test_gpci_parser_negatives.py
# - 8 failure scenarios

# Integration: tests/integration/test_gpci_payment_spot_check.py
# - CPT 99213 payment calculation
```

### 4️⃣ Document (20 min)
```bash
# Update:
# - CHANGELOG.md (GPCI complete + schema v1.2)
# - STATUS_REPORT.md (3/6 parsers complete)
# - README in tests/fixtures/gpci/golden/
```

---

## 📋 **Critical Checklist**

**Before coding:**
- [ ] Read `GPCI_PARSER_PLAN_V2.md` (comprehensive plan)
- [ ] Review schema `cms_gpci_v1.2.json` (CMS-native)
- [ ] Verify layout alignment (locality_code, gpci_mp)
- [ ] Measure actual line length from sample data

**During coding:**
- [ ] Follow 9-step template (STD-parser-contracts §21.1)
- [ ] Use parser kit utilities (no duplication)
- [ ] WARN severity for duplicates (not BLOCK)
- [ ] Exclude enrichment from hash

**After coding:**
- [ ] 14/14 tests passing
- [ ] Golden hash determinism verified
- [ ] Payment spot-check passes (±$0.01)
- [ ] Documentation complete

---

## 🔑 **Key Differences from PPRRVU/CF**

| Aspect | PPRRVU | CF | GPCI |
|--------|--------|----|----|
| Natural keys | hcpcs, modifier, effective_from | cf_type, effective_from | **locality_code, effective_from** |
| Uniqueness | BLOCK | BLOCK | **WARN** (overlaps allowed) |
| Row count | ~19,000 | 2 rows | **100-120 rows** |
| Precision | 2 decimals | 4 decimals | **3 decimals** |
| Enrichment | None | None | **state, locality_name, mac (optional)** |

---

## 📚 **Quick References**

**Files to read:**
- `cms_pricing/ingestion/parsers/conversion_factor_parser.py` (best template)
- `cms_pricing/ingestion/contracts/cms_gpci_v1.2.json` (schema)
- `sample_data/rvu25d_0/GPCI2025.csv` (sample data)

**PRDs to check:**
- REF-cms-pricing-source-map-prd-v1.0.md §2 (GPCI source)
- PRD-rvu-gpci-prd-v0.1.md §1.2, §2.4 (requirements)

**Standards:**
- STD-parser-contracts v1.7 §21.1 (9-step template)
- STD-parser-contracts §8.5 (GPCI = WARN severity)

---

## 💡 **Pro Tips**

1. **Copy CF parser as starting point** - Closest structure to GPCI
2. **Measure before assuming** - Check actual line length, don't guess
3. **Test early, test often** - Golden test first, then implement
4. **CMS terminology matters** - Use "MP" not "MALP", "locality_code" not "locality_id"
5. **Enrichment is optional** - Parser includes if present, warehouse enriches fully

---

## 🎯 **Success Criteria**

✅ 14/14 tests passing  
✅ CMS-native naming (`gpci_mp`)  
✅ Payment spot-check within ±$0.01  
✅ 100-120 row validation  
✅ Enrichment columns optional  
✅ Provenance complete (release + inner file)  

---

**Ready to implement!** 🚀

Follow `GPCI_PARSER_PLAN_V2.md` for detailed step-by-step implementation.


