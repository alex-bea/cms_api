# Conversion Factor (ANES) Parser Planning Documents

**Last Updated:** 2025-10-17  
**Status:** ✅ Complete (Parser implemented, docs backfilled)  
**Schema:** cms_anescf_v1.0

---

## 🎯 **Quick Start**

### Parser Status
- ✅ **Parser File:** `cms_pricing/ingestion/parsers/conversion_factor_parser.py`
- ✅ **Implementation:** Complete (supports CSV, XLSX, ZIP)
- ✅ **Schema:** `cms_pricing/ingestion/contracts/cms_anescf_v1.0.json`
- ✅ **Tests:** Passing (golden + negative fixtures)

**This parser is production-ready!**

---

## 📋 **Parser Overview**

**Purpose:** Parse CMS Anesthesia Conversion Factor (ANES) files from quarterly RVU bundles

**Formats Supported:**
- ✅ CSV (primary format)
- ✅ XLSX (Excel format)
- ✅ ZIP (single-member extraction)

**Natural Keys:** `['effective_from']` (one CF per effective period)

**Expected Rows:** 1-5 rows (typically 1 per quarter)

---

## 📂 **File Guide**

| File | Purpose | Status |
|------|---------|--------|
| **README.md** | This file | ✅ Complete |
| **IMPLEMENTATION.md** | ⏳ Future (parser already complete, can backfill if needed) |

**Note:** Parser was implemented before standardized planning structure. This README provides discovera bility. Full implementation details are in the parser file itself.

---

## 🔍 **Implementation Details**

### Parser Template
Follows **STD-parser-contracts v1.6 §21.1** (9-step template):
1. Input validation
2. Format detection & routing
3. Column normalization
4. Data type casting
5. Business rule validation
6. Row-level hash computation
7. Deduplication
8. Quality metrics
9. ParseResult output

### Key Features
- ✅ Multi-format support (CSV, XLSX, ZIP)
- ✅ Header normalization & aliasing
- ✅ Decimal precision (4dp for conversion factors)
- ✅ Deterministic row hashing
- ✅ Tiered validation (BLOCK, WARN, INFO)
- ✅ ParseResult with rejects & metrics

### Helper Functions
- `_parse_zip()` - ZIP extraction
- `_detect_format()` - Format sniffing
- `_normalize_column_names()` - Header aliasing
- `_cast_dtypes()` - Type coercion with precision
- `_validate_business_rules()` - Domain validation

---

## 📊 **Schema Contract**

**Location:** `cms_pricing/ingestion/contracts/cms_anescf_v1.0.json`

### Core Columns (Participate in Hash)
- `effective_from` (date) - CF effective start date
- `conversion_factor` (decimal, 4dp) - Anesthesia conversion factor
- `geographic_adjustment` (boolean) - Whether GPCI applies
- `effective_to` (date, nullable) - CF end date

### Provenance Columns (Excluded from Hash)
- `source_file` (string) - Original filename
- `source_release` (string) - Release ID (e.g., RVU25D)
- `source_uri` (string) - Download URL
- `file_sha256` (string) - File integrity hash
- `parsed_at` (timestamp) - Parse timestamp

---

## 🧪 **Testing**

**Test Files:** `tests/ingestion/test_conversion_factor_parser_*.py`

### Test Coverage
- ✅ Golden tests (CSV, XLSX, ZIP formats)
- ✅ Determinism tests (same input → same hash)
- ✅ Negative tests (malformed inputs, validation failures)
- ✅ Schema drift detection

### Fixtures
**Location:** `tests/fixtures/conversion_factor/`
- `golden/` - Valid sample files
- `negatives/` - Invalid/edge case files

---

## 📖 **Related PRDs & Standards**

### Product Requirements
- **RVU PRD:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-rvu-gpci-prd-v0.1.md` (§1.4 ANES CF)

### Standards
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

---

## 🔧 **Implementation Resources**

- **Parser File:** `cms_pricing/ingestion/parsers/conversion_factor_parser.py`
- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_anescf_v1.0.json`
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Sample Data:** `sample_data/rvu25d_0/ANES2025.{csv,txt,xlsx}`

---

## 🎯 **Current Status**

**Phase:** ✅ Complete  
**Implementation Date:** ~October 2025  
**Version:** v1.0.0  
**Next Steps:** None (production-ready)

---

## 📝 **Notes**

This parser was implemented before the standardized planning structure was created (using GPCI as the template). It follows all parser contracts but doesn't have separate planning docs like IMPLEMENTATION.md or PRE_IMPLEMENTATION_PLAN.md.

**To reference implementation:**
1. Read the parser file directly (`conversion_factor_parser.py`)
2. Review tests (`test_conversion_factor_parser_*.py`)
3. Check schema contract (`cms_anescf_v1.0.json`)

**Need detailed planning docs?** Use GPCI structure as reference:
```bash
ls -1 planning/parsers/gpci/
```

---

**Questions?** See:
- GPCI parser planning: `planning/parsers/gpci/` (reference structure)
- Parser code: `cms_pricing/ingestion/parsers/conversion_factor_parser.py`
- Parser standards: `prds/STD-parser-contracts-prd-v1.0.md`

