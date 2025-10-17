# PPRRVU Parser Planning Documents

**Last Updated:** 2025-10-17  
**Status:** ✅ Complete (Parser implemented)  
**Schema:** cms_pprrvu_v1.1

---

## 🎯 **Quick Start**

### Parser Status
- ✅ **Parser File:** `cms_pricing/ingestion/parsers/pprrvu_parser.py`
- ✅ **Implementation:** Complete (supports TXT, CSV, XLSX)
- ✅ **Schema:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.1.json`
- ✅ **Tests:** Passing (golden + negative fixtures)
- ✅ **Documentation:** `PARSER_DOCUMENTATION.md`, `PPRRVU_HANDOFF.md`

**This parser is production-ready!**

---

## 📋 **Parser Overview**

**Purpose:** Parse CMS Physician/Practitioner Relative Value Units (PPRRVU) files from quarterly RVU bundles

**Formats Supported:**
- ✅ Fixed-width TXT (primary format)
- ✅ CSV (alternate format)
- ✅ XLSX (Excel format)

**Natural Keys:** `['hcpcs', 'modifier', 'effective_from']`

**Expected Rows:** ~10,000-12,000 rows per quarter

**Special Features:**
- Fixed-width layout with precise column positions
- Multi-modifier support (array format)
- Comprehensive RVU components (work, PE, MP)
- Policy indicators (global days, bilateral, etc.)
- Natural key uniqueness validation (BLOCK severity)

---

## 📂 **File Guide**

| File | Purpose | Status |
|------|---------|--------|
| **README.md** | This file (index & quick start) | ✅ Complete |
| **PARSER_DOCUMENTATION.md** | Complete parser documentation | ✅ Complete |
| **PPRRVU_HANDOFF.md** | Handoff notes & implementation guide | ✅ Complete |
| **archive/** | Historical planning docs | 📋 Reference |

---

## 🔍 **Implementation Details**

### Parser Template
Follows **STD-parser-contracts v1.2 §21** (11-step template):
1. Input validation & preflight
2. Format detection & routing
3. Fixed-width/CSV/XLSX parsing
4. Column normalization
5. Data type casting (schema-driven precision)
6. Business rule validation
7. Natural key uniqueness check
8. Row-level hash computation (deterministic)
9. Deduplication & sorting
10. Quality metrics
11. ParseResult output

### Key Features
- ✅ **Fixed-width parsing** with layout registry integration
- ✅ **Encoding cascade:** UTF-8 → CP1252 → Latin-1
- ✅ **Schema-driven precision:** 2 decimals, HALF_UP rounding
- ✅ **Categorical validation:** Pre-cast domain checks
- ✅ **Deterministic hashing:** SHA-256, schema-defined column order
- ✅ **Metadata injection:** 9 provenance columns
- ✅ **Performance:** < 2s for 10K rows

### Helper Functions
- `_parse_fixed_width()` - Layout-based extraction
- `_parse_csv()` - Delimiter-agnostic CSV parsing
- `_parse_xlsx()` - Excel parsing with dtype=str
- `_normalize_column_names()` - Header aliasing
- `_cast_dtypes()` - Type coercion with precision
- `_validate_business_rules()` - Domain validation
- `_validate_natural_key_uniqueness()` - Duplicate detection

---

## 📊 **Schema Contract**

**Location:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.1.json`

### Core Columns (Participate in Hash)
- `hcpcs` (string, 5 chars) - HCPCS code
- `modifier` (string, nullable) - Modifier code
- `work_rvu` (decimal, 2dp) - Work RVU
- `pe_rvu_nonfac` (decimal, 2dp) - PE RVU non-facility
- `pe_rvu_fac` (decimal, 2dp) - PE RVU facility
- `mp_rvu` (decimal, 2dp) - Malpractice RVU
- `status_code` (string) - Status indicator
- `global_days` (string) - Global period
- *(many more policy indicators)*
- `effective_from` (date) - Effective start date
- `effective_to` (date, nullable) - Effective end date

### Provenance Columns (Excluded from Hash)
- `source_file` (string) - Original filename
- `source_release` (string) - Release ID (e.g., RVU25D)
- `source_uri` (string) - Download URL
- `file_sha256` (string) - File integrity hash
- `parsed_at` (timestamp) - Parse timestamp
- *(4 more metadata columns)*

---

## 🧪 **Testing**

**Test Files:** `tests/ingestion/test_pprrvu_parser_*.py`

### Test Coverage
- ✅ Golden tests (TXT, CSV, XLSX formats)
- ✅ Determinism tests (same input → same hash)
- ✅ Negative tests (malformed inputs, validation failures)
- ✅ Schema drift detection
- ✅ Layout version tests
- ✅ Performance benchmarks (< 2s for 10K rows)

### Fixtures
**Location:** `tests/fixtures/pprrvu/`
- `golden/` - Valid sample files
- `negatives/` - Invalid/edge case files

---

## 📖 **Related PRDs & Standards**

### Product Requirements
- **RVU PRD:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-rvu-gpci-prd-v0.1.md` (§1.1 PPRRVU)

### Standards
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

---

## 🔧 **Implementation Resources**

- **Parser File:** `cms_pricing/ingestion/parsers/pprrvu_parser.py`
- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.1.json`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` (PPRRVU_2025D_LAYOUT)
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Sample Data:** `sample_data/rvu25d_0/PPRRVU2025_Oct.{csv,txt,xlsx}`

---

## 🎯 **Current Status**

**Phase:** ✅ Complete & Production-Ready  
**Implementation Date:** ~October 2025  
**Version:** v1.0.0  
**Schema Version:** v1.1  
**Next Steps:** None (production-ready)

---

## 📝 **Additional Documentation**

- **Full Parser Docs:** See `PARSER_DOCUMENTATION.md` for complete API reference
- **Handoff Notes:** See `PPRRVU_HANDOFF.md` for implementation guidance
- **Historical Plans:** See `archive/` for previous planning iterations

---

**Questions?** See:
- Parser documentation: `PARSER_DOCUMENTATION.md` (this directory)
- Parser code: `cms_pricing/ingestion/parsers/pprrvu_parser.py`
- GPCI parser (reference for new parsers): `planning/parsers/gpci/`
- Parser standards: `prds/STD-parser-contracts-prd-v1.0.md`

