# OPPSCAP Parser Planning Documents

**Last Updated:** 2025-10-17  
**Status:** ⏳ Planned (Not yet started)  
**Schema:** cms_oppscap_v1.0

---

## 🎯 **Quick Start**

### Parser Status
- ⏳ **Parser File:** `cms_pricing/ingestion/parsers/oppscap_parser.py` (planned)
- ⏳ **Implementation:** Not started
- ✅ **Schema:** `cms_pricing/ingestion/contracts/cms_oppscap_v1.0.json` (exists)
- ⏳ **Tests:** Not created

**This parser is planned for implementation after GPCI.**

---

## 📋 **Parser Overview**

**Purpose:** Parse CMS OPPS Inpatient Cap (OPPSCAP) files from quarterly RVU bundles

**Formats Expected:**
- Fixed-width TXT (primary)
- CSV (alternate)
- XLSX (alternate)

**Natural Keys:** TBD (likely `['hcpcs', 'modifier', 'locality_code', 'effective_from']`)

**Expected Rows:** ~10,000-15,000 rows (HCPCS codes × localities)

---

## 📂 **File Guide**

| File | Purpose | Status |
|------|---------|--------|
| **README.md** | This file (stub) | ✅ Complete |
| **IMPLEMENTATION.md** | Full implementation guide | ⏳ Not created |
| **PRE_IMPLEMENTATION_PLAN.md** | Pre-work steps | ⏳ Not created |

---

## 📖 **Related PRDs & Standards**

### Product Requirements
- **RVU PRD:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-rvu-gpci-prd-v0.1.md` (§1.3 OPPSCAP)

### Standards
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

---

## 🔧 **Implementation Resources**

- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_oppscap_v1.0.json`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` (OPPSCAP_2025D_LAYOUT exists)
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Reference Parsers:** `pprrvu_parser.py` (fixed-width), `conversion_factor_parser.py` (CSV/XLSX)
- **Sample Data:** `sample_data/rvu25d_0/OPPSCAP_Oct.{csv,txt,xlsx}`

---

## 🎯 **Current Status**

**Phase:** Planning  
**Priority:** P1 (High - after GPCI complete)  
**Next Action:** Create implementation plan after GPCI parser complete  
**Estimated Time:** TBD

---

## 📝 **Implementation Notes**

**When starting this parser:**
1. Review GPCI planning structure as reference: `planning/parsers/gpci/`
2. Copy template from `planning/parsers/_template/README.md`
3. Examine sample data: `sample_data/rvu25d_0/OPPSCAP_Oct.txt`
4. Verify layout in `layout_registry.py` (OPPSCAP_2025D_LAYOUT)
5. Create full planning docs before coding

---

**Questions?** See:
- GPCI parser (reference): `planning/parsers/gpci/`
- Parser template: `planning/parsers/_template/`
- Parser standards: `prds/STD-parser-contracts-prd-v1.0.md`

