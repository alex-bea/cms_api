# Locality Crosswalk Parser Planning Documents

**Last Updated:** 2025-10-17  
**Status:** ⏳ Planned (Not yet started)  
**Schema:** cms_localitycounty_v1.0

---

## 🎯 **Quick Start**

### Parser Status
- ⏳ **Parser File:** `cms_pricing/ingestion/parsers/locality_parser.py` (planned)
- ⏳ **Implementation:** Not started
- ✅ **Schema:** `cms_pricing/ingestion/contracts/cms_localitycounty_v1.0.json` (exists)
- ⏳ **Tests:** Not created

**This parser is planned for implementation after OPPSCAP.**

---

## 📋 **Parser Overview**

**Purpose:** Parse CMS Locality-to-County crosswalk files from quarterly RVU bundles

**Formats Expected:**
- Fixed-width TXT (primary)
- CSV (alternate)
- XLSX (alternate)

**Natural Keys:** TBD (likely `['mac', 'state_fips', 'county_fips', 'locality_code']`)

**Expected Rows:** ~3,000-5,000 rows (counties × localities)

**Note:** This is a geography/reference data parser, critical for ZIP→Locality resolution.

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
- **RVU PRD:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-rvu-gpci-prd-v0.1.md` (§1.5 Locality Crosswalk)
- **Geography Mapping:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-geography-source-map-prd-v1.0.md`

### Standards
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

---

## 🔧 **Implementation Resources**

- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_localitycounty_v1.0.json`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` (may need LOCCO layout)
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Reference Parsers:** `pprrvu_parser.py` (fixed-width), `conversion_factor_parser.py` (CSV/XLSX)
- **Sample Data:** `sample_data/rvu25d_0/25LOCCO.{csv,txt,xlsx}`

---

## 🎯 **Current Status**

**Phase:** Planning  
**Priority:** P1 (High - after OPPSCAP complete)  
**Next Action:** Create implementation plan after OPPSCAP parser complete  
**Estimated Time:** TBD

---

## 📝 **Implementation Notes**

**When starting this parser:**
1. Review GPCI planning structure as reference: `planning/parsers/gpci/`
2. Copy template from `planning/parsers/_template/README.md`
3. Examine sample data: `sample_data/rvu25d_0/25LOCCO.txt`
4. Check if layout exists in `layout_registry.py` (may need to add)
5. Coordinate with geography team for ZIP→Locality integration
6. Create full planning docs before coding

**Special Considerations:**
- This parser feeds into geography resolution (ZIP→Locality)
- Data may be relatively stable (annual updates)
- Critical for pricing accuracy (locality-specific adjustments)

---

**Questions?** See:
- GPCI parser (reference): `planning/parsers/gpci/`
- Parser template: `planning/parsers/_template/`
- Geography PRD: `prds/REF-geography-source-map-prd-v1.0.md`
- Parser standards: `prds/STD-parser-contracts-prd-v1.0.md`

