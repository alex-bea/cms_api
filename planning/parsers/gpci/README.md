# GPCI Parser Planning Documents

**Last Updated:** 2025-10-16  
**Status:** ✅ Ready for implementation  
**Schema:** cms_gpci_v1.2 (CMS-native)

---

## 🎯 **Quick Start**

### Prerequisites
- **Environment Setup:** See `/Users/alexanderbea/Cursor/cms-api/HOW_TO_RUN_LOCALLY.md`
  - Install dependencies: `pip install -r requirements.txt`
  - Or use Docker: `docker-compose up -d`

### For Implementation (Start Here)
1. **Pre-Implementation:** `PRE_IMPLEMENTATION_PLAN.md` (25 min) ⭐ **DO FIRST**
2. **Main Implementation:** `IMPLEMENTATION.md` (2 hours)
3. **Quick Reference:** `GPCI_QUICK_START.md`

### For Context
- **Schema Rationale:** `GPCI_SCHEMA_V1.2_RATIONALE.md`
- **Data Provenance:** `DATA_PROVENANCE.md` (sample file verification)
- **Line Measurements:** `LINE_LENGTH_ANALYSIS.md` (layout verification)
- **Archived Plans:** `archive/` (historical reference)

### Related PRDs & Standards
- **Product Requirements:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-rvu-gpci-prd-v0.1.md`
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

### Implementation Resources
- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_gpci_v1.2.json`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` (GPCI_2025D_LAYOUT v2025.4.1)
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Reference Parsers:** `pprrvu_parser.py`, `conversion_factor_parser.py`
- **Sample Data:** `sample_data/rvu25d_0/GPCI2025.txt`

---

## 📂 **File Guide**

| File | Size | Purpose | Status |
|------|------|---------|--------|
| **PRE_IMPLEMENTATION_PLAN.md** | 11K | Step-by-step pre-work (25 min) | ⏸️ In Progress |
| **IMPLEMENTATION.md** | 26K | Complete parser implementation (v2.1) | ✅ Ready |
| **GPCI_QUICK_START.md** | 4.2K | Fast reference for implementers | ✅ Ready |
| **GPCI_SCHEMA_V1.2_RATIONALE.md** | 6.6K | Schema migration ADR | 📋 Reference |
| **DATA_PROVENANCE.md** | 4.7K | Sample file verification | ✅ Complete |
| **LINE_LENGTH_ANALYSIS.md** | 2.8K | Layout measurement results | ✅ Complete |

### Archive (Historical Reference)
- `archive/plan_v1.0.md` (35K) - Original + v2.0 mixed version
- `archive/plan_v2.0.md` (18K) - CMS-native (superseded by FINAL)
- `archive/improvements.md` (7.6K) - 9 improvements analysis
- `archive/completeness.md` (7.2K) - Completeness report
- `archive/review.md` (13K) - Review feedback

---

## 🚀 **Implementation Sequence**

### Phase 0: Pre-Implementation (25 min) ⬅️ **YOU ARE HERE**
📄 **Plan:** `PRE_IMPLEMENTATION_PLAN.md`

**Tasks:**
1. Measure line length from sample data (5 min)
2. Verify column positions (10 min)
3. Update layout registry to v2025.4.1 (10 min)

**Output:**
- Updated `layout_registry.py` with CMS-native names
- Documented measurements

### Phase 1: Golden-First Development (75 min)
📄 **Plan:** `IMPLEMENTATION.md` § Golden-First Workflow

**Tasks:**
1. Extract golden fixtures (15 min)
2. Write golden tests (10 min)
3. Implement parser (50 min)

### Phase 2: Comprehensive Testing (45 min)
**Tasks:**
1. Create negative fixtures (15 min)
2. Write negative tests (20 min)
3. Write integration test (10 min)

### Phase 3: Documentation (20 min)
**Tasks:**
1. Parser docstring (10 min)
2. Fixture README (5 min)
3. Update CHANGELOG (5 min)

**Total:** 2.5 hours

---

## 📋 **Key Decisions (v1.2 Schema)**

### CMS-Native Naming
- ✅ `gpci_mp` (not `gpci_malp`) - CMS "MP" terminology
- ✅ `state` enrichment (not required `state_fips`) - CMS uses state names
- ✅ `source_release` provenance (RVU25A/B/C/D tracking)

### Validation Strategy
- ✅ 100-120 rows expected (~109 CMS localities)
- ✅ GPCI bounds: [0.30, 2.00] warn, [0.20, 2.50] fail
- ✅ WARN severity for duplicates (not BLOCK)

### Separation of Concerns
- ✅ Parser delivers **raw GPCI indices** from CMS
- ✅ Pricing logic applies **floors** (Alaska 1.50, Congressional 1.00)
- ✅ Warehouse layer performs **geography enrichment**

---

## ✅ **Readiness Checklist**

**Schema & Contracts:**
- [x] Schema v1.2 created (`cms_gpci_v1.2.json`)
- [x] Schema v1.1 deprecated with notice
- [ ] Layout v2025.4.1 updated (PRE-IMPLEMENTATION Step 3)

**Planning Documents:**
- [x] Pre-implementation plan ready
- [x] Implementation plan complete (v2.1)
- [x] Quick start guide ready
- [x] Schema rationale documented

**Infrastructure:**
- [x] Parser kit utilities available (`_parser_kit.py`)
- [x] Schema registry operational
- [x] Router infrastructure ready
- [x] Reference parsers complete (PPRRVU, CF)

**Sample Data:**
- [x] `sample_data/rvu25d_0/GPCI2025.csv` (~115 rows)
- [x] `sample_data/rvu25d_0/GPCI2025.txt` (~115 rows)
- [x] `sample_data/rvu25d_0/GPCI2025.xlsx` (~115 rows)

---

## 🎯 **Current Status**

**Phase:** Pre-Implementation (Phase 0)  
**Next Action:** Execute `PRE_IMPLEMENTATION_PLAN.md`  
**Estimated Time:** 25 minutes  
**Then:** Start parser implementation (2 hours)

---

**Questions?** See:
- Implementation details: `IMPLEMENTATION.md`
- Fast overview: `GPCI_QUICK_START.md`
- Schema decisions: `GPCI_SCHEMA_V1.2_RATIONALE.md`

