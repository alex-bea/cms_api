# {PARSER_NAME} Parser Planning Documents

**Last Updated:** YYYY-MM-DD  
**Status:** ⏳ Planned / 🚧 In Progress / ✅ Complete  
**Schema:** {schema_id}

---

## 🎯 **Quick Start**

### Prerequisites
- **Environment Setup:** See `/Users/alexanderbea/Cursor/cms-api/HOW_TO_RUN_LOCALLY.md`
  - Install dependencies: `pip install -r requirements.txt`
  - Or use Docker: `docker-compose up -d`

### For Implementation (Start Here)
1. **Pre-Implementation:** `PRE_IMPLEMENTATION_PLAN.md` (if exists) ⭐ **DO FIRST**
2. **Main Implementation:** `IMPLEMENTATION.md`
3. **Quick Reference:** `QUICK_START.md` (if exists)

### For Context
- **Schema Rationale:** `SCHEMA_RATIONALE.md` (if custom schema)
- **Data Provenance:** `DATA_PROVENANCE.md` (if applicable)
- **Archived Plans:** `archive/` (historical reference)

### Related PRDs & Standards
- **Product Requirements:** `/Users/alexanderbea/Cursor/cms-api/prds/PRD-{relevant-prd}.md`
- **Parser Standards:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **CMS Source Map:** `/Users/alexanderbea/Cursor/cms-api/prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Master Catalog:** `/Users/alexanderbea/Cursor/cms-api/prds/DOC-master-catalog-prd-v1.0.md`

### Implementation Resources
- **Schema Contract:** `cms_pricing/ingestion/contracts/{schema_id}.json`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` ({LAYOUT_NAME} if fixed-width)
- **Parser Kit:** `cms_pricing/ingestion/parsers/_parser_kit.py`
- **Reference Parsers:** `pprrvu_parser.py`, `conversion_factor_parser.py`
- **Sample Data:** `sample_data/{path}/{filename}`

---

## 📂 **File Guide**

| File | Size | Purpose | Status |
|------|------|---------|--------|
| **README.md** | - | This file | ✅ Ready |
| **IMPLEMENTATION.md** | - | Complete implementation guide | ⏳ Planned |
| **PRE_IMPLEMENTATION_PLAN.md** | - | Pre-work steps (optional) | ⏳ Planned |
| **QUICK_START.md** | - | Fast reference (optional) | ⏳ Planned |
| **SCHEMA_RATIONALE.md** | - | Schema decisions (if custom) | ⏳ Planned |

---

## 🚀 **Implementation Sequence**

### Phase 0: Pre-Implementation (if needed)
📄 **Plan:** `PRE_IMPLEMENTATION_PLAN.md`

**Tasks:**
1. Verify sample data
2. Measure line lengths (if fixed-width)
3. Update layout registry

### Phase 1: Implementation
📄 **Plan:** `IMPLEMENTATION.md`

**Tasks:**
1. Create parser file
2. Implement 11-step template (STD-parser-contracts §21.1)
3. Write tests

### Phase 2: Documentation
**Tasks:**
1. Update CHANGELOG
2. Create fixture README
3. Update status reports

---

## 📋 **Key Decisions**

### Parser Approach
- ✅ Format: {CSV / Fixed-width / XLSX / ZIP}
- ✅ Natural Keys: {list keys}
- ✅ Expected Row Count: {range}

### Schema Design
- ✅ Schema Version: {version}
- ✅ Core Columns: {list}
- ✅ Enrichment Columns: {list}

---

## ✅ **Readiness Checklist**

**Schema & Contracts:**
- [ ] Schema contract created
- [ ] Layout defined (if fixed-width)
- [ ] Sample data available

**Planning Documents:**
- [x] README ready (this file)
- [ ] Implementation plan complete
- [ ] Quick start guide (if needed)

**Infrastructure:**
- [ ] Parser kit utilities available
- [ ] Schema registry operational
- [ ] Reference parsers reviewed

---

## 🎯 **Current Status**

**Phase:** {Planning / Pre-Implementation / Implementation / Complete}  
**Next Action:** {describe next step}  
**Estimated Time:** {estimate}

---

**Questions?** See:
- GPCI parser as reference: `planning/parsers/gpci/`
- Parser standards: `prds/STD-parser-contracts-prd-v1.0.md`

