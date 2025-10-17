# CMS Parser Planning Documents

**Purpose:** Centralized planning and implementation guides for all CMS data parsers  
**Last Updated:** 2025-10-17  
**Standards:** Follows STD-parser-contracts-prd-v1.0.md (v1.7)

---

## 📁 **Parser Structure**

Each parser has a standardized planning directory with the following structure:

```
planning/parsers/{parser_name}/
├── README.md                      # Index & quick start for this parser
├── IMPLEMENTATION.md              # Complete implementation guide
├── PRE_IMPLEMENTATION_PLAN.md     # Pre-work (layout verification, etc.)
├── QUICK_START.md                 # Fast reference guide
├── SCHEMA_RATIONALE.md            # Schema design decisions (if custom schema)
├── DATA_PROVENANCE.md             # Sample data verification (optional)
├── LINE_LENGTH_ANALYSIS.md        # Fixed-width layout measurements (if applicable)
└── archive/                       # Historical planning docs
```

---

## 🛰️ **How Provenance Works**

- Discovery scrapers emit structured manifests with source URLs, checksums, and metadata so we can recreate any crawl (`cms_pricing/ingestion/metadata/discovery_manifest.py`).
- Each ingestion run writes a DIS-compliant `manifest.json` and dataset digest alongside staged artifacts (`cms_pricing/ingestion/base.py`) and records its run in the `ingest_runs` ledger with the same provenance fields (`cms_pricing/models/nearest_zip.py`).
- Parsers inject provenance columns—`release_id`, `source_file_sha256`, `source_uri`, etc.—into both canonical rows and rejects via the shared parser kit (`cms_pricing/ingestion/parsers/_parser_kit.py`).
- Curated datasets retain `dataset_digest`/`dataset_id`, and snapshot services keep the manifest JSON so downstream products can pin digests (`cms_pricing/models/geography.py`, `cms_pricing/services/geography_snapshot.py`).

**Reference PRDs**
- `prds/STD-data-architecture-prd-v1.0.md` — provenance & lineage standard
- `prds/STD-parser-contracts-prd-v1.0.md` — parser IO contracts (`provenance.json`, rejects schema)
- `prds/PRD-geography-locality-mapping-prd-v1.0.md` — dataset digest & snapshot expectations

---

## 🎯 **CMS Parsers (RVU Bundle)**

Per `PRD-rvu-gpci-prd-v0.1.md`, the following parsers are required for the CMS RVU bundle:

### 1. PPRRVU Parser
**Status:** ✅ Complete  
**Location:** `pprrvu/`  
**File:** `cms_pricing/ingestion/parsers/pprrvu_parser.py`  
**Schema:** `cms_pprrvu_v1.0.json`  
**Docs:** `PARSER_DOCUMENTATION.md`, `PPRRVU_HANDOFF.md`

**Priority:** P0 (Core - Complete)

---

### 2. GPCI Parser
**Status:** 🚧 In Progress (Pre-Implementation Phase)  
**Location:** `gpci/`  
**File:** `cms_pricing/ingestion/parsers/gpci_parser.py` (planned)  
**Schema:** `cms_gpci_v1.2.json`  
**Docs:** Complete planning structure (7 docs)

**Priority:** P0 (Core - Next to implement)

---

### 3. Conversion Factor Parser
**Status:** ✅ Complete  
**Location:** `conversion_factor/`  
**File:** `cms_pricing/ingestion/parsers/conversion_factor_parser.py`  
**Schema:** `cms_anescf_v1.0.json`  
**Docs:** ⚠️ Missing (only empty directory)

**Priority:** P0 (Core - Complete but needs docs)

---

### 4. OPPSCAP Parser
**Status:** ⏳ Planned  
**Location:** `oppscap/` (to be created)  
**File:** (planned)  
**Schema:** `cms_oppscap_v1.0.json`  
**Docs:** Not started

**Priority:** P1 (High - After GPCI)

---

### 5. Locality Crosswalk Parser
**Status:** ⏳ Planned  
**Location:** `locality/` (to be created)  
**File:** (planned)  
**Schema:** `cms_localitycounty_v1.0.json`  
**Docs:** Not started

**Priority:** P1 (High - After OPPSCAP)

---

## 📋 **Standard Planning Template**

### Required Files (Minimum)

| File | Purpose | When to Create |
|------|---------|---------------|
| `README.md` | Index, status, quick links | Always (first) |
| `IMPLEMENTATION.md` | Full implementation guide | Before coding |
| `PRE_IMPLEMENTATION_PLAN.md` | Setup & verification steps | If complex setup needed |

### Optional Files (As Needed)

| File | Purpose | When to Create |
|------|---------|---------------|
| `QUICK_START.md` | Fast reference | For complex parsers |
| `SCHEMA_RATIONALE.md` | Schema design decisions | If schema v1.0+ or breaking change |
| `DATA_PROVENANCE.md` | Sample data verification | If using external sample files |
| `LINE_LENGTH_ANALYSIS.md` | Fixed-width measurements | Fixed-width parsers only |

---

## 🔧 **Creating a New Parser Plan**

### Step 1: Create Directory
```bash
mkdir -p planning/parsers/{parser_name}/archive
```

### Step 2: Copy Template Files
```bash
# From template directory (or manually create using GPCI as reference)
cp planning/parsers/_template/* planning/parsers/{parser_name}/
```

### Step 3: Update README.md
Fill in parser-specific details:
- Parser name and purpose
- Schema contract path
- Sample data location
- Related PRD references
- Implementation status

### Step 4: Create Implementation Plan
Follow GPCI structure as reference:
- Prerequisites
- Schema overview
- 11-step parser template (STD-parser-contracts §21.1)
- Helper functions
- Testing strategy
- Time estimates

---

## 🎯 **Documentation Standards**

### Headers (Every Doc)
```markdown
# {Parser Name} Parser - {Doc Type}

**Last Updated:** YYYY-MM-DD
**Status:** ✅ Ready / 🚧 In Progress / ⏳ Planned / ✅ Complete
**Schema:** {schema_id}
```

### Cross-References (In README)
```markdown
### Related PRDs & Standards
- **Product Requirements:** prds/PRD-{name}
- **Parser Standards:** prds/STD-parser-contracts-prd-v1.0.md
- **Master Catalog:** prds/DOC-master-catalog-prd-v1.0.md

### Implementation Resources
- **Schema Contract:** cms_pricing/ingestion/contracts/{schema}.json
- **Layout Registry:** cms_pricing/ingestion/parsers/layout_registry.py
- **Sample Data:** sample_data/{path}
```

### Bidirectional Links
- Planning docs → PRDs (in README "Related PRDs")
- PRDs → Planning docs (in "Implementation Resources")

---

## ✅ **Standardization Checklist**

For each parser, ensure:

- [ ] `planning/parsers/{name}/` directory exists
- [ ] `README.md` with status, links, and structure
- [ ] Links to relevant PRD in README
- [ ] PRD has backwards link to planning docs
- [ ] `IMPLEMENTATION.md` if parser is being implemented
- [ ] Archive for historical docs (if any)
- [ ] Sample data provenance documented (if applicable)
- [ ] Schema rationale if custom schema (v1.0+)

---

## 📊 **Current Status Summary**

| Parser | Status | Docs Status | Planning Dir | Parser File |
|--------|--------|-------------|--------------|-------------|
| PPRRVU | ✅ Complete | ⚠️ Partial | ✅ Exists | ✅ Exists |
| GPCI | 🚧 In Progress | ✅ Complete | ✅ Exists | ⏳ Planned |
| Conversion Factor | ✅ Complete | ❌ Missing | ⚠️ Empty | ✅ Exists |
| OPPSCAP | ⏳ Planned | ❌ None | ❌ Missing | ⏳ Planned |
| Locality | ⏳ Planned | ❌ None | ❌ Missing | ⏳ Planned |

---

## 🚀 **Next Actions**

### Immediate (Before GPCI Implementation)
1. ✅ Standardize GPCI structure (DONE)
2. ⏳ Backfill Conversion Factor README
3. ⏳ Standardize PPRRVU structure

### Short-Term (After GPCI Complete)
4. Create OPPSCAP planning directory with stub README
5. Create Locality planning directory with stub README

### Long-Term
6. Create parser planning template directory (`_template/`)
7. Add validation script to check all parsers follow structure

---

## 📚 **References**

- **Parser Standards:** `prds/STD-parser-contracts-prd-v1.0.md` (v1.7)
- **RVU PRD:** `prds/PRD-rvu-gpci-prd-v0.1.md`
- **Master Catalog:** `prds/DOC-master-catalog-prd-v1.0.md`
- **Environment Setup:** `HOW_TO_RUN_LOCALLY.md` (root)

---

**Questions or need to create a new parser plan?** Use GPCI as the reference structure:
```bash
ls -1 planning/parsers/gpci/
```
