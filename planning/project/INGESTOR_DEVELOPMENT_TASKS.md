# Ingestor Development Tasks & Priority Matrix

## 🎯 **Current Status Overview**

### ✅ **Completed Ingestors (3/12)**
- **RVU Ingestor** - Fully DIS-compliant, tested, production-ready
- **CMS ZIP9 Ingester** - Fully DIS-compliant, tested, production-ready  
- **CMS ZIP Locality Production Ingester** - Production-ready with observability

### ❌ **Missing Ingestors (9/12)**
- **MPFS Ingester** - Referenced but doesn't exist (broken imports)
- **OPPS Ingester** - Referenced but doesn't exist (broken imports)
- **ASC Ingester** - Not implemented
- **IPPS Ingester** - Not implemented
- **CLFS Ingester** - Not implemented
- **DMEPOS Ingester** - Not implemented
- **ASP Ingester** - Not implemented
- **NADAC Ingester** - Not implemented
- **Reference Data Ingestors** - Not implemented

---

## 📋 **Priority Matrix & Implementation Tasks**

### 🔴 **Phase 1: Critical Core CMS Ingestors (2-3 weeks)**

#### **Task 1: MPFS Ingester** 
- **Priority**: 🔴 Critical
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Missing (broken imports across codebase)

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/309, https://github.com/alex-bea/cms_api/issues/310, https://github.com/alex-bea/cms_api/issues/311, https://github.com/alex-bea/cms_api/issues/312, https://github.com/alex-bea/cms_api/issues/313, https://github.com/alex-bea/cms_api/issues/314, https://github.com/alex-bea/cms_api/issues/315, https://github.com/alex-bea/cms_api/issues/316, https://github.com/alex-bea/cms_api/issues/317, https://github.com/alex-bea/cms_api/issues/318, https://github.com/alex-bea/cms_api/issues/319, https://github.com/alex-bea/cms_api/issues/320, https://github.com/alex-bea/cms_api/issues/321, https://github.com/alex-bea/cms_api/issues/322, https://github.com/alex-bea/cms_api/issues/323, https://github.com/alex-bea/cms_api/issues/324, https://github.com/alex-bea/cms_api/issues/325, https://github.com/alex-bea/cms_api/issues/326, https://github.com/alex-bea/cms_api/issues/327, https://github.com/alex-bea/cms_api/issues/328, https://github.com/alex-bea/cms_api/issues/329, https://github.com/alex-bea/cms_api/issues/330, https://github.com/alex-bea/cms_api/issues/331, https://github.com/alex-bea/cms_api/issues/332, https://github.com/alex-bea/cms_api/issues/333, https://github.com/alex-bea/cms_api/issues/334, https://github.com/alex-bea/cms_api/issues/335

**Files to Create/Modify:**
- `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` (new)
- `cms_pricing/ingestion/contracts/cms_mpfs_v1.0.json` (new)
- `tests/integration/test_mpfs_ingestor_e2e.py` (new)
- `tests/fixtures/mpfs/` (new test data)
- Fix imports in: `examples/ingestion_example.py`, `scripts/ingest_all.py`, `cms_pricing/cli.py`, `cms_pricing/worker.py`

**DIS Compliance Checklist:**
- [ ] Land stage: Download and store raw files with manifest
- [ ] Validate stage: Structural, domain, and statistical validation
- [ ] Normalize stage: Canonicalize column names and data types
- [ ] Enrich stage: Join with reference data (GPCI, localities)
- [ ] Publish stage: Store in curated format with metadata
- [ ] Schema Registry integration
- [ ] Quarantine zone for rejects
- [ ] Reference data integration
- [ ] Idempotent upserts
- [ ] Schema drift detection

---

#### **Task 2: OPPS Ingester**
- **Priority**: 🔴 Critical  
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Missing (broken imports across codebase)

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/336, https://github.com/alex-bea/cms_api/issues/337, https://github.com/alex-bea/cms_api/issues/338, https://github.com/alex-bea/cms_api/issues/339, https://github.com/alex-bea/cms_api/issues/340, https://github.com/alex-bea/cms_api/issues/341, https://github.com/alex-bea/cms_api/issues/342, https://github.com/alex-bea/cms_api/issues/343, https://github.com/alex-bea/cms_api/issues/344

**Files to Create/Modify:**
- `cms_pricing/ingestion/ingestors/opps_ingestor.py` (new)
- `cms_pricing/ingestion/contracts/cms_opps_v1.0.json` (new)
- `tests/integration/test_opps_ingestor_e2e.py` (new)
- `tests/fixtures/opps/` (new test data)
- Fix imports in: `examples/ingestion_example.py`, `scripts/ingest_all.py`, `cms_pricing/cli.py`, `cms_pricing/worker.py`

---

#### **Task 3: ASC Ingester**
- **Priority**: 🟡 High
- **Complexity**: Low
- **Dependencies**: None
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/345, https://github.com/alex-bea/cms_api/issues/346, https://github.com/alex-bea/cms_api/issues/347, https://github.com/alex-bea/cms_api/issues/348, https://github.com/alex-bea/cms_api/issues/349, https://github.com/alex-bea/cms_api/issues/350, https://github.com/alex-bea/cms_api/issues/351, https://github.com/alex-bea/cms_api/issues/352

**Files to Create/Modify:**
- `cms_pricing/ingestion/ingestors/asc_ingestor.py` (new)
- `cms_pricing/ingestion/contracts/cms_asc_v1.0.json` (new)
- `tests/integration/test_asc_ingestor_e2e.py` (new)
- `tests/fixtures/asc/` (new test data)

---

### 🟡 **Phase 2: Supporting CMS Ingestors (2-3 weeks)**

#### **Task 4: CLFS Ingester**
- **Priority**: 🟡 High
- **Complexity**: Low
- **Dependencies**: None
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/353, https://github.com/alex-bea/cms_api/issues/354, https://github.com/alex-bea/cms_api/issues/355, https://github.com/alex-bea/cms_api/issues/356, https://github.com/alex-bea/cms_api/issues/357, https://github.com/alex-bea/cms_api/issues/358, https://github.com/alex-bea/cms_api/issues/359, https://github.com/alex-bea/cms_api/issues/360

---

#### **Task 5: DMEPOS Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: Geography data
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/361, https://github.com/alex-bea/cms_api/issues/362, https://github.com/alex-bea/cms_api/issues/363, https://github.com/alex-bea/cms_api/issues/364, https://github.com/alex-bea/cms_api/issues/365, https://github.com/alex-bea/cms_api/issues/366, https://github.com/alex-bea/cms_api/issues/367, https://github.com/alex-bea/cms_api/issues/368, https://github.com/alex-bea/cms_api/issues/369

---

#### **Task 6: ASP Ingester**
- **Priority**: 🟡 High
- **Complexity**: Low
- **Dependencies**: None
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/370, https://github.com/alex-bea/cms_api/issues/371, https://github.com/alex-bea/cms_api/issues/372, https://github.com/alex-bea/cms_api/issues/373, https://github.com/alex-bea/cms_api/issues/374, https://github.com/alex-bea/cms_api/issues/375, https://github.com/alex-bea/cms_api/issues/376, https://github.com/alex-bea/cms_api/issues/377

---

#### **Task 7: IPPS Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/378, https://github.com/alex-bea/cms_api/issues/379, https://github.com/alex-bea/cms_api/issues/380, https://github.com/alex-bea/cms_api/issues/381, https://github.com/alex-bea/cms_api/issues/382, https://github.com/alex-bea/cms_api/issues/383, https://github.com/alex-bea/cms_api/issues/384, https://github.com/alex-bea/cms_api/issues/385

---

### 🟢 **Phase 3: Reference Data Ingestors (1-2 weeks)**

#### **Task 8: Census Geography Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/386, https://github.com/alex-bea/cms_api/issues/387, https://github.com/alex-bea/cms_api/issues/388, https://github.com/alex-bea/cms_api/issues/389, https://github.com/alex-bea/cms_api/issues/390, https://github.com/alex-bea/cms_api/issues/391, https://github.com/alex-bea/cms_api/issues/392, https://github.com/alex-bea/cms_api/issues/393

---

#### **Task 9: HRSA Ingester**
- **Priority**: 🟢 Medium
- **Complexity**: Low
- **Dependencies**: Geography data
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/394, https://github.com/alex-bea/cms_api/issues/395, https://github.com/alex-bea/cms_api/issues/396, https://github.com/alex-bea/cms_api/issues/397, https://github.com/alex-bea/cms_api/issues/398, https://github.com/alex-bea/cms_api/issues/399, https://github.com/alex-bea/cms_api/issues/400, https://github.com/alex-bea/cms_api/issues/401, https://github.com/alex-bea/cms_api/issues/402

---

#### **Task 10: NBER Distance Ingester**
- **Priority**: 🟢 Medium
- **Complexity**: Low
- **Dependencies**: Geography data
- **Estimated Time**: 1-2 days
- **Status**: ❌ Not implemented

**Requirements:**
> **Tasks now tracked in Project:** https://github.com/alex-bea/cms_api/issues/403, https://github.com/alex-bea/cms_api/issues/404, https://github.com/alex-bea/cms_api/issues/405, https://github.com/alex-bea/cms_api/issues/406, https://github.com/alex-bea/cms_api/issues/407, https://github.com/alex-bea/cms_api/issues/408, https://github.com/alex-bea/cms_api/issues/409, https://github.com/alex-bea/cms_api/issues/410, https://github.com/alex-bea/cms_api/issues/411

---

#### **Task 11: NADAC Ingester**
- **Priority**: 🟢 Medium
- **Complexity**: Low
- **Dependencies**: None
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/nadac_ingestor.py`
- [ ] Implement DIS-compliant NADAC ingester
- [ ] Handle National Average Drug Acquisition Cost data
- [ ] Support weekly releases
- [ ] Add schema contracts for NADAC data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

---

## 🏗️ **Implementation Standards**

### **DIS Compliance Requirements**
Each ingester must implement:

1. **Land Stage**: Download and store raw files with manifest
2. **Validate Stage**: Structural, domain, and statistical validation
3. **Normalize Stage**: Canonicalize column names and data types
4. **Enrich Stage**: Join with reference data
5. **Publish Stage**: Store in curated format with metadata

### **Technical Requirements**
- [ ] Schema Registry integration
- [ ] Quarantine zone for rejects
- [ ] Reference data integration
- [ ] Idempotent upserts
- [ ] Schema drift detection
- [ ] 5-pillar observability (Freshness, Volume, Schema, Quality, Lineage)
- [ ] Comprehensive test suite following QTS standards
- [ ] API integration following Global API Program standards

### **File Structure**
```
cms_pricing/ingestion/ingestors/
├── mpfs_ingestor.py          # Task 1
├── opps_ingestor.py          # Task 2
├── asc_ingestor.py           # Task 3
├── clfs_ingestor.py          # Task 4
├── dmepos_ingestor.py        # Task 5
├── asp_ingestor.py           # Task 6
├── ipps_ingestor.py          # Task 7
├── census_geography_ingestor.py  # Task 8
├── hrsa_ingestor.py          # Task 9
├── nber_distance_ingestor.py # Task 10
└── nadac_ingestor.py         # Task 11
```

### **Test Structure**
```
tests/integration/
├── test_mpfs_ingestor_e2e.py
├── test_opps_ingestor_e2e.py
├── test_asc_ingestor_e2e.py
├── test_clfs_ingestor_e2e.py
├── test_dmepos_ingestor_e2e.py
├── test_asp_ingestor_e2e.py
├── test_ipps_ingestor_e2e.py
├── test_census_geography_ingestor_e2e.py
├── test_hrsa_ingestor_e2e.py
├── test_nber_distance_ingestor_e2e.py
└── test_nadac_ingestor_e2e.py
```

---

## 🎯 **Next Steps**

1. **Start with Task 1: MPFS Ingester** - Most critical and has broken imports
2. **Follow DIS standards** - Use RVU ingester as template
3. **Implement comprehensive testing** - Follow QTS standards
4. **Update documentation** - Fix misleading status claims
5. **Fix broken imports** - Update all references across codebase

---

## 📊 **Progress Tracking**

| Task | Status | Priority | Est. Time | Dependencies | Assigned |
|------|--------|----------|-----------|--------------|----------|
| 1. MPFS Ingester | ❌ Not Started | 🔴 Critical | 3-4 days | None | - |
| 2. OPPS Ingester | ❌ Not Started | 🔴 Critical | 3-4 days | None | - |
| 3. ASC Ingester | ❌ Not Started | 🟡 High | 2-3 days | None | - |
| 4. CLFS Ingester | ❌ Not Started | 🟡 High | 2-3 days | None | - |
| 5. DMEPOS Ingester | ❌ Not Started | 🟡 High | 3-4 days | Geography | - |
| 6. ASP Ingester | ❌ Not Started | 🟡 High | 2-3 days | None | - |
| 7. IPPS Ingester | ❌ Not Started | 🟡 High | 3-4 days | None | - |
| 8. Census Geography | ❌ Not Started | 🟡 High | 3-4 days | None | - |
| 9. HRSA Ingester | ❌ Not Started | 🟢 Medium | 2-3 days | Geography | - |
| 10. NBER Distance | ❌ Not Started | 🟢 Medium | 1-2 days | Geography | - |
| 11. NADAC Ingester | ❌ Not Started | 🟢 Medium | 2-3 days | None | - |

**Total Estimated Time**: 4-6 weeks
**Current Completion**: 3/12 ingestors (25%)
