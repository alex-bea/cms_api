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
- [ ] Create `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`
- [ ] Implement DIS-compliant MPFS ingester following RVU ingester pattern
- [ ] Handle MPFS RVU data, conversion factors, and fee schedule data
- [ ] Support both annual and quarterly correction releases
- [ ] Add schema contracts for MPFS data validation
- [ ] Implement 5-pillar observability (Freshness, Volume, Schema, Quality, Lineage)
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards
- [ ] Fix all broken imports across codebase
- [ ] Update documentation to reflect actual status

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
- [ ] Create `cms_pricing/ingestion/ingestors/opps_ingestor.py`
- [ ] Implement DIS-compliant OPPS ingester
- [ ] Handle APC payment rates and HCPCS to APC mapping
- [ ] Support quarterly releases (Q1-Q4)
- [ ] Add schema contracts for OPPS data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards
- [ ] Fix all broken imports across codebase

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
- [ ] Create `cms_pricing/ingestion/ingestors/asc_ingestor.py`
- [ ] Implement DIS-compliant ASC ingester
- [ ] Handle Ambulatory Surgical Center fee schedule data
- [ ] Support quarterly releases
- [ ] Add schema contracts for ASC data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

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
- [ ] Create `cms_pricing/ingestion/ingestors/clfs_ingestor.py`
- [ ] Implement DIS-compliant CLFS ingester
- [ ] Handle Clinical Laboratory Fee Schedule data
- [ ] Support quarterly releases
- [ ] Add schema contracts for CLFS data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

---

#### **Task 5: DMEPOS Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: Geography data
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/dmepos_ingestor.py`
- [ ] Implement DIS-compliant DMEPOS ingester
- [ ] Handle Durable Medical Equipment pricing data
- [ ] Support quarterly releases
- [ ] Add schema contracts for DMEPOS data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards
- [ ] Integrate with geography data for rural adjustments

---

#### **Task 6: ASP Ingester**
- **Priority**: 🟡 High
- **Complexity**: Low
- **Dependencies**: None
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/asp_ingestor.py`
- [ ] Implement DIS-compliant ASP ingester
- [ ] Handle Average Sales Price data for Part B drugs
- [ ] Support quarterly releases
- [ ] Add schema contracts for ASP data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

---

#### **Task 7: IPPS Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/ipps_ingestor.py`
- [ ] Implement DIS-compliant IPPS ingester
- [ ] Handle Inpatient Prospective Payment System data
- [ ] Support annual releases (FY)
- [ ] Add schema contracts for IPPS data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

---

### 🟢 **Phase 3: Reference Data Ingestors (1-2 weeks)**

#### **Task 8: Census Geography Ingester**
- **Priority**: 🟡 High
- **Complexity**: Medium
- **Dependencies**: None
- **Estimated Time**: 3-4 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/census_geography_ingestor.py`
- [ ] Implement DIS-compliant Census geography ingester
- [ ] Handle ZCTA, Gazetteer, FIPS codes
- [ ] Support annual releases
- [ ] Add schema contracts for Census data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards

---

#### **Task 9: HRSA Ingester**
- **Priority**: 🟢 Medium
- **Complexity**: Low
- **Dependencies**: Geography data
- **Estimated Time**: 2-3 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/hrsa_ingestor.py`
- [ ] Implement DIS-compliant HRSA ingester
- [ ] Handle HPSA, MUA/P, NHSC sites data
- [ ] Support monthly releases
- [ ] Add schema contracts for HRSA data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards
- [ ] Integrate with geography data for geocoding

---

#### **Task 10: NBER Distance Ingester**
- **Priority**: 🟢 Medium
- **Complexity**: Low
- **Dependencies**: Geography data
- **Estimated Time**: 1-2 days
- **Status**: ❌ Not implemented

**Requirements:**
- [ ] Create `cms_pricing/ingestion/ingestors/nber_distance_ingestor.py`
- [ ] Implement DIS-compliant NBER distance ingester
- [ ] Handle precomputed ZIP-to-ZIP distances
- [ ] Support ad-hoc releases (static data)
- [ ] Add schema contracts for NBER data validation
- [ ] Implement 5-pillar observability
- [ ] Add quarantine workflow for rejected records
- [ ] Create comprehensive test suite following QTS standards
- [ ] Integrate with geography data for validation

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
