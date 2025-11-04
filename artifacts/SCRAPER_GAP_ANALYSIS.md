# Scraper Gap Analysis - Missing Scrapers

**Date:** 2025-10-28  
**Analysis:** Identifying missing scrapers for CMS data sources

## Existing Scrapers ✅

| Scraper | Status | Coverage |
|---------|--------|----------|
| `cms_rvu_scraper.py` | ✅ Complete | PPRRVU, GPCI, ANES, OPPSCap, LocalityCounty |
| `cms_opps_scraper.py` | ✅ Complete | OPPS Addendum A/B, APC rates, HCPCS mapping |
| `cms_mpfs_scraper.py` | ⚠️ **DEPRECATED** (2025-01-15) | MPFS now uses snapshot reuse + `ConversionFactorFetcher`; scraper to be removed |

## Missing Scrapers ❌

Based on pricing engines and models, the following scrapers are **NOT IMPLEMENTED**:

### 1. ASC (Ambulatory Surgical Center) ⚠️ HIGH PRIORITY

**What:** Ambulatory Surgical Center payment rates  
**Why Needed:** ASC engine exists (`asc.py`), model exists (`FeeASC`), but no scraper  
**CMS Source:** https://www.cms.gov/medicare/payment/asc

**Files to Scrape:**
- ASC payment rates by CPT code
- ASC conversion factors
- Wage index adjustments

**Impact:** ⚠️ **HIGH** - ASC pricing engine requires data but cannot get it

---

### 2. CLFS (Clinical Laboratory Fee Schedule) ⚠️ HIGH PRIORITY

**What:** Clinical lab fee schedule rates  
**Why Needed:** CLFS engine exists (`clfs.py`), model exists (`FeeCLFS`), but no scraper  
**CMS Source:** https://www.cms.gov/medicare/payment/clinical-lab-fee-schedule

**Files to Scrape:**
- HCPCS codes for lab tests
- Payment rates by test code
- Geographic adjustments

**Impact:** ⚠️ **HIGH** - CLFS pricing engine requires data but cannot get it

---

### 3. DMEPOS (Durable Medical Equipment, Prosthetics, Orthotics, and Supplies) ⚠️ MEDIUM PRIORITY

**What:** DMEPOS fee schedule rates  
**Why Needed:** DMEPOS engine exists (`dmepos.py`), model exists (`FeeDMEPOS`), but no scraper  
**CMS Source:** https://www.cms.gov/medicare/payment/dmepos

**Files to Scrape:**
- HCPCS codes for DME items
- Payment rates by item code
- ZIP code-based pricing adjustments

**Impact:** 🟡 **MEDIUM** - Less commonly used than ASC/CLFS

---

### 4. IPPS (Inpatient Prospective Payment System) ⚠️ MEDIUM PRIORITY

**What:** Inpatient hospital payment rates  
**Why Needed:** IPPS engine exists (`ipps.py`), models exist (`FeeIPPS`, `IPPSBaseRate`, `WageIndex`), but no scraper  
**CMS Source:** https://www.cms.gov/medicare/payment/acuteinpatientpps

**Files to Scrape:**
- DRG (Diagnosis Related Group) payment rates
- Base payment rates
- Wage index adjustments
- Area wage index files

**Impact:** 🟡 **MEDIUM** - Critical for inpatient pricing

---

### 5. Drugs ⚠️ MEDIUM PRIORITY

**What:** Drug pricing data (ASP, NADAC)  
**Why Needed:** Drug engine exists (`drugs.py`), models exist (`DrugASP`, `DrugNADAC`, `NDCHCPCSXwalk`), but no scraper  
**CMS Sources:** 
- ASP: https://www.cms.gov/medicare/payment/fee-for-service-provider-payment/pharmacy-pricing
- NADAC: https://www.medicaid.gov/medicaid/prescription-drugs/pharmacy-pricing/index.html

**Files to Scrape:**
- ASP (Average Sales Price) rates
- NADAC (National Average Drug Acquisition Cost)
- NDC to HCPCS crosswalks

**Impact:** 🟡 **MEDIUM** - Important for drug pricing accuracy

---

## Priority Ranking

### 🔴 CRITICAL (Build First)
**None** - Core systems have scrapers

### 🟠 HIGH (Build Soon)
1. **ASC Scraper** - Ambulatory Surgical Center data critical for outpatient procedures
2. **CLFS Scraper** - Clinical lab data critical for lab test pricing

### 🟡 MEDIUM (Build Eventually)
3. **DMEPOS Scraper** - DME pricing data
4. **IPPS Scraper** - Inpatient pricing data
5. **Drugs Scraper** - Drug pricing data (ASP, NADAC)

### 🟢 LOW (Consider Later)
**None identified**

---

## Recommendation: ASC Scraper Next ⭐

**Why ASC First:**
- High usage for outpatient procedures
- Engine already implemented
- Database model exists
- Similar complexity to existing scrapers

**ASC Scraper Implementation Plan:**

1. **Research ASC Data Structure**
   - Identify CMS URLs for ASC data
   - Document file formats and layouts
   - Map to existing ASC models

2. **Build Scraper**
   - Follow pattern from existing scrapers
   - Use same discovery/download approach
   - Generate manifests like RVU/OPPS scrapers

3. **Create Ingestor** (if needed)
   - Or extend existing patterns
   - Integrate with ASC engine

4. **Testing**
   - Test with real CMS data
   - Verify pricing accuracy
   - Validate database loading

**Estimated Effort:** 1-2 days  
**Similarity to Existing:** High (can reuse RVU/OPPS patterns)

---

## Summary

**Total Missing Scrapers:** 5  
**Priority 1 (High):** 2 (ASC, CLFS)  
**Priority 2 (Medium):** 3 (DMEPOS, IPPS, Drugs)

**Next Action:** Build ASC scraper first
