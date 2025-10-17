# GPCI Schema v1.2 Migration Rationale

**Date:** 2025-10-16  
**Migration:** `cms_gpci_v1.1` → `cms_gpci_v1.2`  
**Type:** Breaking changes (column renames, required → optional)  
**Impact:** First parser to adopt v1.2; establishes CMS-native naming pattern

---

## 📊 **What Changed**

### Breaking Changes

| v1.1 (Old) | v1.2 (New) | Reason |
|------------|------------|--------|
| `gpci_malp` | **`gpci_mp`** | CMS-native terminology (MP = Malpractice, not MALP) |
| `state_fips` (required) | **`state` (optional enrichment)** | CMS Locality Key uses state names, not FIPS codes |
| Hash includes `state_fips` | **Hash excludes enrichment** | Stability: enrichment shouldn't break hash determinism |

### Additive Changes

| Field | Type | Purpose |
|-------|------|---------|
| `source_release` | str | CMS release ID (e.g., "RVU25A", "RVU25D") for audit trail |
| `source_inner_file` | str | Inner ZIP filename (e.g., "GPCI2025.csv") for replayability |
| `locality_name` | str (optional) | Enrichment from CMS Locality Key or source file |
| `mac` | str (optional) | Medicare Administrative Contractor (5 digits) |

### Quality Threshold Updates

| Threshold | v1.1 | v1.2 | Reason |
|-----------|------|------|--------|
| Row count | Undefined | 100-120 expected, fail <90 | CMS post-CA consolidation: ~109 localities |
| GPCI bounds (warn) | [0.3, 2.0] | [0.30, 2.00] | Same (clarity) |
| GPCI bounds (fail) | Undefined | [0.20, 2.50] | Headroom for edge localities |
| Coverage | Undefined | ≥99.5% | PRD-rvu-gpci §11 requirement |

---

## 🎯 **CMS-Native Design Principles**

### 1. Terminology Alignment
**CMS uses "MP" not "MALP":**
- CMS files: "MP GPCI" column headers
- Federal Register: "malpractice (MP) component"
- Industry standard: MP (2-letter abbrev)

**Schema follows CMS:** `gpci_mp` matches source terminology.

### 2. State Identification Strategy
**Why `state` (not `state_fips`):**
- CMS Locality Key files use **state names/abbreviations**, not FIPS codes
- FIPS codes are Census/geography domain, not CMS native
- Example: CMS file has "AL" (state), we'd need to enrich FIPS "01" separately

**Parser strategy:**
- Include `state` if present in source (USPS 2-letter code)
- Enrichment layer adds `state_fips` from geography crosswalk if needed
- Keeps parser lean and CMS-aligned

### 3. Separation of Concerns
**GPCI Floors:**
- **Alaska:** 1.50 work GPCI floor (permanent, statutory)
- **Congressional:** 1.00 work GPCI floor (time-boxed, periodically extended)

**Parser responsibility:**
- ✅ Deliver **raw GPCI indices** from CMS files
- ❌ Do NOT apply floors in parser

**Pricing engine responsibility:**
- ✅ Apply statutory/congressional floors at payment calculation time
- ✅ Document which floor rules are active for given service date

**Rationale:** Parser = canonical source data; pricing = business rules.

### 4. Provenance & Auditability
**CMS RVU Bundle Structure:**
- Quarterly ZIP: `rvu25A.zip`, `rvu25D.zip`
- Inner files: `GPCI2025.csv`, `PPRRVU25_Oct.txt`, etc.
- Provenance requires BOTH levels

**Schema v1.2 captures:**
- `source_release`: "RVU25D" (outer ZIP identifier)
- `source_inner_file`: "GPCI2025.csv" (specific file within bundle)
- Benefit: Can replay "Give me the GPCI file from RVU25D release"

---

## 📈 **Validation Improvements**

### CMS-Realistic Bounds

**v1.1 (Generic):**
- GPCI range: [0.3, 2.0] (single threshold)
- Row count: Undefined

**v1.2 (CMS-Calibrated):**
- GPCI warn: [0.30, 2.00] (expected range)
- GPCI fail: [0.20, 2.50] (headroom for outliers)
- Row count: 100-120 expected, fail <90
- Rationale: Based on CMS historical data + post-CA consolidation universe (~109 localities)

### Locality Universe Tracking

**CMS Reconfiguration Events:**
- California consolidation (2017): Reduced localities significantly
- Periodic MAC boundary changes
- Locality splits/merges

**Parser robustness:**
- Allow 100-120 rows (±10% tolerance)
- Fail only if <90 rows (critical data loss)
- Don't enforce strict 1:1 locality continuity across releases

---

## 🔄 **Migration Path**

### For GPCI Parser (Immediate)
1. ✅ Use schema v1.2 from day 1
2. Update layout to v2025.4.1 with CMS-native names
3. Follow updated column order (excludes enrichment from hash)

### For Other Parsers (Future - Phase 2)
**Candidates for CMS-native naming:**
- PPRRVU: Consider `rvu_mp` instead of `rvu_malp` (schema v2.0)
- ANES: Use `cf_anes` or keep as-is (already CMS-aligned)
- OPPSCAP: Review column names against CMS terminology

**Coordination:**
- Batch schema updates across all parsers
- Single migration event (not piecemeal)
- Update column mappers for API compatibility

### For Enrichment (Future - Warehouse Layer)
**Locality enrichment pipeline:**
1. Parser outputs lean schema (locality_code, GPCI indices)
2. Warehouse layer joins CMS Locality Key:
   - Add locality_name (full name)
   - Add state (USPS 2-letter)
   - Add state_fips (from geography crosswalk if needed)
   - Add county mappings
3. Curated view: `vw_gpci_enriched` with full context

---

## 💡 **Benefits of v1.2**

**CMS Alignment:**
- ✅ Terminology matches Federal Register and CMS files
- ✅ State identification follows CMS conventions (not Census FIPS)
- ✅ "MP" abbreviation is industry standard

**Cleaner Architecture:**
- ✅ Parser schema is lean (core data only)
- ✅ Enrichment columns clearly marked as optional
- ✅ Hash excludes enrichment for stability
- ✅ Separation of concerns (parser vs enrichment vs pricing)

**Better Validation:**
- ✅ CMS-realistic bounds with headroom
- ✅ Row count expectations from real CMS data
- ✅ Coverage threshold from PRD requirements

**Auditability:**
- ✅ Full CMS provenance (release + inner file)
- ✅ Can replay specific file from specific release
- ✅ Integration test validates against CMS PFS Lookup Tool

---

## 🚨 **Deprecation Notice**

**Schema v1.1 Status:** Deprecated as of 2025-10-16

**Reason:** Superseded by v1.2 with CMS-native naming and better validation thresholds.

**Migration:** Any code using v1.1 should upgrade to v1.2 before production deployment.

---

## ✅ **Approval & Sign-Off**

**Reviewed by:** CMS domain expert (2025-10-16 feedback)

**Key improvements accepted:**
1. ✅ Locality count: 100-120 rows (CMS realistic)
2. ✅ `gpci_mp` naming (CMS-native)
3. ✅ `state` enrichment (not required `state_fips`)
4. ✅ GPCI bounds with headroom [0.20, 2.50]
5. ✅ Payment spot-check integration test
6. ✅ CMS provenance fields (source_release, source_inner_file)
7. ✅ GPCI floors → pricing logic (not parser)
8. ✅ Locality reconfiguration handling

**Status:** ✅ Ready for implementation

---

**Next:** Implement GPCI parser using schema v1.2 and updated plan.


