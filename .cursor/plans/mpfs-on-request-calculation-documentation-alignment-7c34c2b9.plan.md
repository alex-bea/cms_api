<!-- 7c34c2b9-cbb8-4708-8032-b2a50b767310 55a850d4-a54f-4541-aad1-6457d04ceaf4 -->
# MPFS On-Request Calculation Documentation Alignment Plan

## Objective

Ensure all documentation consistently reflects the MPFS design decision: **ingestion stores authoritative inputs only; payment calculations are performed on-request by the Pricing API at runtime**. Codify reusable patterns for future calculators.

## Context

The MPFS PRD (v1.0) has been updated to remove pre-computed payment tables. The ingestor now stores:

- `mpfs_rvu` (RVUs + indicators)
- `mpfs_gpci` (geography indices)
- `mpfs_cf_vintage` (conversion factors)
- `mpfs_indicators_all` (policy flags)

Payment computation is handled by the Pricing API using the formula:

```
Payment = CF × [(Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP)]
```

## Files Requiring Updates

### Priority 1: Standards (Foundation)

#### STD-data-architecture-prd-v1.0.md

**Location:** `prds/STD-data-architecture-prd-v1.0.md`

**Changes:**

- Add new section §3.8 "Calculator Pattern" after §3.7 (Shared Stage Modules)
- Document the pattern: ingestor stores inputs only, calculator applies formulas at runtime
- Include benefits: decouples formula changes from ingestion, enables immediate CF/GPCI updates, reduces recomputation cost
- Reference REF-pricing-calculator-prd-v1.0.md as the implementation guide
- Update §3.6 (Publish) to clarify that curated outputs are input datasets, not derived values

**Pattern Template:**

```markdown
### 3.8 Calculator Pattern (DIS Extension)

**Purpose:** Enable flexible, low-maintenance computation of derived values (prices, premiums, scores) after ingestion, based on authoritative stored inputs.

**Applies to:** All pricing or reimbursement systems that use stable reference inputs (MPFS, OPPS, DMEPOS, etc.).

**Pattern Components:**
- **Ingestor (DIS):** Validate and store raw reference inputs (RVU, GPCI, CF). No math or aggregation.
- **Calculator Engine:** Apply formulas dynamically at runtime.
- **API Layer:** Expose results via request/response, cache outputs, and tag with dataset digests.
- **Parity QA:** Run API-level comparisons vs authoritative sources (e.g., CMS Look-Up Tool).

**Benefits:**
- Simplifies ingestion pipelines
- Decouples formula changes from data ingestion cadence
- Enables immediate updates when CF or GPCI changes
- Reduces recomputation cost
```

#### STD-qa-testing-prd-v1.0.md

**Location:** `prds/STD-qa-testing-prd-v1.0.md`

**Changes:**

- Add new section §7.6 "Dual-Level Validation Model" after §7.5 (Validation Patterns)
- Document two validation layers: ingestion (DIS) and API (calculator)
- Clarify that parity testing happens at API level, not ingestion level

**Pattern Template:**

```markdown
### 7.6 Dual-Level Validation Model

**Purpose:** Separate validation concerns between ingestion (data quality) and runtime (formula accuracy).

**Layer Responsibilities:**

| Layer | Validation Focus | Example |
|-------|------------------|---------|
| Ingestion (DIS) | Schema integrity, key coverage, data lineage | Column presence, join coverage ≥99.5% |
| API (Calculator) | Formula accuracy, parity vs CMS, rounding correctness | /pricing/codes/price parity ±$0.01 |

**Testing Strategy:**
- Ingestion tests: Validate input datasets (RVU, GPCI, CF) meet schema contracts
- API tests: Validate computed prices match CMS Look-Up Tool within tolerance
- Parity tests: Run nightly against top 100 HCPCS codes per quarter
```

### Priority 2: Runbooks (Operations)

#### RUN-mpfs-ingestion-v1.0.md

**Location:** `prds/RUN-mpfs-ingestion-v1.0.md`

**Changes:**

- Remove §4.2 "Payment Sanity Check" (references `mpfs_payment_curated` which no longer exists)
- Replace with §4.2 "Input Dataset Verification" that checks RVU, GPCI, CF tables
- Update §4.3 "API Contract Check" to reference Pricing API parity testing instead of payment table verification
- Update §4.1 "Manifest & Curated Outputs" to remove `mpfs_payment_curated` from expected datasets list

**Specific Edits:**

- Line 182: Remove `mpfs_payment_curated` from expected datasets
- Lines 190-202: Replace payment sanity check with input verification
- Lines 204-213: Update API contract check to reference calculator parity testing

### Priority 3: Product PRDs (Consumers)

#### PRD-opps-prd-v1.0.md

**Location:** `prds/PRD-opps-prd-v1.0.md`

**Changes:**

- Add note in "Design Decisions" section (after line 147) stating: "OPPS payment logic will also move to on-request mode when implemented; OPPS ingestor stores inputs (APC, status indicator, OPPS rate) only."
- Update "Consumers" section to mention "Pricing Calculator uses OPPS inputs for on-request computation."

#### PRD-rvu-gpci-prd-v0.1.md

**Location:** `prds/PRD-rvu-gpci-prd-v0.1.md`

**Changes:**

- Add new subsection §2.6 "Downstream Consumers" after §2.5 (Publication)
- Document: "RVU and GPCI datasets are used by Pricing Calculator at runtime, not pre-joined during ingestion."
- Update "Consumers" field in header to include "Pricing Calculator"

**Note:** This PRD doesn't have a dedicated "Consumers" section, so we'll add it to the functional requirements area.

### Priority 4: Reference Documents (Lineage)

#### DOC-master-catalog-prd-v1.0.md

**Location:** `prds/DOC-master-catalog-prd-v1.0.md`

**Changes:**

- Update §3 "Product & Dataset PRDs" table entry for `PRD-mpfs-prd-v1.0.md`
- Add note: "Payment calculation is runtime (Pricing API), not materialized during ingestion"
- Consider adding `REF-pricing-calculator-prd-v1.0.md` to §2 "Reference Architectures" if not already present

#### REF-cms-pricing-source-map-prd-v1.0.md

**Location:** `prds/REF-cms-pricing-source-map-prd-v1.0.md`

**Changes:**

- Update §2 "Source Inventory" table row for "CMS MPFS"
- Ensure "Output" column lists only input datasets: `mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_indicators_all`, `mpfs_locality`
- Remove any references to `mpfs_payment_curated` or pre-computed payment outputs
- Update §2B "Discovery → Land → API Trace" for CMS MPFS to clarify calculator responsibility

## Implementation Order

1. **STD-data-architecture-prd-v1.0.md** - Add Calculator Pattern section (foundation for all other docs)
2. **STD-qa-testing-prd-v1.0.md** - Add Dual-Level Validation Model (establishes testing approach)
3. **RUN-mpfs-ingestion-v1.0.md** - Update runbook to remove payment table references
4. **PRD-opps-prd-v1.0.md** - Add on-request calculation note for consistency
5. **PRD-rvu-gpci-prd-v0.1.md** - Document downstream calculator usage
6. **DOC-master-catalog-prd-v1.0.md** - Update lineage and catalog entries
7. **REF-cms-pricing-source-map-prd-v1.0.md** - Ensure output columns are correct

## Verification Checklist

After updates, verify:

- [ ] No references to `mpfs_payment_curated` table in runbooks or source maps
- [ ] Calculator Pattern documented in STD-data-architecture-prd-v1.0.md
- [ ] Dual-Level Validation documented in STD-qa-testing-prd-v1.0.md
- [ ] All PRDs mention on-request calculation where applicable
- [ ] Source map outputs list only input datasets
- [ ] Master catalog reflects runtime calculation for MPFS
- [ ] Cross-references to REF-pricing-calculator-prd-v1.0.md are present

## Cross-Reference Validation

Ensure all documents that reference MPFS payment calculation:

- Link to REF-pricing-calculator-prd-v1.0.md for calculator details
- Link to PRD-mpfs-prd-v1.0.md for ingestion details
- Use consistent terminology: "on-request calculation" or "runtime calculation"
- Distinguish between "input datasets" (ingestion) and "computed prices" (calculator)

## Notes

- REF-pricing-calculator-prd-v1.0.md already exists and is correctly structured
- PRD-mpfs-prd-v1.0.md is already updated (source of truth)
- This plan ensures all dependent documentation aligns with the MPFS PRD changes