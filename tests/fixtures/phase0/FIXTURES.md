# Phase 0 Test Fixtures

Pinned test data with SHA-256 for reproducibility per STD-parser-contracts v1.1.

**Purpose:** Provide stable, version-controlled test data for Phase 0 integration tests.

---

## Files

### pprrvu_sample.csv (113 bytes)
**SHA-256:** `a947d772c13b5079d292393d9e8094e7c58af181085dc50d484e6b73b068e523`

**Content:** 3 valid PPRRVU rows for basic pipeline testing

```csv
hcpcs,modifier,work_rvu,effective_from
99213,,0.93,2025-01-01
99214,26,1.50,2025-01-01
99215,TC,1.10,2025-01-01
```

**Used in:**
- `test_pprrvu_full_flow_with_duplicates()` (valid case)
- `test_determinism_end_to_end_bulletproof()`

---

### pprrvu_with_dupes.csv (111 bytes)
**SHA-256:** `02beef0e5920a1c261827a5e05e245828bfe0549aab93af3a1e5502d7f043050`

**Content:** 3 rows with 2 duplicates (same hcpcs + modifier)

```csv
hcpcs,modifier,work_rvu,effective_from
99213,,0.93,2025-01-01
99213,,0.93,2025-01-01
99214,26,1.50,2025-01-01
```

**Used in:**
- `test_pprrvu_full_flow_with_duplicates()` - Tests `NATURAL_KEY_DUPLICATE` detection

---

### pprrvu_invalid_modifier.csv (118 bytes)
**SHA-256:** `994673948ec4bbb37a1287ddc06fb2a28271e517a23c3e82d374373e5ada7020`

**Content:** 3 rows with 1 invalid modifier ("INVALID" not in enum)

```csv
hcpcs,modifier,work_rvu,effective_from
99213,,0.93,2025-01-01
99214,INVALID,1.50,2025-01-01
99215,TC,1.10,2025-01-01
```

**Used in:**
- `test_categorical_rejection_flow_enhanced()` - Tests categorical validation rejects

---

### cf_sample.csv (158 bytes)
**SHA-256:** `c333c78491135b788c1a8a8420c77e773739c229c957a19633bf4f1810d9245a`

**Content:** 2 conversion factor rows with unique natural keys

```csv
cf_type,cf_value,cf_description,effective_from
physician,33.2875,PFS Conversion Factor,2025-01-01
anesthesia,22.1234,Anesthesia Conversion Factor,2025-01-01
```

**Used in:**
- `test_conversion_factor_flow_integration()` - Tests unique natural keys (cf_type + effective_from)

---

## Validation

To verify fixtures haven't changed:

```bash
cd tests/fixtures/phase0
shasum -a 256 -c <<EOF
a947d772c13b5079d292393d9e8094e7c58af181085dc50d484e6b73b068e523  pprrvu_sample.csv
02beef0e5920a1c261827a5e05e245828bfe0549aab93af3a1e5502d7f043050  pprrvu_with_dupes.csv
994673948ec4bbb37a1287ddc06fb2a28271e517a23c3e82d374373e5ada7020  pprrvu_invalid_modifier.csv
c333c78491135b788c1a8a8420c77e773739c229c957a19633bf4f1810d9245a  cf_sample.csv
EOF
```

Expected output: `OK` for all files

---

## Schema Contracts

All fixtures use schema contracts from `cms_pricing/ingestion/contracts/`:
- **PPRRVU:** `cms_pprrvu_v1.0.json` (v1.1 with natural_keys)
- **Conversion Factor:** `cms_conversion_factor_v1.0.json` (v2.0)

---

## Maintenance

When updating fixtures:
1. Modify CSV file
2. Recompute SHA-256: `shasum -a 256 <file>`
3. Update hash in this document
4. Update affected tests if schema changes

---

**Generated:** Phase 0 Commit 5  
**Schema Version:** v1.1 (PPRRVU, GPCI), v2.0 (CF)  
**Last Updated:** 2025-10-15

