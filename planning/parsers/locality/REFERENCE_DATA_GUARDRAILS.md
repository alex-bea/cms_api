# Reference Data Guardrails

**Status:** ✅ Critical implemented, 📋 Documented for Phase 0A completion

## Implemented (Phase 0B - 7 min) ✅

### 1. Feature Flag (`REF_MODE`)
**File:** `cms_pricing/ingestion/normalize/reference_mode.py`

```bash
# Dev/CI: Use inline dict
export REF_MODE=inline

# Production: Use curated /ref/ infrastructure (default)
export REF_MODE=curated
```

**Behavior:**
- `inline`: Blocks publish, allows normalize/enrich for dev inspection
- `curated`: Allows full pipeline including publish

### 2. Fail-Closed Policy
**Function:** `validate_publish_allowed()`

- Enricher **refuses** to publish curated outputs if `REF_MODE=inline`
- Raises `RuntimeError` with clear message
- Matches DIS Appendix J (fail-closed on missing refs)

### 3. Contract Validation
**Functions:** `get_states_dataframe()`, `get_counties_dataframe()`

- Inline dicts validate against `STATE_SCHEMA`, `COUNTY_SCHEMA`
- Schema matches curated reference data structure
- Prevents "works in dev, breaks in prod"

### 4. Determinism Guards
**Added to inline DataFrames:**
- `mapping_confidence = 1.0` (always present)
- Sorted outputs (`state_fips`, `county_fips`)
- Ensures stable checksums between inline/curated modes

---

## Documented for Phase 0A (30 min) 📋

### 5. Inline Provider Shim
**TODO:** Create `InlineReferenceProvider` class

```python
class InlineReferenceProvider:
    """Shim providing same interface as ReferenceDataManager"""
    
    def get_reference_data(self, source_name, effective_date=None):
        # Returns pandas/Arrow with fake version="dev-inline"
        # Writes mock manifest under /stage/<release>/ref_mocks/
        pass
```

**Benefits:**
- Same interface as `ReferenceDataManager`
- Easy mode switching
- Mock manifests for lineage visibility

### 6. Observability Metrics
**Function:** `get_reference_metadata()` ✅ (partial)

**TODO:** Add to run summaries and dashboards:
- `ref_vintage_used`
- `ref_source` (curated vs inline)
- `conflict_rate`
- `fallback_usage_rate`
- `ref_coverage` (% of lookups matched)

### 7. Minimal True /ref Seed
**TODO:** Ship minimal parquet in repo

```
/ref/
  census/
    fips_states/2025/
      us_states.parquet    # 51 rows (ship in git)
      manifest.json
```

**Strategy:**
- Ship lightweight refs (states) in repo
- Use inline only for heavy sets (ZIP↔ZCTA, counties) during dev
- Reduces inline scope

---

## Policy (No Code, Document Only) 📝

### 8. No CPT Leakage
**Policy:** Inline data = keys only, **no descriptions** for Restricted data

**Enforcement:**
- Code review checklist
- Inline dicts contain only:
  - FIPS codes (public domain)
  - State/county names (public domain)
  - Geographic keys
- **Never** include:
  - CPT descriptions (AMA licensed)
  - HCPCS long descriptions (CMS restricted)
  - ICD-10 narratives (WHO licensed)

### 9. Zip Safety (If Needed)
**Policy:** If bundling inline refs as ZIPs:
- Max compressed size: 10 MB
- Max uncompressed size: 50 MB
- Ban path traversal (`..` in paths)
- Validate all entries before extract

---

## Testing

### Test 1: Mode Switching
```python
def test_ref_mode_inline():
    os.environ['REF_MODE'] = 'inline'
    config = get_reference_config()
    assert config.mode == ReferenceMode.INLINE
    assert config.allow_publish == False

def test_ref_mode_curated():
    os.environ['REF_MODE'] = 'curated'
    config = get_reference_config()
    assert config.mode == ReferenceMode.CURATED
    assert config.allow_publish == True
```

### Test 2: Fail-Closed Enforcement
```python
def test_publish_blocked_with_inline():
    os.environ['REF_MODE'] = 'inline'
    config = get_reference_config()
    
    with pytest.raises(RuntimeError, match="Cannot.*publish.*inline"):
        validate_publish_allowed(config)
```

### Test 3: Schema Validation
```python
def test_inline_matches_curated_schema():
    states = get_states_dataframe()
    assert 'state_fips' in states.columns
    assert 'mapping_confidence' in states.columns
    assert states['state_fips'].dtype == 'object'  # str
```

### Test 4: Determinism
```python
def test_inline_deterministic_output():
    df1 = get_states_dataframe()
    df2 = get_states_dataframe()
    
    # Same order, same content
    assert df1.equals(df2)
    
    # Mapping confidence present
    assert (df1['mapping_confidence'] == 1.0).all()
```

---

## Impact Summary

| Guardrail | Status | Risk Prevented | Time |
|-----------|--------|----------------|------|
| Feature flag | ✅ Impl | Inline in prod | 3 min |
| Fail-closed policy | ✅ Impl | Bad publish | 2 min |
| Contract validation | ✅ Impl | Dev/prod drift | 2 min |
| Determinism | ✅ Impl | Hash mismatch | 1 min |
| Inline provider shim | 📋 Doc | Interface divergence | 15 min |
| Observability | 📋 Doc | Blind mode issues | 10 min |
| Minimal ref seed | 📋 Doc | Large inline scope | 5 min |
| No CPT leakage | 📝 Policy | Compliance violation | - |
| Zip safety | 📝 Policy | Security issues | - |

**Total Implemented:** 7 minutes  
**Total Remaining:** 30 minutes (Phase 0A completion)

---

## Recommendation

✅ **Critical guardrails in place** - safe to proceed to Phase 1

**Before production:** Complete Phase 0A tasks (Inline Provider shim, observability)

**Reference:**
- User feedback 2025-10-17
- Aligns with GPCI lessons learned (catch issues early)
- Follows DIS fail-closed policy (Appendix J)

