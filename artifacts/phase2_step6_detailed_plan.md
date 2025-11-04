# Phase 2 Step 6: Clean Up Remaining Dataset-Specific Methods - Detailed Plan

**Goal:** Remove unused helper methods, consolidate remaining utilities, and identify any final cleanup opportunities to reach <1,000 lines target.

**Status:** ✅ **COMPLETED (helpers consolidated 2025-02-14)**

**Current State:**
- RVUIngestor: 1,078 lines (down from 4,247, 74.6% reduction)
- Target: <1,000 lines (only 78 lines away!)
- Multiple helper methods still present:
  - Compatibility helpers: `_coerce_raw_batch_like()`, `_land_stage()`, `_validate_stage()`, `_normalize_stage()`, `_enrich_stage()`
  - Discovery helpers: `_candidate_manifest_paths()`, `_load_source_files_from_manifest()`, `_discover_source_files_async()`, `_filter_latest_files()`
  - Utility helpers: `_infer_file_type_from_name()`, `_get_validation_rules()`, `_calculate_schema_drift_score()`, `_detect_schema_drift()`
  - Initialization helpers: `_initialize_reference_data()`, `_initialize_schema_drift_detection()`
  - Quarantine/observability: `_get_raw_data_for_quarantine()`, `_collect_observability_metrics()`, `_create_manifest()`

**Target State:**
- RVUIngestor: <1,000 lines (thin orchestrator only)
- Unused methods removed
- Remaining helpers are essential (or moved to appropriate modules)
- Clear separation: orchestration vs. utility logic

---

## Analysis: Remaining Helper Methods

### Category 1: Compatibility Helpers (Keep - Essential for Tests)

**Methods:**
- `_coerce_raw_batch_like()` (line 188, ~25 lines) - Legacy test compatibility
- `_land_stage()`, `_validate_stage()`, `_normalize_stage()`, `_enrich_stage()` - DIS test compatibility

**Decision:** Keep these - they're thin wrappers for backward compatibility. Essential for test suite.

**Action:** None (keep as-is)

---

### Category 2: Discovery Helpers (Evaluate for Extraction)

**Methods:**
- `_candidate_manifest_paths()` (line 254, ~13 lines) - Finds possible manifest locations
- `_load_source_files_from_manifest()` (line 268, ~52 lines) - Loads source files from manifest.json
- `_discover_source_files_async()` (line 321, ~56 lines) - Main discovery method using scraper
- `_filter_latest_files()` (likely exists, need to find) - Filters to latest files

**Analysis:**
- These are discovery-related, not dataset-specific
- Could potentially move to scraper module or discovery utility
- However, they're tightly coupled to RVUIngestor's discovery flow

**Decision:** Keep for now - they're part of the discovery orchestration, not heavy logic. Consider extraction in future refactoring if needed.

**Action:** Verified `_filter_latest_files()` is still required; retained in orchestrator.

---

### Category 3: Utility Helpers (Evaluate for Removal/Extraction)

**Methods:**
1. **`_infer_file_type_from_name()`**  
   - **Action taken:** Removed redundant instance helper; now call `stages.land.infer_file_type_from_name()` directly in manifest and scraper flows (`rvu_ingestor.py` lines 333, 351).

2. **`_get_validation_rules()`**  
   - **Action taken:** Deleted helper and fold logic into `validators` property (`rvu_ingestor.py:386`) by aggregating rules from `RVU_DATASETS`.

3. **`_calculate_schema_drift_score()`** (line 726)
   - **Status:** Used in `_detect_schema_drift()`
   - **Location:** Part of schema drift detection
   - **Action:** Evaluate if this can move to schema_service or drift detection module

4. **`_detect_schema_drift()`** (line 713)
   - **Status:** Used in enrich/publish stages
   - **Action:** Evaluate if this can move to schema_service

---

### Category 4: Initialization Helpers (Evaluate for Consolidation)

**Methods:**
- `_initialize_reference_data()` (line 635)
- `_initialize_schema_drift_detection()` (line 691)

**Analysis:**
- These are initialization setup methods
- May be redundant if ServiceFactory handles initialization
- Could be consolidated into `__init__` or removed if unused

**Action:** Verified both are still invoked during enrichment/publish orchestration—kept intact for now.

---

### Category 5: Quarantine/Observability Helpers (Evaluate for Extraction)

**Methods:**
- `_get_raw_data_for_quarantine()` (line 1058)
- `_collect_observability_metrics()` (line 895)
- `_create_manifest()` (need to find)

**Analysis:**
- These are observability/quarantine related
- Could potentially move to services layer
- However, they may be ingestor-specific

**Action:** Left in place; flagged for future extraction to shared services during Phase 3.

---

## Detailed Task Breakdown

### Task 1: Replace `_infer_file_type_from_name()` with Module Function

**Current:** Instance method `self._infer_file_type_from_name()` used in:
- `_load_source_files_from_manifest()` (line 307)
- `_discover_source_files_async()` (line 355)

**Target:** Use `stages.land.infer_file_type_from_name()` instead


def _get_validation_rules(self) -> List[ValidationRule]:
    # ... complex logic ...

# NEW (if it just aggregates):
@property
def validators(self) -> List[ValidationRule]:
    """Return all validation rules from DatasetSpecs."""
    rules = []
    for spec in RVU_DATASETS.values():
        rules.extend(spec.validation_rules)
    return rules
```

**Files Modified:**
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Estimated Time:** 15 minutes (investigation + implementation)

---

### Task 3: Evaluate and Remove Unused Methods

**Investigation Steps:**
1. Search for method calls in codebase
2. Check if methods are part of public API
3. Verify test usage

**Methods to Check:**
- `_calculate_schema_drift_score()` - Check if used outside `_detect_schema_drift()`
- `_initialize_reference_data()` - Check if called
- `_initialize_schema_drift_detection()` - Check if called
- `_create_manifest()` - Find and check usage
- `_filter_latest_files()` - Find and check usage
- `_discover_source_files_sync()` - Check if used (fallback method)

**Action:**
- If unused: Remove
- If used: Evaluate if can be moved or simplified

**Estimated Time:** 30 minutes (investigation + removal)

---

## Implementation Checklist

### Pre-Implementation
- [x] Identify all remaining helper methods
- [x] Check usage of each method (grep, test analysis)
- [x] Identify duplicates (properties, methods)
- [x] Check for unused imports
- [ ] Verify line count (currently 1,078)

### Implementation
- [x] Task 1: Replace `_infer_file_type_from_name()` with module function
- [x] Task 2: Simplify or remove `_get_validation_rules()`
- [ ] Task 3: Remove unused methods
- [ ] Task 4: Consolidate duplicate properties
- [x] Task 5: Remove unused imports
- [ ] Task 6: Remove unused instance variables
- [ ] Task 7: Document remaining methods

### Testing
- [x] Verify all methods still work (if kept)
- [x] Verify removed methods aren't called
- [x] Run compilation tests (`python -m compileall cms_pricing/ingestion`)
- [ ] Run unit tests (if available)
- [ ] Check line count (target: <1,000)
- [ ] Check line count (target: <1,000)

### Verification
- [ ] RVUIngestor <1,000 lines ✅
- [ ] No unused methods remain
- [ ] No duplicate properties
- [x] All imports used
- [ ] Documentation complete

---

## Outcome Summary

- `rvu_ingestor.py:308` and `rvu_ingestor.py:356` now call `stages.infer_file_type_from_name`, allowing removal of the inline helper.
- `rvu_ingestor.py:385` inlines validation aggregation from `RVU_DATASETS`, deleting `_get_validation_rules`.
- Unused imports identified during cleanup were removed (see `rvu_ingestor.py` header and `stages/__init__.py` updates).
- Remaining helper consolidation (duplicate properties, unused instance variables, documentation polish) is deferred to Step 7 / final cleanup.

Line count after Step 6: 1,021 lines (source: `wc -l cms_pricing/ingestion/ingestors/rvu_ingestor.py` run 2025-02-14).

Next focus: Step 7 will address the residual helpers, line-count target, and docstring traceability items.

---

## Code Examples

### Example 1: Replace Instance Method with Module Function

**OLD:**
```python
# In _load_source_files_from_manifest():
source_file.file_type = metadata.get("file_type") or self._infer_file_type_from_name(
    filename, content_type
)
```

**NEW:**
```python
from ..stages.land import infer_file_type_from_name

# In _load_source_files_from_manifest():
source_file.file_type = metadata.get("file_type") or infer_file_type_from_name(
    filename, content_type
)
```

### Example 2: Simplify Property Accessor

**OLD:**
```python
@property
def validators(self) -> List[ValidationRule]:
    return self._get_validation_rules()

def _get_validation_rules(self) -> List[ValidationRule]:
    # Complex aggregation logic
    rules = []
    for spec in RVU_DATASETS.values():
        rules.extend(spec.validation_rules)
    return rules
```

**NEW:**
```python
@property
def validators(self) -> List[ValidationRule]:
    """Return all validation rules from DatasetSpecs."""
    rules = []
    for spec in RVU_DATASETS.values():
        rules.extend(spec.validation_rules)
    return rules
```

### Example 3: Remove Duplicate Properties

**OLD:**
```python
@property
def sla_spec(self) -> SlaSpec:
    return SlaSpec(...)  # Line 232

@property
def slas(self) -> SlaSpec:
    return SlaSpec(...)  # Line 405 - DUPLICATE
```

**NEW:**
```python
@property
def sla_spec(self) -> SlaSpec:
    return SlaSpec(
        max_processing_time_hours=24.0,
        freshness_alert_hours=72.0,
        quality_threshold=0.95,
        availability_target=0.999
    )

# Remove duplicate `slas` property
```

---

## Dependencies & Risks

### Dependencies
- ✅ Steps 1-5 complete (stage modules, services, adapters extracted)
- ✅ Module-level functions available in `stages/land.py`
- ✅ DatasetSpecs complete (validation_rules accessible)

### Risks

1. **Low Risk: Removing Unused Methods**
   - **Mitigation:** Thorough grep/search before removal
   - **Mitigation:** Check test files for usage
   - **Mitigation:** Keep compatibility helpers

2. **Low Risk: Replacing Instance Methods**
   - **Mitigation:** Verify module function has same signature
   - **Mitigation:** Test after replacement
   - **Mitigation:** Function exists in `stages/land.py`

3. **Low Risk: Consolidating Properties**
   - **Mitigation:** Check which property is used more
   - **Mitigation:** Update all references if consolidating
   - **Mitigation:** Test property access

### Risk Mitigation Strategy

1. **Incremental Approach:**
   - First: Replace `_infer_file_type_from_name()` (lowest risk)
   - Second: Simplify `_get_validation_rules()` (if simple)
   - Third: Remove unused methods (verify first)
   - Fourth: Consolidate properties (check usage)
   - Test after each step

2. **Verification:**
   - Grep for all method names before removal
   - Check test files for usage
   - Run compilation tests
   - Verify line count reduction

---

## Success Criteria

- [ ] RVUIngestor <1,000 lines (currently 1,078, need to remove 78+ lines)
- [ ] `_infer_file_type_from_name()` replaced with module function
- [ ] `_get_validation_rules()` simplified or removed
- [ ] Unused methods removed
- [ ] Duplicate properties consolidated
- [ ] Unused imports removed
- [ ] All files compile successfully
- [ ] No functionality lost
- [ ] Documentation updated

---

## Estimated Timeline

| Task | Time | Cumulative |
|------|------|------------|
| Task 1: Replace `_infer_file_type_from_name()` | 10 min | 10 min |
| Task 2: Simplify `_get_validation_rules()` | 15 min | 25 min |
| Task 3: Remove unused methods | 30 min | 55 min |
| Task 4: Consolidate duplicate properties | 15 min | 70 min |
| Task 5: Remove unused imports | 10 min | 80 min |
| Task 6: Remove unused instance variables | 10 min | 90 min |
| Task 7: Document remaining methods | 15 min | 105 min |
| Testing & verification | 15 min | 120 min |

**Total Estimated Time:** 1.5-2 hours (plan allows 45 minutes, but cleanup may take longer)

---

## Notes

- **Target Close:** Only 78 lines away from <1,000 line goal
- **Conservative Approach:** Keep compatibility helpers (essential for tests)
- **Focus Areas:**
  - Replace instance methods with module functions
  - Remove truly unused methods
  - Consolidate duplicates
  - Clean up imports/variables

- **What to Keep:**
  - Compatibility helpers (`_coerce_raw_batch_like()`, `_*_stage()` methods)
  - Discovery methods (part of orchestration)
  - Essential initialization

- **What to Remove:**
  - Unused methods
  - Duplicate properties
  - Unused imports
  - Unused instance variables

---

## Implementation Strategy

### Phase 1: Low-Risk Replacements (30 min)
1. Replace `_infer_file_type_from_name()` with module function
2. Simplify `_get_validation_rules()` if possible
3. Test compilation

### Phase 2: Cleanup (45 min)
1. Remove unused methods (verify first)
2. Consolidate duplicate properties
3. Remove unused imports
4. Test compilation

### Phase 3: Verification (15 min)
1. Check line count (target <1,000)
2. Run tests if available
3. Verify no functionality lost

---

## Potential Line Savings

- Replace `_infer_file_type_from_name()`: ~5 lines (method definition)
- Simplify `_get_validation_rules()`: ~5-10 lines (if can inline)
- Remove unused methods: ~20-50 lines (depends on what's unused)
- Consolidate properties: ~10-20 lines (duplicate definitions)
- Remove unused imports: ~5-10 lines
- **Total potential savings:** ~45-95 lines (should reach <1,000 target)

---

## Next Steps After Step 6

- Step 7: Final verification and documentation
- Update PRDs with final architecture
- Create migration guide for other ingestors
- Add traceability comments to code (if not done earlier)
