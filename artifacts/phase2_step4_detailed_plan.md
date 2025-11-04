# Phase 2 Step 4: Move Validation Rules to DatasetSpec - Detailed Plan

**Goal:** Extract `_register_validation_rules()` (98 lines) from RVUIngestor into `rvu_spec.py` and auto-register business rules during initialization.

**Status:** ⏳ **PENDING**

**Current State:**
- `_register_validation_rules()` in RVUIngestor (line 187, ~98 lines)
- Two business rules: `validate_pprrvu_uniqueness` and `validate_gpci_ranges_wrapper`
- Business rules return `ValidationResult` objects (more complex than ValidationRule bool functions)
- DatasetSpecs already have `validation_rules` (ValidationRule objects with bool validators)
- Business rules are registered via `register_business_rule(dataset_id, rule_function)`

**Target State:**
- Business rule functions extracted to `rvu_spec.py`
- Business rules auto-registered from DatasetSpec during RVUIngestor initialization
- `_register_validation_rules()` removed from RVUIngestor
- All validation rules centralized in DatasetSpec

---

## Key Distinction: ValidationRule vs Business Rules

**ValidationRule objects** (already in DatasetSpec):
- Simple boolean validators: `validate_hcpcs_format(df) -> bool`
- Used for structural/format validation
- Stored in `DatasetSpec.validation_rules: List[ValidationRule]`

**Business Rules** (need to extract):
- Complex validators returning `ValidationResult`: `validate_pprrvu_uniqueness(df) -> ValidationResult`
- Used for business logic validation (uniqueness, ranges, etc.)
- Registered via `validation_engine.register_business_rule(dataset_id, rule_function)`

**Decision:** Add `business_rules: List[Callable]` field to DatasetSpec to store business rule functions.

---

## Detailed Task Breakdown

### Task 1: Add Business Rules Field to DatasetSpec

**File:** `cms_pricing/ingestion/datasets/spec.py`

**Changes:**
```python
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class DatasetSpec:
    dataset_id: str
    parser: Callable
    schema_id: str
    natural_keys: List[str]
    loader: Callable
    validation_rules: List[ValidationRule] = field(default_factory=list)
    enrichment_rules: List[EnrichmentRule] = field(default_factory=list)
    filename_patterns: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # NEW: Business rules that return ValidationResult
    business_rules: List[Callable[[pd.DataFrame], ValidationResult]] = field(default_factory=list)
```

**Estimated Time:** 15 minutes

---

### Task 2: Extract Business Rule Functions to rvu_spec.py

**File:** `cms_pricing/ingestion/datasets/rvu_spec.py`

**Changes:**
1. Import required dependencies:
   ```python
   from ..contracts.ingestor_spec import ValidationResult, ValidationSeverity
   from ..validators.validation_engine import check_natural_key_uniqueness
   from ..parsers.gpci_parser import _validate_gpci_ranges
   import pandas as pd
   ```

2. Extract business rule functions:
   ```python
   def _create_pprrvu_business_rules() -> List[Callable[[pd.DataFrame], ValidationResult]]:
       """Create business rule validators for PPRRVU dataset."""
       
       def validate_pprrvu_uniqueness(df: pd.DataFrame) -> ValidationResult:
           """Validate PPRRVU natural key (hcpcs, modifier) uniqueness."""
           # ... (move from RVUIngestor._register_validation_rules)
       
       return [validate_pprrvu_uniqueness]
   
   def _create_gpci_business_rules() -> List[Callable[[pd.DataFrame], ValidationResult]]:
       """Create business rule validators for GPCI dataset."""
       
       def validate_gpci_ranges_wrapper(df: pd.DataFrame) -> ValidationResult:
           """Validate GPCI values are within acceptable ranges."""
           # ... (move from RVUIngestor._register_validation_rules)
       
       return [validate_gpci_ranges_wrapper]
   ```

3. Update DatasetSpec instances to include business_rules:
   ```python
   RVU_DATASETS: Dict[str, DatasetSpec] = {
       "pprrvu": DatasetSpec(
           dataset_id="pprrvu",
           parser=parse_pprrvu,
           schema_id=PPRRVU_SCHEMA_ID,
           natural_keys=PPRRVU_NK,
           loader=load_pprrvu_data,
           validation_rules=_create_pprrvu_validation_rules(),
           business_rules=_create_pprrvu_business_rules(),  # NEW
           # ... other fields
       ),
       "gpci": DatasetSpec(
           dataset_id="gpci",
           parser=parse_gpci,
           schema_id=GPCI_SCHEMA_ID,
           natural_keys=GPCI_NK,
           loader=load_gpci_data,
           validation_rules=_create_gpci_validation_rules(),
           business_rules=_create_gpci_business_rules(),  # NEW
           # ... other fields
       ),
       # ... other datasets (oppscap, anescf, localitycounty can have empty business_rules=[])
   }
   ```

**Key Logic to Extract:**
- `validate_pprrvu_uniqueness`: ~47 lines (includes check_natural_key_uniqueness call)
- `validate_gpci_ranges_wrapper`: ~40 lines (includes _validate_gpci_ranges call)

**Estimated Time:** 45 minutes

---

### Task 3: Create Auto-Registration Helper

**Option A: Add to ValidationService** (Recommended)

**File:** `cms_pricing/ingestion/services/validation_service.py`

**Changes:**
```python
class ValidationService:
    """Adapter for ValidationEngine with multi-ingestor support."""
    
    def __init__(self, validation_engine: Any):
        self.validation_engine = validation_engine
    
    def register_dataset_business_rules(self, dataset_spec: DatasetSpec) -> None:
        """
        Register all business rules for a dataset from its DatasetSpec.
        
        Args:
            dataset_spec: DatasetSpec containing business_rules list
        """
        if not dataset_spec.business_rules:
            return
        
        for rule_func in dataset_spec.business_rules:
            self.validation_engine.register_business_rule(
                dataset_spec.dataset_id,
                rule_func
            )
        logger.debug(
            "Registered business rules",
            dataset=dataset_spec.dataset_id,
            count=len(dataset_spec.business_rules)
        )
```

**Option B: Add to RVUIngestor.__init__** (Simpler, but less reusable)

**Estimated Time:** 20 minutes

---

### Task 4: Update RVUIngestor Initialization

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Changes:**

1. **Remove method call:**
   ```python
   # OLD (in __init__):
   # Register validation rules
   self._register_validation_rules()  # Remove this
   ```

2. **Add auto-registration:**
   ```python
   # NEW (in __init__):
   # Auto-register business rules from DatasetSpecs
   from ..datasets import RVU_DATASETS
   for dataset_id, spec in RVU_DATASETS.items():
       if spec.business_rules:
           self.services.validation_service.register_dataset_business_rules(spec)
   ```

3. **Remove method:**
   - Delete `_register_validation_rules()` method (lines 187-284)

**Estimated Time:** 15 minutes

---

### Task 5: Update ServiceFactory (if using Option A)

**File:** `cms_pricing/ingestion/services/service_factory.py`

**Changes:**
If ValidationService doesn't exist yet, create it as a thin adapter:
```python
@property
def validation_service(self) -> Any:
    """Get or create validation service (lazy)."""
    if not self.config.enable_validation:
        raise NotImplementedError("Validation is disabled in ServiceConfig.")
    
    if "validation_service" not in self._services:
        from .validation_service import ValidationService
        self._services["validation_service"] = ValidationService(
            self.validation_engine
        )
        logger.debug("Validation service initialized", dataset=self.config.dataset_name)
    
    return self._services["validation_service"]
```

**Estimated Time:** 10 minutes (if needed)

---

### Task 6: Handle Edge Cases

**Edge Cases:**

1. **Empty Business Rules:**
   - Some datasets (oppscap, anescf, localitycounty) may not have business rules
   - Return empty list `[]` from `_create_*_business_rules()`

2. **Missing Dependencies:**
   - `check_natural_key_uniqueness` must be importable
   - `_validate_gpci_ranges` must be importable from gpci_parser
   - Handle import errors gracefully

3. **Registration Order:**
   - Business rules should be registered before validation stage runs
   - Registration happens in `__init__`, so it's early enough

4. **Idempotent Registration:**
   - `register_business_rule` may need to handle duplicate registrations
   - Check if ValidationEngine supports idempotent registration

**Estimated Time:** 15 minutes

---

## Implementation Checklist

### Pre-Implementation
- [ ] Review `_register_validation_rules()` method thoroughly
- [ ] Verify `check_natural_key_uniqueness` import path
- [ ] Verify `_validate_gpci_ranges` import path
- [ ] Check if ValidationEngine.register_business_rule is idempotent
- [ ] Review DatasetSpec structure

### Implementation
- [ ] Add `business_rules` field to DatasetSpec
- [ ] Extract `_create_pprrvu_business_rules()` to rvu_spec.py
- [ ] Extract `_create_gpci_business_rules()` to rvu_spec.py
- [ ] Update RVU_DATASETS to include business_rules
- [ ] Create ValidationService.register_dataset_business_rules() helper (Option A) OR add inline registration (Option B)
- [ ] Update RVUIngestor.__init__ to auto-register business rules
- [ ] Remove `_register_validation_rules()` from RVUIngestor

### Testing
- [ ] Test business rules are registered during initialization
- [ ] Test validate_pprrvu_uniqueness still works
- [ ] Test validate_gpci_ranges_wrapper still works
- [ ] Test validation stage uses business rules
- [ ] Test empty business_rules list (for datasets without business rules)
- [ ] Run full pipeline test to verify validation still works

### Verification
- [ ] Verify no functionality lost
- [ ] Verify validation rules still applied correctly
- [ ] Check line count reduction (~98 lines removed)
- [ ] Run full test suite

---

## Code Examples

### Example 1: Business Rule Function Extraction

**OLD (RVUIngestor._register_validation_rules):**
```python
def _register_validation_rules(self):
    def validate_pprrvu_uniqueness(df: pd.DataFrame) -> ValidationResult:
        # ... 47 lines of logic ...
    
    self.services.validation_engine.register_business_rule("pprrvu", validate_pprrvu_uniqueness)
```

**NEW (rvu_spec.py):**
```python
def _create_pprrvu_business_rules() -> List[Callable[[pd.DataFrame], ValidationResult]]:
    def validate_pprrvu_uniqueness(df: pd.DataFrame) -> ValidationResult:
        # ... 47 lines of logic (moved from RVUIngestor) ...
    
    return [validate_pprrvu_uniqueness]

RVU_DATASETS["pprrvu"] = DatasetSpec(
    # ... other fields ...
    business_rules=_create_pprrvu_business_rules(),
)
```

### Example 2: Auto-Registration

**OLD (RVUIngestor.__init__):**
```python
self._register_validation_rules()  # 98-line method
```

**NEW (RVUIngestor.__init__):**
```python
# Auto-register business rules from DatasetSpecs
from ..datasets import RVU_DATASETS
for dataset_id, spec in RVU_DATASETS.items():
    if spec.business_rules:
        self.services.validation_service.register_dataset_business_rules(spec)
```

### Example 3: ValidationService Helper (Option A)

**NEW (services/validation_service.py):**
```python
def register_dataset_business_rules(self, dataset_spec: DatasetSpec) -> None:
    """Register all business rules for a dataset."""
    if not dataset_spec.business_rules:
        return
    
    for rule_func in dataset_spec.business_rules:
        self.validation_engine.register_business_rule(
            dataset_spec.dataset_id,
            rule_func
        )
```

---

## Dependencies & Risks

### Dependencies
- ✅ DatasetSpec must support business_rules field (Task 1)
- ✅ ValidationEngine.register_business_rule() must exist
- ✅ `check_natural_key_uniqueness` must be importable
- ✅ `_validate_gpci_ranges` must be importable

### Risks

1. **Low Risk: Import Path Changes**
   - **Mitigation:** Verify import paths before extraction
   - **Mitigation:** Test imports work from rvu_spec.py

2. **Low Risk: Registration Timing**
   - **Mitigation:** Register in `__init__` before validation stage
   - **Mitigation:** Test that rules are available when validation runs

3. **Low Risk: Function Closure**
   - **Mitigation:** Business rules are self-contained functions
   - **Mitigation:** No instance variables needed (all use parameters)

### Risk Mitigation Strategy

1. **Incremental Approach:**
   - First: Add business_rules field to DatasetSpec
   - Second: Extract one business rule function
   - Third: Extract second business rule function
   - Fourth: Wire up auto-registration
   - Test after each step

2. **Backward Compatibility:**
   - Keep old method until new one verified (optional)
   - Compare registration before/after

3. **Comprehensive Testing:**
   - Unit tests for business rule functions
   - Integration tests with validation stage
   - Regression tests comparing validation results

---

## Success Criteria

- [ ] `business_rules` field added to DatasetSpec
- [ ] `_create_pprrvu_business_rules()` extracted to rvu_spec.py
- [ ] `_create_gpci_business_rules()` extracted to rvu_spec.py
- [ ] All RVU_DATASETS updated with business_rules
- [ ] Business rules auto-registered during RVUIngestor initialization
- [ ] `_register_validation_rules()` removed from RVUIngestor (~98 lines)
- [ ] All tests pass
- [ ] Validation still works end-to-end
- [ ] No functionality lost

---

## Estimated Timeline

| Task | Time | Cumulative |
|------|------|------------|
| Task 1: Add business_rules field to DatasetSpec | 15 min | 15 min |
| Task 2: Extract business rule functions | 45 min | 1 hour |
| Task 3: Create auto-registration helper | 20 min | 1.25 hrs |
| Task 4: Update RVUIngestor initialization | 15 min | 1.5 hrs |
| Task 5: Update ServiceFactory (if needed) | 10 min | 1.67 hrs |
| Task 6: Handle edge cases | 15 min | 1.83 hrs |
| Testing & verification | 20 min | 2 hrs |

**Total Estimated Time:** 1.5-2 hours (plan allows 30 minutes, but extraction may take longer)

---

## Notes

- This step is simpler than Step 3 (adapter extraction) because:
  - Only 2 business rule functions to extract (vs 429 lines of adapter logic)
  - Business rules are self-contained (no instance variables)
  - Registration is straightforward (just call register_business_rule)
  
- **Key Decision:** Whether to create ValidationService helper (Option A) or inline registration (Option B)
  - **Option A (Recommended):** More reusable, aligns with ServiceFactory pattern
  - **Option B (Simpler):** Faster to implement, but less reusable

- **ValidationRule vs Business Rules:** Keep both patterns:
  - `validation_rules`: Simple boolean validators (structural validation)
  - `business_rules`: Complex ValidationResult validators (business logic validation)

- Consider adding business_rules to DatasetSpec for future ingestors (MPFS, OPPS, ZIP9)

---

## Implementation Notes

### Import Dependencies
```python
# In rvu_spec.py:
from ..contracts.ingestor_spec import ValidationResult, ValidationSeverity
from ..validators.validation_engine import check_natural_key_uniqueness
from ..parsers.gpci_parser import _validate_gpci_ranges
import pandas as pd
```

### Function Signatures
```python
# Business rule functions must match this signature:
def validate_*(df: pd.DataFrame) -> ValidationResult:
    """Validate dataset-specific business rules."""
    # Returns ValidationResult with passed, message, details
```

### Registration Pattern
```python
# Pattern for auto-registration:
for spec in RVU_DATASETS.values():
    if spec.business_rules:
        for rule_func in spec.business_rules:
            validation_engine.register_business_rule(spec.dataset_id, rule_func)
```

