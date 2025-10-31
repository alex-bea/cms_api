# Phase 2.8: Documentation & Rollout Plan

## Overview
This plan covers documenting the provenance fields added in Phase 2 across OpenAPI schemas, endpoint documentation, and readiness tracking.

## 1. OpenAPI Schema Documentation

### 1.1 Update `PricingResponse.datasets_used` Schema
**File:** `cms_pricing/schemas/pricing.py`

**Current State:**
```python
datasets_used: List[Dict[str, Any]] = Field(..., description="Datasets and versions used")
```

**Updates Required:**
- Enhance `Field` description to document the structure of each dictionary
- Add detailed explanation of provenance fields: `dataset_id`, `release_id`, `batch_id`, `effective_from`, `effective_to`, `digest`
- Document that `release_id` and `batch_id` may be `None` for legacy data
- Include example structure in docstring or via `Field` description

**Target Schema Documentation:**
```python
datasets_used: List[Dict[str, Any]] = Field(
    ...,
    description="""List of datasets used in this pricing calculation, including provenance metadata.
    
    Each entry contains:
    - dataset_id (str): Dataset identifier (e.g., 'MPFS', 'OPPS', 'ASC', 'CLFS', 'DMEPOS', 'IPPS')
    - release_id (str, optional): CMS release identifier (e.g., 'mpfs_2025_annual_20250115'). 
      May be None for legacy data ingested before Phase 2.
    - batch_id (str, optional): Batch identifier from ingestion run (e.g., 'batch_abc123').
      May be None for legacy data ingested before Phase 2.
    - effective_from (str, optional): ISO date when dataset snapshot becomes effective
    - effective_to (str, optional): ISO date when dataset snapshot expires (None for current)
    - digest (str, optional): Content digest/hash of dataset snapshot
    
    Example:
    [
        {
            "dataset_id": "MPFS",
            "release_id": "mpfs_2025_annual_20250115",
            "batch_id": "batch_abc123",
            "effective_from": "2025-01-01",
            "effective_to": None
        }
    ]
    """
)
```

### 1.2 Update `LineItemResponse.trace_refs` Documentation
**File:** `cms_pricing/schemas/pricing.py`

**Updates Required:**
- Document standardized provenance format: `{dataset_id}:release:{release_id}` and `{dataset_id}:batch:{batch_id}`
- Explain deduplication behavior
- Provide example trace_refs array

**Target Documentation:**
```python
trace_refs: List[str] = Field(
    default_factory=list,
    description="""Trace reference IDs for debugging and audit purposes.
    
    Format:
    - Dataset-specific references: '{dataset}_{year}_{params}_{code}' (e.g., 'mpfs_2025_01_99213')
    - Provenance references (Phase 2): '{dataset_id}:release:{release_id}' or '{dataset_id}:batch:{batch_id}'
      Examples: 'MPFS:release:mpfs_2025_annual_20250115', 'MPFS:batch:batch_abc123'
    
    Duplicates are automatically removed to keep logs clean.
    """
)
```

### 1.3 Verify FastAPI Auto-Documentation
- Ensure Pydantic `Field` descriptions appear in `/docs` OpenAPI UI
- Check that examples render correctly in Swagger UI
- Validate schema export at `/openapi.json` includes all provenance fields

## 2. Endpoint Documentation

### 2.1 Update Router Docstrings
**Files:** 
- `cms_pricing/routers/pricing.py` (`price_plan`, `compare_locations`, `price_single_code`)
- Any other pricing-related routers

**Updates Required:**
- Add notes about provenance fields in response documentation
- Document trace_ref format for API consumers
- Mention backward compatibility (None values for legacy data)

**Example Docstring Addition:**
```python
@router.post("/price", response_model=PricingResponse)
async def price_plan(...):
    """
    Price a treatment plan.
    
    **Provenance Metadata (Phase 2):**
    The response includes provenance information in two places:
    
    1. **datasets_used** (response.datasets_used): List of datasets with release_id and batch_id
       - May contain None values for legacy data ingested before Phase 2
       - Enables traceability to specific CMS data versions
    
    2. **trace_refs** (line_items[].trace_refs): Trace references in standardized format
       - Format: `{dataset_id}:release:{release_id}` or `{dataset_id}:batch:{batch_id}`
       - Examples: 'MPFS:release:mpfs_2025_annual_20250115', 'OPPS:batch:batch_abc123'
       - Automatically deduplicated
    
    **Backward Compatibility:**
    Legacy data (ingested before Phase 2) will have None values for release_id and batch_id.
    The API structure remains consistent; consumers should handle optional provenance gracefully.
    """
```

### 2.2 Create/Update API Reference Documentation
**Location:** `docs/api/` or `README.md` section

**Content Required:**
- Provenance fields overview
- Trace reference format specification
- Example response payloads with provenance
- Migration guide for consumers

## 3. Readiness Plan Updates

### 3.1 Mark Phase 2.7 Complete
**File:** `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md`

**Updates:**
- Add entry to Section 10 (Change Log) marking Phase 2.7 completion
- Update Section 5.1 (Data Quality & Provenance) workstream status

### 3.2 Add Phase 2.8 Action Items
**File:** `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md`

**Add to Section 5.2 (API Contract & Clients):**
- [ ] Document provenance fields in OpenAPI schema (`datasets_used`, `trace_refs`)
- [ ] Update endpoint docstrings with provenance format documentation
- [ ] Publish API reference guide with provenance examples
- [ ] Update client SDK examples to demonstrate provenance parsing

**Add to Section 5.1 (Data Quality & Provenance):**
- Mark Phase 2.7 (Testing) as complete with date
- Add Phase 2.8 (Documentation) as in-progress

## 4. Validation Checklist

### 4.1 Schema Documentation
- [ ] `PricingResponse.datasets_used` Field description documents all provenance fields
- [ ] `LineItemResponse.trace_refs` documents standardized format
- [ ] FastAPI `/docs` UI shows provenance field descriptions
- [ ] `/openapi.json` export includes complete provenance schema

### 4.2 Endpoint Documentation
- [ ] All pricing router endpoints have provenance documentation in docstrings
- [ ] API reference documentation created/updated with examples
- [ ] Trace reference format documented for consumers

### 4.3 Readiness Plan
- [ ] Phase 2.7 marked complete in change log
- [ ] Phase 2.8 action items added to Section 5.2
- [ ] Status updated in milestone tracking

## 5. Deliverables Summary

1. **OpenAPI Schema Enhancements**
   - Detailed `datasets_used` field description with structure documentation
   - Enhanced `trace_refs` field description with format specification

2. **Endpoint Documentation**
   - Updated docstrings for pricing routers
   - API reference guide with provenance examples

3. **Readiness Plan Updates**
   - Phase 2.7 completion entry
   - Phase 2.8 action items tracked
   - Documentation milestone status updated

## 6. Rollout Sequence

1. Update Pydantic schemas with enhanced Field descriptions
2. Update router docstrings with provenance notes
3. Verify OpenAPI auto-documentation renders correctly
4. Update readiness plan with Phase 2.7 completion and Phase 2.8 tracking
5. Create/update API reference documentation (if separate from code)
6. Validate all changes with schema export and documentation review

## 7. Dependencies

- Phase 2.1-2.7 must be complete (migration, models, ingestion, engines, service layer, tests)
- FastAPI application running for `/docs` and `/openapi.json` validation
- Documentation standards from `prds/STD-api-docs-prd-v1.0.md`

