## Phase 3 – Shared Services Extraction Plan

### Objective
Centralize repeated service construction (validation, observability, quarantine, reference data, schema registry) into a reusable `cms_pricing/ingestion/services/` package that supports lazy initialization, uniform configuration, and easier testing across all DIS ingestors.

### Current Pain Points
- Each ingestor instantiates the same objects inside `__init__`, leading to copy/paste drift.
- Schema registry bootstrap and reference-data setup happen ad hoc; cache warm-up is inconsistent.
- Tests must stub multiple dependencies directly on ingestor instances, making setup brittle.

### Guardrails
1. **Clear Module Naming & Discovery**  
   - Align filenames with responsibilities (`observability_service.py`, `schema_service.py`, etc.).  
   - Document any placeholder modules so future maintainers do not assume functionality that is not implemented yet.

2. **Consistent Factory Surface Area**  
   - Expose a stable set of attributes from the factory (e.g., `services.validation_engine`, `services.observability`, `services.reference_data`).  
   - If a service is intentionally unavailable for an ingestor, raise a descriptive `NotImplementedError` via the property rather than returning `None`.

3. **Schema Bootstrap Coordination**  
   - Ensure legacy schema-registration code is migrated exactly once; avoid double-registration when both the factory and existing ingestors run initialization.  
   - Maintain existing schema-contract caching semantics so validation performance does not regress.

4. **Lazy Initialization Coverage**  
   - Provide unit tests that verify services are instantiated only when accessed, and that dependency wiring (e.g., `DISReferenceDataEnricher` depending on `ReferenceDataManager`) still behaves correctly.

### Proposed Structure
```
cms_pricing/ingestion/services/
├── __init__.py
├── service_config.py        # Dataclass with dataset/output_dir/flags
├── service_factory.py       # Lazy accessor properties for each service
├── observability_service.py # Thin wrapper around DISObservabilityCollector
├── quarantine_service.py    # Wrapper for QuarantineManager + helpers
├── reference_data_service.py# ReferenceDataManager + enricher helpers
├── validation_service.py    # ValidationEngine helpers / rule registration hooks
└── schema_service.py        # Schema registry bootstrap + caching
```

### Implementation Steps
1. **Scaffold Services Package** — ✅ Completed (services directory, config, factory landed in repo)  
   - Create config + factory classes.  
   - Implement lazy properties that respect the guardrails above.

2. **Adapter Modules** — ✅ Completed (observability/quarantine/reference/schema/validation services extracted)  
   - Move existing helper logic (observability, quarantine, reference data, schema registry) into the dedicated modules.  
   - Provide utilities for registering dataset-specific hooks (e.g., schema contracts, validation rules).

3. **Refactor RVUIngestor** — ✅ Completed (RVU uses `ServiceFactory`, direct instantiations removed)  
   - Replace direct instantiations with the service factory.  
   - Update references throughout the file (e.g., `self.services.validation_engine`).

4. **Apply to Other Ingestors** — ✅ Completed (MPFS, OPPS, ZIP9 migrated to ServiceFactory)  
   - Update MPFS, OPPS, ZIP9, and any additional ingestors to use the same factory.  
   - Capture any dataset-specific overrides in their own modules or service extensions.

5. **Testing & Documentation** — ⏳ Pending  
   - Add unit tests for lazy-init paths and failure modes.  
   - Update the ingestion playbook / PRDs to describe the new service layer and guardrails.

### Success Criteria
- All ingestors construct shared services via the factory, reducing `__init__` duplication.
- Schema registration happens in a single, well-documented place without duplicate side-effects.
- Lazy initialization is covered by tests and verified in at least one ingestion run.
- Documentation reflects the new architecture, including the guardrails listed above.

---

## Detailed Completion Plan (Phase 3 Remaining Work)

### Step 4 — Extend ServiceFactory to Remaining Ingestors ✅ **COMPLETED**
- **Targets:** `cms_pricing/ingestion/ingestors/{mpfs_ingestor.py, opps_ingestor.py, cms_zip9_ingester.py}` (plus any others still hand-wiring services).
- **Tasks:**
  1. ✅ Audit each ingestor's `__init__` for duplicated service construction (validation engines, quarantine managers, observability collectors, schema bootstrap).
  2. ✅ Replace inline setups with `ServiceFactory(service_config)` usage mirroring RVU; ensure dataset-specific flags (e.g., disable reference data for ZIP9) are honoured via `ServiceConfig`.
  3. ✅ Update stage calls (`self.land`, `self.validate`, etc.) to reference `self.services.*` instead of local attributes.
  4. ✅ Run lightweight compile check (`python -m compileall cms_pricing/ingestion/ingestors/mpfs_ingestor.py` etc.) to catch missing references.
- **Deliverable:** ✅ All ingestors rely on the shared factory; plan updated with completion notes.
- **Estimated Time:** 1.5–2 hours (per ingestor updates + sanity passes). **Actual:** Completed successfully.

### Step 5 — Test Coverage & Documentation
- **Unit Tests:** Add tests covering lazy initialization, repeated access, and failure paths inside `cms_pricing/ingestion/services/tests/` (or existing ingestion test suite). Mock config permutations to ensure toggles behave.
- **Integration Check:** Execute at least one DIS pipeline run per ingestor to confirm services are wired correctly; capture any sandbox restrictions (Signal 11) in verification notes.
- **Documentation Updates:** Refresh
  - `artifacts/phase2_documentation_refresh_plan.md` follow-up items,
  - relevant PRDs/runbooks (data architecture implementation guide, ingestion playbook) to describe ServiceFactory usage and guardrails,
  - change log entry summarising the shared-services migration.
- **Estimated Time:** ~2 hours (tests + docs + validation).
- **Deliverable:** Tests committed/passing locally, documentation references updated, plan marked complete.
