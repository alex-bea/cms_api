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
1. **Scaffold Services Package**  
   - Create config + factory classes.  
   - Implement lazy properties that respect the guardrails above.

2. **Adapter Modules**  
   - Move existing helper logic (observability, quarantine, reference data, schema registry) into the dedicated modules.  
   - Provide utilities for registering dataset-specific hooks (e.g., schema contracts, validation rules).

3. **Refactor RVUIngestor**  
   - Replace direct instantiations with the service factory.  
   - Update references throughout the file (e.g., `self.services.validation_engine`).

4. **Apply to Other Ingestors**  
   - Update MPFS, OPPS, ZIP9, and any additional ingestors to use the same factory.  
   - Capture any dataset-specific overrides in their own modules or service extensions.

5. **Testing & Documentation**  
   - Add unit tests for lazy-init paths and failure modes.  
   - Update the ingestion playbook / PRDs to describe the new service layer and guardrails.

### Success Criteria
- All ingestors construct shared services via the factory, reducing `__init__` duplication.
- Schema registration happens in a single, well-documented place without duplicate side-effects.
- Lazy initialization is covered by tests and verified in at least one ingestion run.
- Documentation reflects the new architecture, including the guardrails listed above.
