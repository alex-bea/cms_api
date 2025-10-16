# Changelog

All notable changes to the CMS Pricing API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added (Phase 1 - In Progress)
- Custom parser error types: `ParseError`, `DuplicateKeyError`, `CategoryValidationError`, `LayoutMismatchError`, `SchemaRegressionError`
- Configurable natural key uniqueness severity (BLOCK/WARN) in `check_natural_key_uniqueness()`
- PPRRVU parser implementation (90% complete, debugging fixed-width parsing)
- PPRRVU golden fixture (94 rows, SHA-256: `b4437f4534b999e1764a4bbb4c13f05dc7e18e256bdbc9cd87e82a8caed05e1e`)
- 4 negative test fixtures for parser validation (bad_layout, bad_dup_keys, bad_category, bad_schema_regression)
- 7 comprehensive PPRRVU tests (written, debugging in progress)
- `README_PPRRVU.md` parser-specific documentation
- `ingestor` pytest marker for parser/ingestor tests

### Changed
- `check_natural_key_uniqueness()` enhanced with configurable `severity` parameter (BLOCK/WARN)
- Schema loading logic strips minor version from schema_id (e.g., `cms_pprrvu_v1.1` → `cms_pprrvu_v1.0.json`)
- `pyproject.toml` pytest markers updated to include `ingestor`

### In Progress
- PPRRVU parser fixed-width parsing (KeyError: 'hcpcs' - debugging column mapping)
- Layout registry integration (parameter order resolved, column creation debugging)
- Test suite validation (7 tests written, awaiting parser fix)

### Technical Debt
- Layout registry API needs documentation (signature, lookup patterns, fallback behavior)
- Schema file naming convention should be in STD-parser-contracts PRD
- Fixed-width parsing helpers should be extracted to parser kit for reuse

### Planned (Next Sessions)
- Complete PPRRVU parser debugging
- Conversion Factor parser
- GPCI, ANES, OPPSCAP, Locality parsers
- Phase 2: Advanced routing (weighted voting, confidence scoring, PII detection)
- Phase 2: Vocabulary registry with normalizers
- Phase 2: Telemetry and observability metrics

---

## [0.1.0-phase0] - 2025-10-16

**Phase 0: Parser Infrastructure Foundation**

Complete production-grade infrastructure for CMS data parsing with deterministic outputs, comprehensive validation, and full provenance tracking.

### Added

#### Core Infrastructure
- **RouteDecision NamedTuple**: Type-safe routing result with schema-driven natural keys ([a4a1878](https://github.com/alex-bea/cms_api/commit/a4a1878))
- **ValidationResult NamedTuple**: Structured validation output with metrics and rejects ([05102c2](https://github.com/alex-bea/cms_api/commit/05102c2))
- **ParseResult NamedTuple**: Standard parser return type (data, rejects, metrics)
- **Parser Kit** (`cms_pricing/ingestion/parsers/_parser_kit.py`): 17+ shared utilities preventing code drift

#### Validation & Type Safety
- **ValidationSeverity enum**: BLOCK/WARN/INFO severity levels (type-safe, prevents string typos)
- **CategoricalRejectReason enum**: Machine-readable codes (CAT_UNKNOWN_VALUE, CAT_NULL_NOT_ALLOWED, etc.)
- **Zero silent NaN coercion**: Explicit validation before pandas categorical conversion (proven by test)
- **Spec-driven categorical validation**: Reads enum/nullable from schema contracts

#### Schema Contracts (10 files enhanced)
- **natural_keys field**: Single source of truth for sort order per dataset
  - PPRRVU: `["hcpcs", "modifier", "effective_from"]`
  - Conversion Factor: `["cf_type", "effective_from"]` (unique!)
  - GPCI: `["locality_code", "effective_from"]`
  - Locality: `["locality_code", "state_fips", "county_fips"]` (composite 3-col key)
- **hash_spec_version**: "1" (formal row hash specification)
- **column_order**: Explicit ordering for deterministic hashing
- **hash_metadata_exclusions**: Columns excluded from content hash
- **precision/rounding**: Per-column decimal places for numeric fields
  - PPRRVU RVUs: 2 decimals
  - GPCI indices: 3 decimals
  - Conversion factors: 4 decimals

#### Hashing & Determinism
- **64-char SHA-256 row content hash**: Full digest (not truncated) for collision avoidance
- **Schema-driven precision**: Per-column decimal places prevent float drift
- **Vectorized hashing**: 100x+ speedup (10K rows: 12ms vs 1200ms) ([17a9443](https://github.com/alex-bea/cms_api/commit/17a9443))
- **row_id computation**: SHA-256 of natural keys for deduplication and lineage
- **Deterministic reject ordering**: Sorted by (column, reason_code, row_id) for reproducible diffs

#### Router & Content Detection
- **file_head parameter**: Optional first 8KB for magic byte/BOM detection ([01c4e58](https://github.com/alex-bea/cms_api/commit/01c4e58))
- **Content sniffing**: Fixed-width vs CSV detection, BOM markers
- **Schema-driven natural keys**: Fetched from schema contracts (prevents router/data-model coupling)
- **Graceful degradation**: Quarantines on schema load failure

#### Parser Kit Utilities
- `compute_row_hashes_vectorized()`: 100x faster than row-wise apply
- `build_precision_map()`: Extract precision from schema contracts
- `canonicalize_numeric_col()`: Deterministic rounding per schema
- `detect_encoding()`: UTF-8 → CP1252 → Latin-1 cascade with BOM detection
- `compute_row_id()`: SHA-256 of natural key values
- `check_natural_key_uniqueness()`: Duplicate detection with NATURAL_KEY_DUPLICATE reason
- `enforce_categorical_dtypes()`: Spec-driven validation, zero silent NaN coercion
- `get_categorical_columns()`: Extract categorical specs from schema
- `finalize_parser_output()`: Hash + sort + metadata injection

#### Testing Infrastructure
- **25 tests total**: 17 unit tests + 8 integration tests ([3b1f1c9](https://github.com/alex-bea/cms_api/commit/3b1f1c9))
- **Golden fixtures**: SHA-256 pinned test data (`tests/fixtures/phase0/`)
  - `pprrvu_sample.csv`: 3 valid rows
  - `pprrvu_with_dupes.csv`: Duplicate detection
  - `pprrvu_invalid_modifier.csv`: Categorical rejects
  - `cf_sample.csv`: Conversion factor unique keys
- **Determinism tests**: Frozen time (freezegun), stable sort, identical hashes
- **Performance micro-budgets**: Route 20ms, validate 300ms, E2E soft guard 1.5s
- **Join invariant verification**: len(valid) + len(rejects) == len(input)

#### Audit & Quality Tools
- **audit_code_patterns.py**: Detects stale function signatures and deprecated patterns
- **validate_schema_contracts.py**: Enforces schema contract compliance (precision, hash spec, etc.)

#### Documentation
- **STD-parser-contracts-prd-v1.0.md v1.1**: Comprehensive parser contracts standard
  - ParseResult specification
  - Router contracts with file_head
  - Row hash specification (64-char SHA-256, schema-driven precision)
  - Categorical validation patterns
  - Metadata injection requirements
- **STD-data-architecture-impl-v1.0.md v1.0.1**: ParseResult integration patterns
- **REF-scraper-ingestor-integration-v1.0.md v1.0.1**: ParseResult cross-references
- **DOC-master-catalog-prd-v1.0.md**: Parser standard registered

### Changed
- **IngestRun model**: Enhanced with 65 fields for five pillar metrics
  - Freshness, Volume, Schema, Quality, Lineage
  - Stage timings, results, outcomes
  - Cost tracking, error handling, compliance
- **Layout registry**: Moved to `parsers/layout_registry.py` with SemVer (v2025.Q.P format)
- **Vintage extractor**: Centralized metadata extraction utility

### Dependencies
- Added `freezegun==1.4.0` for deterministic time testing
- Added `psutil==5.9.6` for performance monitoring

### Performance
- **Vectorized row hashing**: 100x+ speedup (10K rows: 12ms vs 1200ms)
- **Router latency**: < 20ms p95 (tested)
- **Categorical validation**: < 300ms p95 for 10K rows (tested)
- **E2E pipeline**: Soft guard at 1.5s (non-flaky CI)

### Security
- CP1252 encoding support for Windows-encoded CMS files
- BOM detection and stripping
- Content sniffing for format validation

### Fixed
- Removed duplicate `enforce_categorical_dtypes()` function
- Removed old RVU parsing logic (`cms_pricing/ingestion/rvu.py`)
- Fixed test imports after code reorganization

### Commits (5 production-grade commits)
1. [17a9443](https://github.com/alex-bea/cms_api/commit/17a9443) - feat(parser-kit): 64-char hash + schema-driven precision + vectorization
2. [01c4e58](https://github.com/alex-bea/cms_api/commit/01c4e58) - feat(parser-routing): Add content sniffing with file_head parameter
3. [a4a1878](https://github.com/alex-bea/cms_api/commit/a4a1878) - feat(parser-routing): Schema-driven natural keys + row_id + RouteDecision
4. [05102c2](https://github.com/alex-bea/cms_api/commit/05102c2) - feat(parser-validation): Spec-driven categorical validation with enums + reason codes
5. [3b1f1c9](https://github.com/alex-bea/cms_api/commit/3b1f1c9) - test(parser-integration): End-to-end Phase 0 pipeline integration tests

### References
- [STD-parser-contracts-prd-v1.0.md](prds/STD-parser-contracts-prd-v1.0.md) (v1.1)
- [STD-data-architecture-prd-v1.0.md](prds/STD-data-architecture-prd-v1.0.md)
- [STD-data-architecture-impl-v1.0.md](prds/STD-data-architecture-impl-v1.0.md) (v1.0.1)
- [REF-scraper-ingestor-integration-v1.0.md](prds/REF-scraper-ingestor-integration-v1.0.md) (v1.0.1)
- [PRD-mpfs-prd-v1.0.md](prds/PRD-mpfs-prd-v1.0.md)
- [DOC-master-catalog-prd-v1.0.md](prds/DOC-master-catalog-prd-v1.0.md)

---

## [0.0.1] - 2025-10-01

### Added
- Initial project setup
- Basic MPFS, RVU, OPPS data models
- FastAPI application structure
- PostgreSQL database with Alembic migrations
- Geography and ZIP code mapping
- Basic API endpoints

---

[Unreleased]: https://github.com/alex-bea/cms_api/compare/v0.1.0-phase0...HEAD
[0.1.0-phase0]: https://github.com/alex-bea/cms_api/compare/v0.0.1...v0.1.0-phase0
[0.0.1]: https://github.com/alex-bea/cms_api/releases/tag/v0.0.1

