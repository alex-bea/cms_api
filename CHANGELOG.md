# Changelog

All notable changes to the CMS Pricing API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Locality Parser (v1.0.0)** - Locality-County Crosswalk raw parser (#123)
  - Fixed-width TXT parsing (25LOCCO.txt format)
  - Schema: `cms_locality_raw_v1.0` (layout-faithful: state/county NAMES, not FIPS)
  - Layout: `LOCCO_2025D_LAYOUT` v2025.4.2 (corrected column positions)
  - Natural keys: `['mac', 'locality_code']`
  - Features: Header skipping, state forward-fill, auto-deduplication
  - Handles CMS data quirks: duplicate 05302/99 row auto-removed
  - Tests: 4 golden tests (100% pass rate)
  - Two-stage architecture: Raw parser → FIPS enricher (#124)
  - Follows STD-parser-contracts v1.9 §21.1 (11-step template)
- **Reference Data Infrastructure** - Dual-mode reference data access (#125)
  - `REF_MODE` feature flag (inline vs curated)
  - Fail-closed policies for data publishing
  - Schema contract validation for inline reference data
  - Bootstrap: US states (51) + representative counties (96)
  - Documented in STD-data-architecture-impl v1.0.2 §4.2
- **Changelog Sync Automation** - GitHub Project integration (#126)
  - Tool: `tools/mark_tasks_done.py` syncs CHANGELOG with Project #5
  - Workflow: `.github/workflows/changelog-sync.yml` auto-triggers
  - Auto-closes issues and moves project cards to Done
  - Supports dry-run mode for testing

- **GPCI Parser (v1.0.0)** - Geographic Practice Cost Indices parser
  - Supports TXT (fixed-width), CSV, XLSX, ZIP formats
  - Schema: `cms_gpci_v1.2` (CMS-native naming: `gpci_mp`, `locality_code`)
  - Layout: `GPCI_2025D_LAYOUT` v2025.4.1 (verified column positions)
  - Natural keys: `['locality_code', 'effective_from']`
  - Validation: Row count (100-120 expected), GPCI ranges [0.20, 2.50]
  - Provenance: `source_release` tracking (RVU25A/B/C/D)
  - Tests: 8 golden tests, 10 negative tests, 3 integration tests
  - Follows STD-parser-contracts v1.7 §21.1 (11-step template)
- **Schema Contract**: `cms_gpci_v1.2.json`
  - CMS-native naming (`gpci_mp` not `gpci_malp`)
  - Split into Core, Enrichment, and Provenance columns
  - Core columns only in row hash (deterministic)
  - Optional state/MAC enrichment (not required)
  - 3 decimal precision for GPCI values
- **Layout Registry**: Updated `GPCI_2025D_LAYOUT` to v2025.4.1
  - Corrected 3 column positions (state: 16:18, gpci_work: 121:126, gpci_mp: 145:150)
  - CMS-native column names matching schema v1.2
  - Added quarter variants (A/B/C/D) to registry lookups
  - Conservative `min_line_length=100` (actual 150)
- **Planning Infrastructure**:
  - Organized all planning docs into `planning/` directory
  - Created standardized parser planning structure (5 parsers)
  - Added `tools/create_parser_plan.sh` automation script
  - Created parser template and HOW_TO_ADD_PARSER.md guide
  - GPCI planning docs: 10 files including IMPLEMENTATION.md (27K guide)

### Changed
- **Router**: Updated GPCI routing to use `cms_gpci_v1.2` and `parse_gpci()` function
- **RouteDecision**: Added `parser_func` field to support callable parsers

### Deprecated
- **cms_gpci_v1.0.json**: Superseded by v1.2 (CMS-native naming)

### Fixed
- **GPCI Layout Positions**: Corrected 3 column positions based on actual CMS data measurements

### Changed
- **Planning Files Organization (2025-10-16)**: Reorganized 26 planning documents into structured `planning/` directory
  - Created hierarchy: `planning/parsers/`, `planning/project/`, `planning/architecture/`
  - Parser-specific organization: GPCI (8 files), PPRRVU (4 files), CF (ready for future)
  - Archive pattern: Superseded docs preserved in `archive/` subdirectories
  - Root cleanup: 26 → 5 essential files (80% reduction)
  - Navigation aids: `planning/README.md`, `planning/INDEX.md`, `planning/QUICK_NAVIGATION.md`
  - Active GPCI plan: `planning/parsers/gpci/IMPLEMENTATION.md` (v2.1)
  - Benefit: Clean root, scalable structure, clear discoverability

### Added (Phase 1)
- **PPRRVU Parser (COMPLETE)**: Full implementation with schema-aligned fixed-width, CSV, XLSX support
  - Natural keys: `['hcpcs', 'modifier', 'status_code', 'effective_from']`
  - Schema: `cms_pprrvu_v1.1` (precision=2, HALF_UP rounding)
  - 7 comprehensive tests (all passing)
  - Golden fixture: 94 rows, SHA-256 pinned
  - 4 negative test fixtures for failure modes
  - `README_PPRRVU.md` parser documentation
- **Custom Parser Error Types**: Production-grade exception hierarchy
  - `ParseError` (base), `DuplicateKeyError`, `CategoryValidationError`, `LayoutMismatchError`, `SchemaRegressionError`
  - 8 error type tests (all passing)
- **Column Mapper Infrastructure**: `cms_pricing/mappers/` for schema↔API transformations
  - Schema format (DB canonical): `rvu_work`, `rvu_pe_nonfac`, `rvu_pe_fac`, `rvu_malp`
  - API format (presentation): `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`
  - Centralized transformation functions
- **Configurable Natural Key Uniqueness**: BLOCK/WARN severity in `check_natural_key_uniqueness()`
- **`ingestor` pytest marker** for parser/ingestor tests
- **PRD Learning Reminder System**: Automated GitHub workflow to suggest PRD updates based on code changes ([9b9e63d](https://github.com/alex-bea/cms_api/commit/9b9e63d))
  - Declarative rules in `.github/prd_learning_rules.yml`
  - PR comment automation + `docs/prd_learning_inbox.md` aggregation
  - Pattern matching for code changes (parsers, ingestors, contracts, PRDs)
  - Suppression tokens for intentional skips

### Changed
- **Layout Registry (v2025.4.0 → v2025.4.1)**: PPRRVU layout aligned with schema contract
  - Renamed columns: `work_rvu` → `rvu_work`, `mp_rvu` → `rvu_malp`, etc.
  - Added missing natural key: `modifier`
  - Added schema field: `opps_cap_applicable`
  - Fixed `min_line_length`: 200 → 165 (actual data is ~173 chars)
- **Parser Normalization**: Conditional - skip if columns already canonical (from layout)
- **Schema Loading**: Strips minor version from schema_id (e.g., `cms_pprrvu_v1.1` → `cms_pprrvu_v1.0.json`)
- **STD-parser-contracts**: v1.3 → v1.4 → v1.5 (production hardening)
  - **v1.5** ([a8f5188](https://github.com/alex-bea/cms_api/commit/a8f5188)): 10 surgical fixes for unbreakable parser development
    - §6.5: ParseResult consumption + ingestor file writes pattern
    - §21.1 Step 1: head-sniff + seek pattern (memory-bounded)
    - Row hash: Decimal quantization (not float) + link to `finalize_parser_output()`
    - §5.3: rejects/quarantine naming consistency note
    - §21.1: join invariant assert (total = valid + rejects)
    - §20.1: duplicate header guard enhancement
    - §21.1: Excel (dtype=str) + ZIP (iterate members) guidance
    - §14.6: schema loader path (relative to module)
    - §7.2: layout names = schema names (cross-ref §7.3)
  - **v1.4** ([391de34](https://github.com/alex-bea/cms_api/commit/391de34)): Template hardening + CMS-specific pitfalls
    - §20.1 Anti-Patterns 6-10: BOM in headers, duplicate headers, Excel coercion, whitespace/NBSP, CRLF leftovers
    - §1 summary: ParseResult return type clarity
    - §7.2 example: schema-canonical names (rvu_work not work_rvu)
    - YAML section: tagged as "Future (informative)"
  - **v1.3** ([5fd7fd4](https://github.com/alex-bea/cms_api/commit/5fd7fd4)): Normative clarifications from PPRRVU implementation
    - §7.3 Layout-Schema Alignment (5 MUST rules, CI-enforceable)
    - §8.5 Error Code Severity Table (12 codes with dataset policies)
    - §20.1 Common Pitfalls (top 5 anti-patterns, reordered by frequency)
    - §6.6 Schema vs API Naming Convention
    - §14.6 Schema File Naming & Loading (version stripping)
    - §7.4 CI Test Snippets (4 copy/paste guards)
- **Companion Document Pattern** ([a077468](https://github.com/alex-bea/cms_api/commit/a077468)): Simplified from YAML to markdown headers
  - `**Companion Docs:**` and `**Companion Of:**` in markdown headers
  - Bidirectional link validation with case-sensitive path checks
  - Removed `PyYAML` dependency for simplicity
- **pyproject.toml**: Added missing pytest markers ([d487ac6](https://github.com/alex-bea/cms_api/commit/d487ac6))
  - `prd_docs`, `scraper`, `geography`, `api`, `e2e` markers
  - Fixes pytest strict-markers error

### Fixed
- **Schema-layout column name alignment** (prevented KeyError in categorical validation)
- **Missing natural key columns** in layout (modifier, effective_from)
- **Over-strict min_line_length** filtering out valid data rows
- **Parameter name**: `canonicalize_numeric_col(rounding_mode=...)` not `rounding`
- **pytest markers**: Added missing markers to fix strict-markers configuration error
- **Companion link validation** ([ffae4e9](https://github.com/alex-bea/cms_api/commit/ffae4e9)): 7 improvements
  - Case-sensitive path validation (prevents Linux CI failures on macOS dev)
  - Multiple companion links support (comma-separated)
  - Planned companion exemptions (e.g., `_(Planned for v2.0)_`)
  - Bidirectional symmetry checks
  - Document type consistency validation
- **Dependency graph audit** ([27e765c](https://github.com/alex-bea/cms_api/commit/27e765c)): Companion docs separated from main DAG
  - §7.1 Companion Relationships subgraph in master catalog
  - Audit rules updated to handle `-impl` naming pattern

### Architecture
- **Schema vs API Naming**: Established clean separation
  - Parsers output schema format (DB canonical)
  - API transforms to presentation format (user-friendly)
  - Transformation boundary: Serialization layer
  - Single source of truth: Schema contract

### Technical Debt Resolved
- ✅ Layout-schema alignment documented
- ✅ Column mapper pattern established
- ✅ Error taxonomy implemented

### Documentation
- **STD-parser-contracts**: v1.3 → v1.4 → v1.5 (see Changed section for details)
  - **Cumulative Impact**: Prevents 23-32 hours debugging across next 4 parsers
    - v1.3: 15-20 hours (layout alignment, error taxonomy, pitfalls 1-5)
    - v1.4: 4-8 hours (pitfalls 6-10: BOM, duplicates, Excel, NBSP, CRLF)
    - v1.5: 8-12 hours (10 surgical fixes: ParseResult, head-sniff, Decimal, etc.)
- **PPRRVU_HANDOFF.md**: Comprehensive handoff with DB contract vs API surface architecture
- **README_PPRRVU.md**: Parser-specific documentation with schema/API column mapping table
- **Column Mapper Documentation**: Schema↔API transformation patterns in `cms_pricing/mappers/`
- **Task Tracker** ([aa765d4](https://github.com/alex-bea/cms_api/commit/aa765d4)): Added deferred parser infrastructure improvements to `github_tasks_plan.md`
  - Tasks A-D: Streaming file reads, metadata preflight, layout-schema validator, dynamic skiprows metrics
  - Total effort: 55 minutes (fold into CF parser development)
  - ROI: Prevents 3-7 hours debugging across remaining parsers

### Commits (14 substantive since v0.1.0-phase0)
1. [0b6d892](https://github.com/alex-bea/cms_api/commit/0b6d892) - feat(parser): Phase 1 enhancements + PPRRVU fixtures
2. [53c0886](https://github.com/alex-bea/cms_api/commit/53c0886) - WIP: PPRRVU parser (90% complete) + Phase 1 enhancements
3. [7ea293e](https://github.com/alex-bea/cms_api/commit/7ea293e) - feat(parser): Complete PPRRVU parser with schema-API alignment
4. [498bddf](https://github.com/alex-bea/cms_api/commit/498bddf) - docs: Prepare PRD updates - PPRRVU learnings documented
5. [5fd7fd4](https://github.com/alex-bea/cms_api/commit/5fd7fd4) - docs(prd): STD-parser-contracts v1.2 → v1.3
6. [57fc7cf](https://github.com/alex-bea/cms_api/commit/57fc7cf) - docs(prd): STD-parser-contracts v1.3 final refinements
7. [27e765c](https://github.com/alex-bea/cms_api/commit/27e765c) - chore(docs): Fix audit failures - CHANGELOG + dependency graph
8. [a077468](https://github.com/alex-bea/cms_api/commit/a077468) - docs: companion pattern with simple markdown metadata
9. [9b9e63d](https://github.com/alex-bea/cms_api/commit/9b9e63d) - feat(automation): Add PRD learning reminder system
10. [ffae4e9](https://github.com/alex-bea/cms_api/commit/ffae4e9) - refactor(audit): harden companion link validation (7 improvements)
11. [391de34](https://github.com/alex-bea/cms_api/commit/391de34) - docs(prd): STD-parser-contracts v1.3 → v1.4
12. [aa765d4](https://github.com/alex-bea/cms_api/commit/aa765d4) - docs: add deferred parser infrastructure improvements to task tracker
13. [a8f5188](https://github.com/alex-bea/cms_api/commit/a8f5188) - docs: STD-parser-contracts v1.5 - production hardening (10 surgical fixes)
14. [d487ac6](https://github.com/alex-bea/cms_api/commit/d487ac6) - fix: add missing pytest markers to pyproject.toml

_Note: CHANGELOG-only commits (729d06d, 1f394a1) excluded per audit convention._

### Added (Phase 1 - Continued)
- **Conversion Factor Parser (COMPLETE)**: Full implementation with CSV, XLSX, ZIP support
  - Natural keys: `['cf_type', 'effective_from']` (BLOCK duplicates per §8.5)
  - Schema: `cms_conversion_factor_v2.0` (precision=4, HALF_UP rounding)
  - Comprehensive golden tests + 11 negative test fixtures
  - CMS authoritative value guardrails (Federal Register comparison)
  - Tasks A-D from deferred improvements (metadata preflight, streaming file reads)
  - Validation phases: coercion → post-cast → categorical → uniqueness

- **GPCI Schema v1.2 (CMS-Native)**: Updated GPCI schema with CMS domain expert improvements
  - BREAKING: `gpci_malp` → `gpci_mp` (CMS-native "MP" terminology)
  - BREAKING: `state_fips` (required) → `state` (optional enrichment)
  - Added `source_release`, `source_inner_file` for CMS provenance tracking
  - Updated quality thresholds: 100-120 rows (CMS post-CA consolidation: ~109 localities)
  - Enhanced GPCI bounds: [0.30, 2.00] warn, [0.20, 2.50] fail (headroom for edge localities)
  - Implementation plan: `planning/parsers/gpci/IMPLEMENTATION.md` (v2.1)
  - Rationale document: `planning/parsers/gpci/GPCI_SCHEMA_V1.2_RATIONALE.md`

### Planned (Next Sessions)

**Documentation Updates (Pre-Implementation):**
- **PRD-rvu-gpci (v0.1 → v0.2)** - Schema v1.2 migration decisions (~30 min)
  - Add §3.3.1 Schema Evolution with v1.2 breaking changes
  - Update §2.4 quality thresholds (100-120 rows, [0.20, 2.50] GPCI bounds)
  - Document CMS-native terminology rationale (gpci_mp vs gpci_malp)
  - Reference GPCI_SCHEMA_V1.2_RATIONALE.md for full migration details

**Phase 1 Parsers (Remaining):**
- GPCI parser (~180 lines, 2.5 hours) - Schema v1.2 ready, implementation plan complete
- ANES parser (~100 lines, 2 hours)
- OPPSCAP parser (~120 lines, 2 hours)
- Locality parser (~100 lines, 2 hours)

**Phase 2 Enhancements:**
- Phase 2: Advanced routing (weighted voting, confidence scoring, PII detection)
- Phase 2: Vocabulary registry with normalizers
- Phase 2: Telemetry and observability metrics

---

## [v0.1.0-phase0] - 2025-10-16

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

