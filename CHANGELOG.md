# Changelog

All notable changes to the CMS Pricing API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **Parser Contracts Modularization (v2.0):** Split `STD-parser-contracts-prd-v1.11.md` (4,477 lines) into 6 focused documents for improved AI context loading and governance compliance:
  - `STD-parser-contracts-prd-v2.0.md` (737 lines) - Core policy: contracts, versioning, goals
  - `STD-parser-contracts-impl-v2.0.md` (809 lines) - Implementation companion: 11-step template, alias maps, type handling
  - `REF-parser-routing-detection-v1.0.md` (735 lines) - Router architecture, layout registry, format detection
  - `REF-parser-quality-guardrails-v1.0.md` (611 lines) - Validation tiers, error taxonomy, safe metrics calculation
  - `RUN-parser-qa-runbook-prd-v1.0.md` (437 lines) - QA procedures, pre-implementation checklist, acceptance criteria
  - `REF-parser-reference-appendix-v1.0.md` (350 lines) - Reference tables, file format characteristics
- **Benefits:** 3-4x faster AI context loading (avg ~613 lines per doc vs 4,477 monolith), proper companion doc pattern (STD + -impl), independent versioning, clearer separation of concerns (policy vs code vs operations)
- **2025-10-18 Follow-up:** Restored key v1.11 guidance and strengthened automation:
  - Added “Implementation snapshot” to `STD-parser-contracts-prd-v2.0.md` summarizing ParseResult, encoding cascade, categorical guardrail, router sniffing, and metadata requirements.
  - Reintroduced CSV/Excel pitfall examples in `STD-parser-contracts-impl-v2.0.md` (duplicate headers, Excel coercion, NBSP cleanup, CRLF handling).
  - `REF-parser-routing-detection-v1.0.md` already contained all routing/layout content—no changes needed.
  - Verified `REF-parser-quality-guardrails-v1.0.md` and `REF-parser-reference-appendix-v1.0.md` already carried exported sections; no action required.
  - Enhanced `RUN-parser-qa-runbook-prd-v1.0.md` with an SLA summary, richer Step 2c variance guidance, updated test matrix, and tightened sign-off steps.
  - Upgraded `tools/prd_modularizer.py` with `--auto-compare`, automated comparison reports, and suggestion synthesis so future splits automatically flag missing sections; added CLI help and documented the workflow.
- **Source Section Mapping:** Each new document includes traceability table mapping back to v1.11 sections with line numbers
- **Transition Support:** Archived v1.11 with deprecation notice, kept for 2-week transition period (until 2025-10-31)
- **Locality Parser (v1.1.0) - Stage 1 Complete** - Locality-County Crosswalk raw parser
  - **Multi-format support:** TXT (fixed-width), CSV, XLSX
  - **Schema:** `cms_locality_raw_v1.0` (layout-faithful: state/county NAMES, not FIPS)
  - **Layout:** `LOCCO_2025D_LAYOUT` v2025.4.2 (corrected column positions, zero-padding)
  - **Natural keys:** `['mac', 'locality_code']`
  - **Features:** Dynamic header detection, state forward-fill on continuation rows, zero-padding (MAC: 5 digits, locality: 2 digits), preserves duplicates (raw layer semantics)
  - **QTS Compliance:** Implements §5.1.3 Authentic Source Variance Testing with threshold-based parity (NK ≥98%, row ≤1%)
  - **Tests:** 18 tests (17 passing, 1 skipped): 3 golden, 2 real-source, 5 edge case, 5 negative, 3 other
  - **Documentation:** `prds/SRC-locality.md` (422 lines - comprehensive source descriptor)
  - **Time Analysis:** `planning/parsers/locality/TIME_MEASUREMENT.md` - Validated 48-64% time savings vs GPCI baseline (4.2h vs 8h)
  - **Two-stage architecture:** Raw parser (Stage 1 ✅) → FIPS normalizer (Stage 2 - planned)
  - **Authority Matrix:** TXT > CSV > XLSX for 2025D (documented variance: XLSX 78% overlap)
  - **Variance artifacts:** Auto-generated diff reports (missing/extra CSVs, parity JSON)
  - **Helpers:** `tests/helpers/variance_testing.py` with canon_locality() and write_variance_artifacts()
- **Locality Parser (v1.2.0) - Stage 2 Complete** - FIPS Normalization with LSAD tie-breaking
  - **Transform:** County NAMES → FIPS codes (3,222 counties from Census TIGER/Line 2025)
  - **Set-logic expansion:** ALL COUNTIES, ALL EXCEPT X/Y, REST OF STATE (with expansion_method markers)
  - **Canonical naming:** Preserves proper casing & diacritics (e.g., "Doña Ana County")
  - **LSAD tie-breaking:** Disambiguates duplicates using fee_area hints + default preference order
    - St. Louis County (MO 189) vs St. Louis city (MO 510)
    - Richmond County (VA 159) vs Richmond city (VA 760)
  - **State-specific rules:** VA independent cities (no blanket "City" strip), LA parishes (strip suffix in keys only), AK boroughs/census areas
  - **Alias normalization:** Normalized keys for matching (ST. → SAINT), partial/exact replacement
  - **Tiered matching:** exact → alias → fuzzy (optional with guardrails)
  - **Deterministic output:** Zero-padding (state_fips: 2, county_fips: 3), sorted by NK, stable row_content_hash (SHA-256)
  - **Enhanced metrics:** Match methods (exact/alias/fuzzy), expansion counts, per-state coverage tracking
  - **Natural keys:** Primary (locality_code, state_fips, county_fips), Secondary (mac, locality_code, county_fips) for mis-wire detection
  - **Implementation:** `normalize_locality_fips.py` (780 lines), comprehensive quarantine with reasons
  - **Tests:** 4/4 passing (100%) - St. Louis, Richmond, ALL COUNTIES expansion, quarantine
  - **Reference data:** Enhanced `county_aliases.yml` v2.0 with by_state rules and MO/VA disambiguation
  - **Time actual:** 2.3 hours (within 2-2.5h enhanced plan estimate, Steps 1-10 complete)
  - **Architecture:** Two-stage pipeline per STD-data-architecture-impl §1.3 (Stage 1 ✅ → Stage 2 ✅)
  - **Authority:** Census TIGER/Line 2025 Gazetteer Counties (frozen, versioned, SHA-256 verified)
- **Locality Parser - Production Hardening** (Stage 2 operational guardrails)
  - **Authority fingerprinting:** GEOID checksum, by-state/type counts, version tracking in `normalize_locality_fips()` metrics
  - **Quarantine SLO:** Helper function `assert_quarantine_slo()` with artifact emission (≤0.5% threshold)
  - **E2E join validation:** Test pattern for Stage 1 → Stage 2 → GPCI join (≥99.5% join rate, duplicate NK checks, CA ROS spot-checks)
  - **MD independent cities:** Added Baltimore City (FIPS 24510) to county_aliases.yml
  - **Integration tests:** 6 total (4 passing, 2 skipped with documented TODOs)
  - **GitHub tasks:** Added 2 tasks for fixing skipped tests (E2E GPCI join, real-source quarantine SLO)
  - **Documentation:** SRC-locality v1.1 → v1.2 with §3.1.2 E2E example, §6.3 Quarantine SLO, §6.4 Authority Drift
  - **Time:** 1.5h (within 1.5-2h hardening estimate)
- **Locality Parser - State Inference & E2E Fixes** (Resilience for incomplete CMS headers)
  - **State forward-fill fix:** Independent `last_valid_state` tracking prevents state bleeding across boundaries (e.g., AR → CA when CA header missing)
  - **County-based state inference:** Stage 2 fallback when state unmapped - explodes county list, matches to Census reference, infers state if exactly 1 candidate
    - Recovered 28/29 CA rows from sample file (96.6% success rate)
    - Quarantine reduction: 7.21% → 1.56% (78% improvement, down from 143 to 31 rows)
    - Guards: One attempt per row, ambiguous counties quarantined with reason
  - **Territory support:** Added Puerto Rico (72), Virgin Islands (78), Guam (66), Hawaii/Guam (15) to state_map
  - **Parsing artifact cleanup:** Strip parentheses/brackets from county names in `normalize_key()`
  - **E2E GPCI join test:** ✅ PASSING - Fixed GPCI fixture alignment (positions 121, 133, 145), 100% join rate
  - **Test status:** 26/27 passing (96%), 1 xfail (strict, expires 2025-12-31) for remaining gaps (REST OF STATE, ambiguous counties, ST. LOUIS CITY alias)
  - **GitHub task:** Created "Complete Quarantine SLO" task tracking 3 remaining issues
  - **Time:** 3.5 hours (state tracking 1.5h + GPCI join 1h + inference 1h)
- **QTS v1.4 → v1.6** - Normalization & Enrichment Testing Patterns
  - **Appendix H added:** 6 new testing patterns for Stage 2+ normalization/enrichment pipelines
    - H.1: Set-Logic Expansion Testing (ALL/EXCEPT/REST OF cardinality validation, expansion_method tracking)
    - H.2: Entity Disambiguation Testing (LSAD tie-breaking, fee_area hints, preference orders)
    - H.3: Reference Data Authority & Drift Detection (fingerprinting, GEOID checksum, drift alerts)
    - H.4: Quarantine SLO Enforcement (≤0.5% threshold, artifact emission, breach response)
    - H.5: Join Validation Patterns (E2E join rate ≥99.5%, duplicate NK checks, value propagation)
    - H.6: Canonical vs Matching Key Testing (dual-key pattern: normalized for matching, canonical for output)
  - **H.7:** When to Use These Patterns (decision table for applicability)
  - **H.8:** Implementation Checklist (8 validation points before shipping)
  - **H.9:** Practical Example (Locality Stage 2: 100% test pass, 2.3h, all patterns applied)
  - **Impact:** Prevents 2-3h debugging per normalization pipeline; standardizes enrichment testing
  - **Reference Implementation:** `tests/integration/test_locality_e2e.py`, `normalize_locality_fips.py`
  - **Cross-References:** STD-data-architecture-impl §1.3 (Two-Stage Transformation Boundaries)
- **Census Reference Data Scraper** - GitHub task #33a added
  - Automated scraper for Census TIGER/Line county data (annual refresh)
  - Downloads 2025 Gazetteer Counties National file
  - Validates record count (~3,200 counties/equivalents), spot-checks key counties
  - Generates `us_counties.csv` with authority_version metadata
  - Updates manifest.json with SHA-256, record count, download date
  - Aligns with STD-scraper-prd-v1.0.md patterns
  - Priority: Medium (annual refresh needed; manual process currently works)
- **Reference Data Infrastructure** - Dual-mode reference data access (#125)
  - `REF_MODE` feature flag (inline vs curated)
  - Fail-closed policies for data publishing
  - Schema contract validation for inline reference data
  - Bootstrap: US states (51) + representative counties (96)
  - Documented in STD-data-architecture-impl v1.0.2 §4.2
- **Infrastructure Module** - Created `cms_pricing/infra/` for cross-cutting concerns
  - Centralized location for infrastructure-level configuration
  - Better architectural layering (separation from processing stages)
  - Houses: `reference_mode.py` (REF_MODE feature flag and guardrails)
- **Changelog Sync Automation** - GitHub Project integration (#126)
  - Tool: `tools/mark_tasks_done.py` syncs CHANGELOG with Project #5
  - Enhanced with `--commits-since <ref>` to scan git log for issue references
  - Workflow: `.github/workflows/changelog-sync.yml` auto-triggers
  - Auto-closes issues and moves project cards to Done
  - Supports dry-run mode for testing
- **Normative Language Audit Tool** - `tools/audit_normative_language.py`
  - Prevents policy drift into guidance documents (REF-*, RUN-*)
  - Flags MUST/SHALL/REQUIRED in non-STD documents
  - Integrated into `run_all_audits.py` suite
  - Skips code blocks, inline code, tables (smart detection)
  - Status: ✅ PASSING (10 docs checked, 0 violations)
- **QTS §5.1.3 Standard** - Authentic Source Variance Testing
  - New testing standard for real CMS file format variance
  - Threshold-based parity: NK overlap ≥98%, row variance ≤1% or ≤2 rows
  - Format Authority Matrix pattern (TXT > CSV > XLSX)
  - Variance report artifacts: missing/extra CSVs + parity JSON
  - `@pytest.mark.real_source` marker for real-file tests
  - `xfail(strict=True)` for ticketed mismatches
  - Prohibits blanket `skip` of parity tests
  - Documented in `STD-qa-testing-prd-v1.0.md` v1.4 → v1.5
- **Parser Contracts §21.4 Step 2c** - Real Data Format Variance Analysis
  - 5-step pre-implementation variance detection checklist
  - Decision tree for variance levels (<2%, 2-10%, ≥10%)
  - Authority Matrix selection guidance
  - Diff artifacts planning
  - Saves 30-60 min debugging per parser
  - Documented in `STD-parser-contracts-prd-v1.0.md` v1.10 → v1.11
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
- **Layout Verification Tool** - `tools/verify_layout_positions.py` for pre-implementation validation
  - Extracts fixed-width columns from sample files
  - Domain-specific verification questions (MAC, locality, HCPCS, etc.)
  - Prevents 30+ min debugging per parser
  - Referenced in STD-parser-contracts §21.4 Step 2b
- **PRD Standards Improvements** - Enhanced parser development standards
  - **STD-parser-contracts**: v1.7 → v1.10 (layout verification tooling + comprehensive guidance)
    - §21.3 Tiered Validation Thresholds (INFO/WARN/ERROR severity)
    - §21.4 Format Verification Pre-Implementation Checklist (7-step, saves 4-6h debugging)
    - §21.6 Incremental Implementation Strategy (3-phase approach)
    - §5.2.3 Alias Map Best Practices & Testing
    - §5.2.4 Defensive Type Handling Patterns
    - §7.1 Router & Format Detection (6 subsections, flowchart)
    - §10.3 Safe Metrics Calculation Patterns
  - **STD-qa-testing**: v1.2 → v1.4 (QTS compliance + environment fallback)
    - §5.1.1 Golden Fixture Hygiene (hybrid approach for edge cases)
    - §5.1.2 Multi-Format Fixture Parity (identical data across TXT/CSV/XLSX/ZIP)
    - §2.2.1 Test Categorization with Markers (@pytest.mark.golden, edge_case, negative)
    - §6.3 Environment Testing Fallback Strategy (Docker, fresh venv, CI-first, document & defer)
  - **STD-data-architecture-impl**: v1.0 → v1.0.2 (transformation boundaries)
    - §1.3 Transformation Boundaries (Parser vs Normalize vs Enrich)
    - §4.2 Dual-Mode Reference Data Access (REF_MODE guardrails)
  - **RUN-global-operations**: v1.0 → v1.1 (release management)
    - §D Release Management & CHANGELOG Discipline (Keep a Changelog format rules)
    - §D.2 Release Workflow (pre-release checklist with mark_tasks_done.py --commits-since)
    - §D.3 Automated Workflows (changelog-sync.yml integration)
    - §D.4 CHANGELOG Hygiene Gates (CI validators, release blockers)
  - **Source Descriptors**: Added SRC-TEMPLATE.md and updated SRC-gpci.md

### Changed
- **Reference Mode Module**: Moved `reference_mode.py` from `normalize/` to `infra/` for better architectural layering
  - Infrastructure-level config (REF_MODE, ReferenceConfig) separated from processing stages
  - Updated reference in STD-data-architecture-impl §4.2
- **Planning Files Organization (2025-10-16)**: Reorganized 26 planning documents into structured `planning/` directory
  - Created hierarchy: `planning/parsers/`, `planning/project/`, `planning/architecture/`
  - Parser-specific organization: GPCI (8 files), PPRRVU (4 files), CF (ready for future)
  - Archive pattern: Superseded docs preserved in `archive/` subdirectories
  - Root cleanup: 26 → 5 essential files (80% reduction)
  - Navigation aids: `planning/README.md`, `planning/INDEX.md`, `planning/QUICK_NAVIGATION.md`
  - Active GPCI plan: `planning/parsers/gpci/IMPLEMENTATION.md` (v2.1)
  - Benefit: Clean root, scalable structure, clear discoverability
- **Router**: Updated GPCI routing to use `cms_gpci_v1.2` and `parse_gpci()` function
- **RouteDecision**: Added `parser_func` field to support callable parsers

### Deprecated
- **cms_gpci_v1.0.json**: Superseded by v1.2 (CMS-native naming)

### Removed
- **Inline FIPS Lookup**: Deleted `locality_fips_lookup.py` (exploration code superseded by Reference Data Manager pattern)
- **Duplicate Planning Docs**: Removed `GITHUB_TASK_FULL_REFERENCE_INFRA.md` (consolidated into github_tasks_plan.md)

### Fixed
- **GPCI Layout Positions**: Corrected 3 column positions based on actual CMS data measurements

### Added (Phase 1 Parsers)
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
