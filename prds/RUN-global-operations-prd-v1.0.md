# Runbook: MPFS / NCCI / OPPS — Go-Live & Ops

**Status:** Draft v1.1  
**Owners:** Operations & Data Engineering  
**Consumers:** On-call Engineers, Product Ops, QA  
**Change control:** PR review + Ops sign-off

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **DOC-test-patterns-prd-v1.0.md:** Test patterns and best practices guide
- **STD-observability-monitoring-prd-v1.0:** Monitoring and alerting procedures
- **STD-qa-testing-prd-v1.0:** Testing and validation procedures
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestion procedures
- **PRD-opps-prd-v1.0.md:** OPPS ingestion procedures

This runbook lists validation steps and ops playbook items for the new MPFS ingester, related NCCI/MUE storage, and OPPS (stub). Reference links are CMS primary sources.

## A. Go-Live Validation (do on each new drop)

1) **RVU Parity Spot-Checks (per quarter)**
- Confirm RVU quarterly artifact (e.g., **RVU25A/B/C/D**) landed and versioned.  [oai_citation:33‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)
- Randomly sample 10–20 `(HCPCS, MOD)` rows; verify published RVUs & indicators match CMS RVU pages.
- **Parity recompute (non-blocking):** compute `[(Work×GPCIw)+(PE×GPCIpe)+(MP×GPCImp)]×CF` and compare with **PFS Look-Up Tool** national amounts; check **facility vs non-facility** PE.  [oai_citation:34‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/overview?utm_source=chatgpt.com)

2) **Locality & GPCI Integrity**
- Spot-check 5 localities across different MACs against **Locality Key** and **Locality Configuration** notes (e.g., CA MSA consolidation).  [oai_citation:35‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key?utm_source=chatgpt.com)
- Confirm Work/PE/MP GPCIs present for each locality vintage.  [oai_citation:36‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/documentation?utm_source=chatgpt.com)

3) **Conversion Factor Vintage**
- Ensure `mpfs_cf_vintage` has the correct **CY 2025 CF** pinned per final rule context.  [oai_citation:37‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)

4) **API Smoke Tests**
- **Single code** (200 + correlation-id present).
- **Paged list** (pagination sane).
- **Filters** (`quarter_vintage`, `modifier`).
- **Health/stats** (freshness & volume reflect latest RVU drop).

5) **Quarter-over-Quarter Diffs**
- Generate machine-readable diff (adds/deletes, indicator flips). Manually verify 2–3 deltas against CMS release notes or updated lists.  [oai_citation:38‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)

## B. First-Month Ops

- **Cadence:** Monthly poller; quarterlies/annuals reflect real changes.  [oai_citation:39‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)  
- **Lineage/Manifests:** For each artifact, capture **URL, last-modified, checksum, size**, and discovery page (RVU list, Docs, Locality/GPCI).  [oai_citation:40‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)  
- **Source Map Audit:** After discovery jobs run, execute `python tools/verify_source_map.py` to confirm new manifests (`data/manifests/cms_rvu`, `data/scraped/mpfs/manifests`, `data/scraped/opps/manifests`) agree with the reference docs (`REF-cms-pricing-source-map-prd-v1.0.md`). Treat failures as release blockers.
- **Alerting:** Page on (a) schema drift vs last quarter, (b) zero-row partitions per vintage, (c) CF mismatch vs pinned CY2025 value.  [oai_citation:41‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)
- **Fallback checks:** Cross-check anomalies via **PFS Look-Up Tool**; for site-neutral studies later, compare status indicators via **OPPS Addendum B**.  [oai_citation:42‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/overview?utm_source=chatgpt.com)

## C. NCCI/MUE Storage Checks

- Ensure quarterly **PTP** & **MUE** updates landed; produce diffs (new/retired pairs; MUE value changes).  [oai_citation:43‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-procedure-procedure-ptp-edits?utm_source=chatgpt.com)
- Keep **Policy Manual** link handy for rule authors.  [oai_citation:44‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-policy-manual?utm_source=chatgpt.com)

## D. Release Management & CHANGELOG Discipline (Added 2025-10-17)

### D.1 CHANGELOG Format (Keep a Changelog)

**Rules:**
- **Single section per type**: Added, Changed, Deprecated, Removed, Fixed
- **No duplicate subsections**: Only ONE "### Changed" in `[Unreleased]`
- **Chronological ordering**: Newest entries first within each section
- **Issue references**: Use `(#123)` or `GH-123` format
- **Commit references**: Use `[a1b2c3d](https://github.com/owner/repo/commit/a1b2c3d)` format

**Structure:**
```markdown
## [Unreleased]

### Added
- Feature A (#123)
- Feature B (#124)

### Changed
- Component X refactored
- Component Y updated

### Deprecated
- Old API v1

### Removed
- Legacy code

### Fixed
- Bug in module Z
```

**Validation:**
```bash
# Run before every release
python tools/audit_changelog.py

# Expected: ✅ Format valid, no duplicate sections
```

### D.2 Release Workflow

**Pre-Release Checklist:**

1. **Update CHANGELOG** with all completed work
   - Add issue references for all closed items
   - Ensure proper section structure (no duplicates)
   - Date stamp the release section

2. **Sync with Project Board**
   ```bash
   # Dry run (preview)
   python3 tools/mark_tasks_done.py \
     --project-number 5 \
     --owner @me \
     --section Unreleased \
     --commits-since v1.2.0 \
     --dry-run
   
   # Execute (close issues, update board)
   python3 tools/mark_tasks_done.py \
     --project-number 5 \
     --owner @me \
     --section Unreleased \
     --commits-since v1.2.0 \
     --close-issues \
     --comment
   ```

3. **Verify CI Hygiene**
   ```bash
   # Run all checks
   python tools/md_checkbox_scan.py
   python tools/todo_lint.py
   python tools/audit_changelog.py
   ```

4. **Create Release**
   ```bash
   git tag -a v1.3.0 -m "Release v1.3.0"
   git push origin v1.3.0
   ```

### D.3 Automated Workflows

**Changelog Sync Workflow** (`.github/workflows/changelog-sync.yml`):
- **Triggers:** Push to `main` with CHANGELOG.md changes
- **Actions:** Parses `[Unreleased]`, closes issues, updates Project #5
- **Token:** Uses `PROJECT_SYNC_TOKEN` (PAT with 'project' scope)

**Setup:**
1. Create GitHub PAT with 'project' scope
2. Add as repository secret: `PROJECT_SYNC_TOKEN`
3. Workflow auto-triggers on CHANGELOG updates

### D.4 CHANGELOG Hygiene Gates

**Pre-commit hook** (enforced):
- No unchecked checkboxes in committed files
- No naked TODO comments

**CI checks** (recommended):
- CHANGELOG format validator
- Duplicate section detector
- Issue reference validator

**Release blocker criteria:**
- Duplicate "Added" or "Changed" sections
- Missing issue references for major features
- Unclosed issues referenced in `[Unreleased]`

## E. OPPS (when enabled)

- Ingest **Addendum B** quarterly; confirm status indicator/APC shifts match CMS transmittals; keep join keys `(HCPCS, MOD, quarter)`.  [oai_citation:45‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates?utm_source=chatgpt.com)

## F. Test Harness Dependency (Postgres)

- **Purpose:** API and pricing suites depend on PostgreSQL types (JSONB/ARRAY, UUID). Local testing and CI must exercise those flows against a real Postgres instance rather than the default SQLite harness.
- **Database isolation:** Each test run must use a dedicated database to prevent conflicts between app startup table creation and Alembic migrations. Use environment-specific database names: `cms_pricing_{environment}`.
- **Local workflow:**
  1. `docker compose up -d db` (requires Docker socket access).
  2. `export TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_test`.
  3. `scripts/test_with_postgres.sh tests/api/test_plans.py` (wrapper spins up DB, calls `tests/scripts/bootstrap_test_db.py`, executes pytest, and tears down).
- **CI workflow:** mirrors the script above inside the `ci-integration` pipeline; ensure runners have Docker or an ephemeral Postgres service. Use `cms_pricing_ci_{build_id}` for unique database names per CI run.
- **Bootstrap script:** `tests/scripts/bootstrap_test_db.py` runs Alembic migrations and seeds mandatory fixtures; extend it when new suites need additional reference data.
- **Test lifecycle:** Strict order of operations: (1) Environment setup with dedicated DB URL, (2) Infrastructure provisioning, (3) Schema bootstrap via Alembic only (no app table creation), (4) Test execution, (5) Cleanup.
- **Fallback:** If Docker is unavailable, provision a managed Postgres instance, set `TEST_DATABASE_URL`, run the bootstrap script manually, then invoke pytest.
