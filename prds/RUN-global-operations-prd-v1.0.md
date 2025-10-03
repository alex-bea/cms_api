# Runbook: MPFS / NCCI / OPPS — Go-Live & Ops

**Status:** Draft v1.0  
**Owners:** Operations & Data Engineering  
**Consumers:** On-call Engineers, Product Ops, QA  
**Change control:** PR review + Ops sign-off

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
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
- **Alerting:** Page on (a) schema drift vs last quarter, (b) zero-row partitions per vintage, (c) CF mismatch vs pinned CY2025 value.  [oai_citation:41‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)
- **Fallback checks:** Cross-check anomalies via **PFS Look-Up Tool**; for site-neutral studies later, compare status indicators via **OPPS Addendum B**.  [oai_citation:42‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/overview?utm_source=chatgpt.com)

## C. NCCI/MUE Storage Checks

- Ensure quarterly **PTP** & **MUE** updates landed; produce diffs (new/retired pairs; MUE value changes).  [oai_citation:43‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-procedure-procedure-ptp-edits?utm_source=chatgpt.com)
- Keep **Policy Manual** link handy for rule authors.  [oai_citation:44‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-policy-manual?utm_source=chatgpt.com)

## D. OPPS (when enabled)

- Ingest **Addendum B** quarterly; confirm status indicator/APC shifts match CMS transmittals; keep join keys `(HCPCS, MOD, quarter)`.  [oai_citation:45‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates?utm_source=chatgpt.com)

## E. Test Harness Dependency (Postgres)

- **Purpose:** API and pricing suites depend on PostgreSQL types (JSONB/ARRAY). Local testing and CI must exercise those flows against a real Postgres instance rather than the default SQLite harness.
- **Local workflow:**
  1. `docker compose up -d db` (requires Docker socket access).
  2. `export TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing`.
  3. `scripts/test_with_postgres.sh tests/api/test_plans.py` (wrapper spins up DB, calls `tests/scripts/bootstrap_test_db.py`, executes pytest, and tears down).
- **CI workflow:** mirrors the script above inside the `ci-integration` pipeline; ensure runners have Docker or an ephemeral Postgres service.
- **Bootstrap script:** `tests/scripts/bootstrap_test_db.py` runs Alembic migrations and seeds mandatory fixtures; extend it when new suites need additional reference data.
- **Fallback:** If Docker is unavailable, provision a managed Postgres instance, set `TEST_DATABASE_URL`, run the bootstrap script manually, then invoke pytest.
