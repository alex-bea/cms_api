# QA Summary — CMS API Repository

| Item | Details |
| --- | --- |
| **Scope & Ownership** | CMS Pricing ingestion & API platform; Owners: Platform/Data Engineering (primary), QA Guild (shared). |
| **Test Tiers & Coverage** | `unit`, `integration`, `e2e`, `performance`, `golden`. Current focus: unit + integration for doc audits, scrapers, ingestors. Target ≥90% line coverage on scraper/ingestor cores once suites are green. |
| **Fixtures & Baselines** | Source-controlled under `tests/fixtures/` (MPFS, OPPS, RVU, ZIP9). Golden manifests maintained alongside datasets. Temporary SQLite-compatible models defined in `tests/conftest.py`. |
| **Quality Gates** | Local `pytest -m "not slow"` required before PR. CI: doc audits + fast unit suites; integration/e2e gated by missing modules — tracked below. Coverage gate to be re-enabled post-restoration of geography stack. |
| **Production Monitors** | Synthetic health probes TBD; ingestion observability using DIS dashboards (per Observability PRD §3). |
| **Manual QA** | None; automation only. |
| **Outstanding Risks / TODO** | 1) Geography/nearest-zip ingestion modules absent — suites auto-skipped via `pytest_collection_modifyitems`; follow-up tickets required. 2) Event-loop crash in focused MPFS integration run under local Python 3.12 (investigate interpreter/asyncio interplay). 3) Reintroduce coverage enforcement and DIS metadata checks once dependent modules return. |

## Marker Groups

| Marker | Description |
| --- | --- |
| `prd_docs` | Documentation audits (catalog, metadata, links). |
| `scraper` | Scraper unit/component suites (OPPS, MPFS, RVU). |
| `ingestor` | DIS ingestor tests (MPFS, OPPS, RVU, ZIP9). |
| `geography` | Geography resolver and nearest-zip suites; currently skipped until modules land. |
| `api` | REST API contract/health tests (FastAPI routers). |

Use `pytest -m <marker>` to execute a slice (e.g., `pytest -m prd_docs`, `pytest -m "scraper or prd_docs"`). Combine with existing `integration`, `performance`, and dataset-specific markers (`mpfs`, `opps`) for finer control.

## Appendix A — Test Directory Map

- `tests/prd_docs/`: Documentation catalog compliance and metadata audits.
- `tests/scrapers/`: OPPS/MPFS/RVU scraper unit, component, and performance tests.
- `tests/ingestors/`: DIS ingestion pipelines (MPFS, OPPS, RVU, ZIP9) plus shared ingestion utilities.
- `tests/geography/`: Geography resolver, nearest-zip, and state boundary suites (temporarily skipped until modules return).
- `tests/api/`: FastAPI contract, health, and golden parity tests.

Use these paths with the corresponding markers (e.g., `pytest tests/ingestors -m ingestor`) to target a slice of the suite.
