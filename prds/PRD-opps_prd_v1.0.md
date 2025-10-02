# PRD: OPPS Addendum B Ingest


Overview

Build a DIS-compliant ingester for CMS Hospital Outpatient Prospective Payment System (OPPS) quarterly releases, publishing: APC payment rates, HCPCS→APC mapping (Status Indicators), and an enriched curated view that applies the IPPS wage index via CCN→CBSA. Handles Q1–Q4 each year, retains full history and corrections, with 5-pillar observability and quarantine. Addendum A/B are the canonical quarterly snapshots; I/OCE and HCPCS updates provide validation context.  ￼

Status: Draft for approval
Owners: Platform/Data Eng (primary), Medicare SME (review)
Consumers: Pricing engine, analytics, data science, ops
Change control: ADR + Data Architecture Board sign-off

⸻

Goals & Non-Goals

Goals
	•	Ingest all quarterly OPPS releases (history + prospective), including corrections (AR/CR) as new batches.  ￼
	•	Maintain HCPCS⇄APC mapping (Status Indicator) with complete modifier coverage, and publish APC payment tables.  ￼
	•	Align with DIS stages, 5-pillar observability, bitemporal versioning, and quarantine on rule fail.
	•	Provide wage-index-enriched curated views using IPPS wage index rules.  ￼

Non-Goals
	•	OPPS claim adjudication or pricer emulation.
	•	Downstream premium/pricing product logic.
	•	Non-CMS sources (e.g., commercial schedules, ASC addenda—unless explicitly added later).  ￼

⸻

Scope & Assumptions

In scope data products
	•	APC payment rates (Addendum A context).
	•	HCPCS→APC crosswalk with Status Indicators (Addendum B).
	•	Wage index reference + enriched curated view (CCN→CBSA→wage index).  ￼

Assumptions
	•	Quarterly cadence; files may be CSV/XLS/XLSX/TXT, sometimes zipped.  ￼
	•	Environment provides DIS-compliant pipeline, allow-listed CMS domains (we will throttle and checksum; robots are not honored per business directive—see Risks).
	•	Dependencies: schema registry, wage index reference, SI lookup (from rule appendices/I/OCE notes).  ￼

⸻

Data Sources

Source	What	Cadence	Key Fields	Notes
OPPS Quarterly Addenda Updates	Addendum A/B (snapshot of HCPCS, SI, APC, OPPS rates)	Quarterly	HCPCS, Modifier, SI, APC, payment/weight	Canonical landing page for A/B and corrections; reflects OPPS Pricer changes.  ￼
Addendum B page (example)	Quarter-specific B listing	Quarterly	Same as above	Confirms publish/updated dates for specific quarter.  ￼
I/OCE Quarterly Release Files	Specs & edit notes	Quarterly	Code adds/deletes, SI changes	Use for validation cues and expected deltas.  ￼
HCPCS Quarterly Update	Official HCPCS code set updates	Quarterly	HCPCS, effective dates, descriptors	Validate existence/effective dating of HCPCS in B.  ￼
IPPS Wage Index	Annual wage index tables	Annual	CBSA, CCN, wage index	Reference for enrichment rules.  ￼
OPPS Program Page	Context on quarterly retro corrections	Quarterly	N/A	Confirms retro corrections cadence (e.g., ASP-based drugs).  ￼
AMA CPT Licensing	Licensing terms for CPT content	Ad hoc	N/A	CPT® usage generally requires AMA license beyond CMS-program publications.  ￼


⸻

Architecture & Ingestion Lifecycle

DIS Stages: Land → Validate → Normalize → Enrich → Publish
	•	Land: Discover on OPPS Quarterly Addenda page; download A/B (and any ZIP/TXT); store with checksums & manifest (opps_YYYYqN_rNN).  ￼
	•	Validate: Structural (presence, headers), schema (columns/types), domain (HCPCS/APC/SI patterns, ranges), cross-file checks (HCPCS existence, A↔B linkage), temporal windows. Use I/OCE notes for spot validations.  ￼
	•	Normalize: Canonical schemas with effective_from (quarter start) and effective_to (day before next quarter).
	•	Enrich: Join provider CCN → CBSA → wage index via reference tables; publish curated enriched view (denormalized view or materialization).  ￼
	•	Publish: Parquet tables in /curated/opps/... + views (opps_rates_enriched, opps_hcpcs_crosswalk).

Staging layout:
/raw/opps/{opps_YYYYqN_rNN}/... → /stage/opps/{...} → /curated/opps/{...}

⸻

Data Model & Schema Contracts

Core Tables
	1.	curated.opps_apc_payment
	•	Keys: (year, quarter, apc); attributes: payment rate, relative weight, packaging flags, etc.
	2.	curated.opps_hcpcs_crosswalk
	•	Keys: (year, quarter, hcpcs, modifier); attributes: status_indicator, apc, payment context.
	3.	ref.wage_index_ipps (annual)
	•	Keys: (fy, cbsa); attributes: wage index, occ-mix factors (as available).  ￼
	4.	ref.si_lookup
	•	Keys: status_indicator; attributes: human-readable label/description (sourced from rule appendices/I/OCE tables).  ￼

Contracts & Versioning
	•	JSON schema contracts per table; bitemporal (effective_from/to, published_at); batch_id maps to opps_YYYYqN_rNN.
	•	All modifiers retained where present in B; SI kept as attribute (not in PK).  ￼

⸻

Release & Versioning Strategy
	•	Quarterly releases: name batches opps_YYYYqN_rNN (rNN increments for corrections).
	•	Corrections: ingest as new batches; prior batches remain immutable.
	•	Bitemporality: quarter-based effective periods; published_at stamps the CMS publication/update date; views resolve latest valid record for a query date.  ￼

⸻

Validation & Quarantine

Structural: required files present (A & B), non-zero rows, header/delimiter checks, min row thresholds.  ￼
Schema: required columns, types, ranges (rates ≥ 0; weights > 0); patterns (HCPCS [A-Z0-9]{5}; APC id format).
Domain/Cross-file:
	•	HCPCS exists for quarter (HCPCS update).  ￼
	•	APC referenced in B exists in A for that quarter (linkage).
	•	SI values valid (lookup).  ￼
	•	Wage index joinable for covered providers/areas.  ￼
Temporal: no overlapping effective ranges per (hcpcs, modifier) within a batch.
Quarantine: on fail, move artifact & sample rows to /quarantine/opps/{batch} with rule id, file digest, and counts; block publish.

⸻

Delta Checks (Configured Thresholds)
	•	Row-count delta vs prior quarter per SI/APC bucket (e.g., ±10% warn / ±25% fail).
	•	Rate-bounded delta (e.g., >±30% warn / >±60% fail; exclude ASP drug edges unless allow-listed).
	•	Coverage delta (% HCPCS with valid APC/SI): −2pp warn / −5pp fail.
	•	Key movement (HCPCS moving between APC/SI): log top-N; fail on impossible SI transitions (allowlist informed by I/OCE).  ￼

⸻

Observability (5 Pillars)
	•	Metrics: ingest success, row counts, validation pass/fail, publish latency, coverage by SI/APC.
	•	Logs: structured with batch_id, file digests, rule outcomes.
	•	Traces: pipeline steps + dataset digest lineage.
	•	Dashboards: freshness vs CMS release date, validation heatmap, delta trends.  ￼
	•	Alerts: zero-record publish, SLA breach (>5 business days), quarantine > X rows; page on-call.  ￼

⸻

Security & Compliance
	•	Data is public; no PHI/PII.
	•	CPT® licensing: Addendum B mixes HCPCS Level II (CMS) and CPT (Level I, AMA); displaying or distributing CPT content typically requires an AMA license. Until executed, mask or omit CPT descriptors in external outputs.  ￼
	•	Access controls: standard RBAC on raw/stage/curated; secrets only if any authenticated endpoints appear (not expected for CMS public files).

⸻

Testing & QA (per QA Standard)

Test tiers
	•	Unit: parsers/mappers by format (CSV/XLS/XLSX/TXT; ZIP handling).
	•	Integration (E2E): land→publish on golden quarters (2025 Q1 and 2025 Q2).
	•	Data-contract/schema diff: warn on both breaking & non-breaking (column order/type) changes; block on removed/renamed columns.
	•	Scenario/Golden: code adds/deletes & SI changes from I/OCE notes; wage index joinability; corrections (r02) superseding r01.  ￼
	•	Performance: CI gate—single quarter end-to-end < 30 minutes on CI class hardware.

Fixtures
	•	tests/fixtures/opps/2025Q1/{A,B,...} and 2025Q2/{A,B,...} with checksums and minimal row subsets reflecting edge cases.

⸻

Operations Runbook
	•	CLI: discover (scraper diff), download (with checksum), ingest, reprocess --batch opps_YYYYqN_rNN, backfill --from 2018Q1 --to 2025Q4.
	•	Trigger: On discovery via scraper (poll OPPS page), with polite throttle/backoff/retry; store SHA-256 digests.  ￼
	•	Concurrency: initial backfill: max 3 quarters in parallel, 4 files/quarter; live pipeline: serial quarters, parallel files.
	•	Rollback/backfill: publish is idempotent per batch; revert by promoting prior batch view; backfills run through same validations.
	•	SLA: publish ≤ 5 business days post CMS posting.  ￼

⸻

Success Metrics & Acceptance Criteria
	•	Freshness: 100% quarters published within SLA.
	•	Validation: 100% structural/schema/domain/temporal rules pass; quarantines triaged.
	•	Coverage: 100% of HCPCS rows present per quarter; wage index joins materialized in enriched view.
	•	Observability: dashboards populated; alerting wired; lineage complete.

⸻

Risks & Mitigations
	•	Upstream layout/format changes (CSV↔XLS/XLSX/TXT/ZIP): robust sniffer + schema contracts; contract-driven parser selection.  ￼
	•	Retro corrections (ASP drugs/biologics, CR timing): corrections ingested as new batches; dashboards surface deltas and publish dates.  ￼
	•	Robots/Terms posture: business directive is to ignore robots.txt; risk of IP blocks/policy issues. Mitigate with polite fetch, throttling, and mirrored artifact retention.  ￼
	•	CPT licensing: secure AMA license for any UI/export with CPT descriptors; otherwise suppress CPT text externally.  ￼

⸻

QA Summary (per QA Standard)

Area	Plan
Scope	Addendum A/B + wage enrichment; 2018→present backfill; corrections as new batches
Tiers	Unit, Integration (E2E), Contract/Diff, Scenario/Golden, Performance
Fixtures	2025 Q1, 2025 Q2 goldens; I/OCE-driven SI/code change cases
Gates	All validations green; CI perf <30m; schema diffs warned; breaking removals blocked
Monitors	Freshness, validation, deltas, lineage
Manual QA	Spot audit top N SI/APC movements vs I/OCE notes; verify wage joins
Open Risks	Upstream format shifts; licensing; scraper access posture


⸻

Change Log
	•	2025-09-30 (v0.1): Initial draft with decisions, sources, and optional API posture.

⸻

Cross-References
	•	DIS PRD: ingestion stages, manifests, quarantine, lineage.
	•	STD-qa-testing_prd_v1.0: test tiers, fixtures, CI gates.
	•	Scraper Standard: discovery, throttling, checksum/idempotency.
	•	STD-api-architecture_prd_v1.0: Optional API (deferred/derived):

Optional API (Deferred / Derived Layer — DIS-Aligned)
	•	Treat APIs as interfaces over curated data; responses include batch_id/published_at to bind to data vintage.
	•	Example (later):
	•	GET /opps/rates?date=YYYY-MM-DD&hcpcs=&modifier= → APC, SI, rate, batch_id/published_at.
	•	GET /opps/hcpcs/{code}?quarter=YYYYqN → mapping + SI + effective range.
	•	GET /opps/status-indicators → SI label/description (lookup).
	•	GET /opps/wage-index?ccn=&quarter= → CBSA + wage index.
	•	Follow API lint/CI gates, SLOs, and versioning per Global API Program.