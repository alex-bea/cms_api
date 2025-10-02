Scraper Platform PRD (MVP → v1)

0) Summary

Build a cheap, reliable, and auditable scraper/orchestration layer that discovers and fetches latest files monthly across CMS/HRSA (and future sources), snapshots raw artifacts, normalizes to typed tables (Parquet-first), and exposes dataset digests for downstream pricing and comparisons. Freshness is defined, costs are guarded, and observability + alerts route to Page Alex.

⸻

1) Goals & Non‑Goals

Goals
	•	Monthly “latest-only” discovery & ingestion for MVP sources (CMS ZIP9, MPFS RVU/GPCI/CF, OPPS; HRSA later).
	•	Cheap & simple orchestration using GitHub Actions (GHA) with manual dispatch.
	•	Immutable snapshots (bronze), normalized Parquet (silver/gold), 6‑year retention in S3 with lifecycle.
	•	Freshness SLOs aligned to monthly checks; run‑now button for ad‑hoc fetch.
	•	Idempotent ingestion keyed by digest (hash of artifacts + manifests).
	•	Basic observability: success rates, row deltas, freshness, schema drift; alerts → Alex.

Non‑Goals (MVP)
	•	Full Payer TiC/MRF high‑volume crawling (defer; design stubs allowed).
	•	Complex distributed compute; ECS/Batch/self‑hosted runners only when GHA minutes become a constraint.

⸻

2) Scope & Sources (MVP → v1)

In‑scope (MVP): CMS ZIP9 locality mapping; MPFS RVU/GPCI/CF; OPPS packaging minimal; HRSA (later in MVP timeline).
Future: ASC, ASP/NADAC, CLFS, DMEPOS, Medicaid pilots (1–2 states), MRF/TiC large‑scale, hospital CCN resolver and MRF slice cache.

⸻

3) Orchestration & Scheduling

Choice: GitHub Actions (GHA) cron workflows.
	•	One workflow per source (e.g., cms_zip9.yml, cms_mpfs_rvu.yml).
	•	Cadence: weekly change‑check cron; plus workflow_dispatch for manual runs.
	•	Jobs call Python CLI ingestors or the scraper API (FastAPI) running in Docker.
	•	Scale path: use GHA for discovery → trigger ECS Fargate/Batch/self‑hosted runner for heavy downloads later.

Approval Gate (ask before download/process)
	•	Weekly runs perform discovery + change detection only.
	•	If new/changed artifacts are detected, emit an approval request (Slack/Email) to Ops (Page Alex) containing: source, diffs, estimated size, and links to manifests/logs.
	•	On approval, launch the download + normalize job; otherwise, defer until next run or an on‑demand trigger.

Example GHA skeleton

name: CMS RVU Weekly Discovery
on:
  schedule:
    - cron: '0 9 * * 1' # 09:00 PT, every Monday
  workflow_dispatch:
jobs:
  discover:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - env:
          SCRAPER_API_KEY: ${{ secrets.SCRAPER_API_KEY }}
          AWS_REGION: us-west-2
          AWS_ROLE: ${{ secrets.AWS_ROLE_ARN }}
        run: |
          python -m ingestors.rvu --mode discovery --out s3://ORG-pricing-data/bronze/cms_rvu/
      - name: Request approval if changed
        run: python ops/request_approval.py --source cms_rvu --diff artifacts/diff.json


⸻

4) Storage, Retention & Layout (S3)

Bucket: s3://<org>-pricing-data/
	•	Encryption: SSE‑S3 (OK) or SSE‑KMS (recommended for key rotation/audit).
	•	Versioning: ON.
	•	Lifecycle: Standard → Standard‑IA @ 90d → Glacier @ 365d; retain 6 years; delete thereafter.

Data zones

bronze/<source>/<year>/<yyyymmdd_HHMMSS>/raw.zip|raw.csv|... + manifest.json
silver/<source>/<year>/<digest>/normalized.parquet (typed schema)
gold/<dataset>/<year>/<digest>/... (joined/derived tables)

Manifest (bronze)

{
  "source": "cms_rvu",
  "discovered_at": "2025-09-01T16:04:22Z",
  "files": [{"url": "https://.../rvu25a.zip", "sha256": "...", "last_modified": "2025-08-30"}],
  "snapshot_digest": "sha256:...",
  "notes": {"quarter_set": ["A","B","C","D"], "version": "v2 if republished"}
}


⸻

5) Backfill Horizon & Modes
	•	Initial backfill: last 3 years per source.
	•	Modes: latest-only (default), range --start_year YYYY --end_year YYYY (off by default for cost control).
	•	Idempotent: skip processing if digest unchanged; log “no change”.

⸻

6) Freshness SLOs (What they mean)

Definition: time between publisher making a new file available and our snapshot published.
	•	Detection: prefer HTTP ETag/Last-Modified; else compute SHA256 and compare against prior manifest.
	•	Proposed SLOs (aligned to weekly discovery):
	•	CMS MPFS/ZIP9/OPPS: ≤72h from detection.
	•	HRSA: ≤7 days from detection.
	•	MRF/TiC (future): ≤7 days.
	•	Manual Run Now supported to override cadence when needed.

⸻

7) Cost Guardrails
	•	Latest-only ingestion by default.
	•	Skip re‑processing when digest unchanged.
	•	“Custom Range” backfills disabled by default.
	•	Polite throttling (≤0.5 RPS) + exponential backoff; obey robots/ToS.

⸻

8) Legal & Politeness
	•	Maintain a per‑source allowlist of domains/paths; scrapers must not fetch outside allowlisted scopes.
	•	Evaluate and record robots.txt compliance status in manifest; respect crawl‑delay/usage policies.
	•	Ingest Hospital MRFs and Payer TiC at scale within ToS; throttle, rotate user‑agents, expose a per‑source kill switch.

⸻

9) PDF Policy (state Medicaid & edge cases)
	•	Human‑in‑the‑loop: on first PDF‑only source discovery, pause behind a feature flag, prototype parser(s), and validate outputs vs. manual spot‑checks before enabling.

⸻

10) Data Formats & Schema Registry
	•	Parquet default for silver/gold; CSV allowed for tiny feeds/QA samples.
	•	Schema Registry (lightweight): YAML per dataset to lock dtypes and validate at ingest time.

Schema YAML example

version: 1
name: cms_rvu
columns:
  - name: hcpcs
    dtype: string
    required: true
  - name: work_rvu
    dtype: decimal(9,4)
  - name: nonfac_pe_rvu
    dtype: decimal(9,4)
  - name: mp_rvu
    dtype: decimal(9,4)
  - name: effective_date
    dtype: date
primary_key: [hcpcs, effective_date]


⸻

11) Alerts → “Page Alex”

Trigger alerts on:
	•	Freshness SLO breach.
	•	HTTP 429/5xx storms or repeated retries exhaustion.
	•	Schema drift vs. registry.
	•	Huge row deltas outside expected bounds.

Routing: Slack/PagerDuty/email (configurable). Payload includes source, run id, link to logs, and the manifest key.

⸻

12) RVU “Latest Files Integration” (hardening)
	•	Year detection: parse filenames (e.g., rvu25a.zip → 2025) with fallback to page metadata (anchor text/table column).
	•	Quarter packs A/B/C/D: treat as a set; record a single snapshot digest that rolls all four.
	•	Corrections: CMS may republish letters (e.g., RVU25B v2) → rely on Last-Modified/ETag + hash compare to decide re‑ingestion.
	•	Security: keep /api/v1/rvu/scraper/* behind API key + rate limit; manifest logged.
	•	Idempotency: short‑circuit on equal digests; log reason.

⸻

13) Architecture & Workflow

Happy path
	1.	Discovery (GHA): enumerate candidate URLs; collect headers (ETag/Last-Modified), compute URL list.
	2.	Change detection: compare against last snapshot manifest; if unchanged → exit early.
	3.	Fetch: download raw artifacts with throttle/backoff; compute sha256 per file.
	4.	Bronze snapshot: write artifacts + manifest.json; compute snapshot_digest.
	5.	Normalize (silver): parse → typed Parquet; validate against schema registry.
	6.	Gold: optional derivations (joins, effective dating).
	7.	Publish: update dataset registry; emit metrics; send success/alerts.

Key components
	•	Python ingestors (CLI), FastAPI admin endpoints, Docker images, S3 client, small schema registry.

⸻

14) Roadmap (Data acquisition → priced comparisons)

Milestone A — Scraper platform (MVP, cheap + monthly)
	•	GHA workflows per source; S3 bronze/silver/gold; snapshot registry; digest checks; latest‑only default.
Exit: datasets appear in /admin/datasets, pin‑able by digest.

Milestone B — Geography done
	•	ZIP9 fixed‑width loader; nearest‑ZIP resolver (same‑state) in /price; daily gap report.
Exit: every run returns a locality or a strict error.

Milestone C — MPFS pricing v1
	•	RVU/GPCI/CF ingested; POS NF/FAC; modifiers (‑26, ‑TC, ‑50, multi‑procedure); Part‑B deductible & 20% coinsurance.
Exit: golden tests pass (~50 codes × 3 localities); end‑to‑end cents stable.

Milestone D — OPPS & ASC minimal
	•	OPPS packaging (J1, Q1/Q2/Q3, N) with warnings; ASC base loader; 20% coinsurance cap.
Exit: mixed plans (MPFS+OPPS/ASC) priced with traceable math.

Milestone E — Drugs/Labs/DMEPOS
	•	ASP + NADAC; CLFS, DMEPOS loaders; NDC↔HCPCS crosswalk; rural flags.
Exit: drugs/labs/DME priced; reference vs allowed surfaced.

Milestone F — Facility & independents
	•	Hospital MRF slice cache; CCN resolver; CSV rate‑card uploads; fallback multiplier (default 225% of Medicare) per provider.
Exit: compare Medicare vs hospital vs uploaded rates by location.

Milestone G — Compare parity & snapshots
	•	/compare hard‑rejects when snapshots/toggles differ; payer/plan filter order; “winner” column; compare‑lock.
Exit: rigorous, auditable comparisons.

Milestone H — Medicaid scaffolding & future MCO TiC
	•	1–2 state Medicaid fee schedules behind a flag; document future MCO TiC parsing.
Exit: optional Medicaid comparator.

Milestone I — Ops polish
	•	Dashboards (freshness, success, deltas); alerts → Page Alex; runbook; CI (lint/tests); OpenAPI examples; Docker images; seed data.

⸻

15) Observability & Metrics
	•	Counters: runs, successes, failures, retries, bytes downloaded.
	•	Gauges: freshness lag (per source), rows ingested, row delta %, time‑to‑normalize, storage usage, next_run_ts, last_run_ts.
	•	Logs: per run id; include source, mode, snapshot_digest, manifest_key.
	•	Dashboards: Prometheus → Grafana (or GHA summary + CloudWatch alternative).
	•	Alert thresholds: failure rate >5% per run; zero files discovered on an expected cadence; checksum mismatch events; robots violations; weekly discovery not executed within 8 days.

⸻

16) Security & Access
	•	API endpoints behind API key; rate‑limited.
	•	User‑Agent: dedicated UA string with contact email; rotate as needed; documented in manifest.
	•	AWS access via OIDC‑assumed role from GHA to S3 (least privilege, write‑only to bucket prefixes).
	•	Secrets in GHA secrets and environment‑specific parameter store/Vault; never commit tokens.
	•	Audit logs: record download attempts (timestamp, URL, response code) and store with run metadata.

⸻

17) Runbook (High Level)
	•	Incident types: SLO breach; 429 storm; schema drift; parse failure.
	•	First response: check last manifest; validate publisher change; pause source via kill switch if needed; file issue with label incident.
	•	Rollback: re‑pin previous digest in dataset registry; republish.

⸻

18) Risks & Mitigations
	•	Brittle URLs → add discovery by page scraping + pattern fallback.
	•	Publisher silent corrections → rely on Last-Modified/ETag + hashing.
	•	Growing volume → ECS/Batch hand‑off; sharded downloads; caching.
	•	PDF‑only feeds → HiTL policy; compare outputs pre‑enable.

⸻

19) Open Decisions (for config lock)
	1.	Proceed with GitHub Actions monthly schedules + manual run; plan ECS hand‑off for heavy jobs.
	2.	Confirm S3 bucket name / KMS key; enable Versioning + 6‑year lifecycle now.
	3.	Approve Freshness SLOs: ≤72h (CMS) and ≤7 days (HRSA) with monthly discovery.
	4.	Alerts routing: Slack, email, or PagerDuty for Alex; include destination details.
	5.	Make latest‑only the default for all CMS sources (ZIP9/GPCI/OPPS) with backfill override.
	6.	Keep /api/v1/rvu/scraper/* internal-only (API key) for now.
	7.	Approve lightweight schema registry (YAML) per dataset for dtype locks.

⸻

20) Acceptance Criteria (MVP exit)
	•	New datasets appear in /admin/datasets with a pin‑able digest.
	•	Re‑runs with unchanged publisher inputs no‑op and log “no change”.
	•	Freshness metrics present; SLO alerts wired to Alex channel.
	•	Bronze/silver/gold artifacts present with 6‑year lifecycle policy attached.
	•	At least one golden test suite passes for MPFS (≈50 codes × 3 localities).

⸻

21) Appendix

A. S3 key examples

bronze/cms_rvu/2025/20250901_160422/rvu25a.zip
bronze/cms_rvu/2025/20250901_160422/manifest.json
silver/cms_rvu/2025/sha256-XYZ/rvu.parquet

B. Snapshot digest
	•	snapshot_digest = sha256(sorted(file_sha256s) + manifest_metadata)

C. Minimal CLI contract

python -m ingestors.rvu --mode latest-only --start_year 2023 --end_year 2025 --out s3://... --schema schemas/cms_rvu.yaml

D. Alert payload (example)

{
  "source": "cms_rvu",
  "event": "freshness_slo_breach",
  "detected": "2025-09-04T10:12:00Z",
  "lag_hours": 90,
  "last_manifest": "s3://.../bronze/cms_rvu/2025/20250901_160422/manifest.json",
  "run_log": "https://github.com/org/repo/actions/runs/123456789"
}


⸻

22) Addenda: Scraper Standard Details (adopted)
	•	Concurrency & timeouts: async client with bounded concurrency (default ≤4 simultaneous downloads per domain) + retries with exponential backoff.
	•	Size‑drift guard: validate file size vs historical ranges (±20% default) to catch truncations.
	•	Manifest enrichment: include license, discovery_method, scraper_version (semver), robots_compliant flag, anomaly notes.
	•	Error taxonomy & quarantine: standard codes (SCRAPER_INVALID_URL, SCRAPER_FORBIDDEN, SCRAPER_PARSE_ERROR, SCRAPER_TIMEOUT, SCRAPER_CHECKSUM_MISMATCH); quarantine failed artifacts with reason, attempts, remediation guidance.
	•	CI/QA gates: recorded HTML fixtures (VCR); drift‑detection tests that can block merges; target coverage ≥80% for scraper modules.
	•	Change management: semver policy; dry‑run + Ops sign‑off for major changes; changelog maintained.
	•	Audit & compliance: attribution flags (attribution_required); audit logs persisted with run metadata.

(retains 6‑year retention and bronze/silver/gold data zones as previously defined)