# PDF Reader Product Requirements Document (PRD) v1.0

**Status:** Draft v1.0  
**Owners:** Ingestion Engineering  
**Consumers:** Data Platform Engineering, QA/Validation, Observability, Render DevOps  
**Change control:** Architecture owner + Product approval  
**Review cadence:** Monthly — **Last reviewed:** 2025-11-04  

---

## 1. Background & Context
- **Problem statement:** The RVU/MPFS ingestion pipeline lacks an authoritative, machine-readable source for CMS layout definitions. Parser authors currently maintain fixed-width specs manually (`cms_pricing/ingestion/parsers/layout_registry.py`), which makes releases brittle and delays validation whenever CMS changes a PDF layout.
- **Why now:** Phase 2/3 ingestion milestones require automated schema validation for 2025 datasets (see `artifacts/pdf_reader.md` and `artifacts/pdf_reader_execution_plan.md`). The new Render deployment footprint also mandates on-disk guidance bundles instead of ad hoc S3 notes.
- **Stakeholders & beneficiaries:** Ingestion engineering (lands + parsers), QA/Validation, Observability, AI assistance surfaces, and Render DevOps (cron + storage management).
- **Relationship to existing components:**  
  - Consumes PDFs staged by `land` and scraper stages.  
  - Produces layout artifacts that replace / augment `layout_registry.py`.  
  - Feeds metadata into `cms_pricing/ingestion/docs/guidance_summary.py`.  
  - Emits events consumed by the ingestion observability pipeline.  
- **Datasets in scope for v1.0:** CMS RVU bundles (PPRRVU), MPFS support tables (GPCI, LOCCO, OPPS CAP), ANES conversion factors, and accompanying release PDFs (`RVU25A.pdf`, etc.). Future extensions may cover OPPS and locality PDFs but are not required for the first release.

Here’s a tightened and non-repetitive revision of your PRD section.
I’ve removed redundancies (e.g., duplicate background sections), merged similar ideas, and streamlined overlapping explanations while preserving all technical and operational details.

⸻

PDF Reader — Product Requirements Document (PRD v1.0)

Product Owner: Ingestion Engineering
Author: Arnina Moore-Bea
Last Updated: 2025-11-04

⸻

1. Background & Context

The ingestion platform currently lacks a single authoritative, machine-readable source of CMS layout definitions.
Parser authors maintain fixed-width specs manually in layout_registry.py, making releases brittle and slow to adapt when CMS updates documentation.

The new Render-based deployment footprint requires all artifacts — including schema definitions and guidance — to be generated and stored locally instead of maintained ad hoc.
The PDF Reader will automatically extract schema layouts, enumerations, and metadata directly from CMS release PDFs, replacing manual maintenance.

Stakeholders: Ingestion engineering (lands + parsers), QA/Validation, Observability, AI Assistance, and Render DevOps (cron + storage).
Scope: CMS RVU bundles (PPRRVU), MPFS support files (GPCI, LOCCO, OPPSCAP), and ANES conversion factors.
Key insight: Each CMS scraper downloads a unique PDF with different headings and layouts; the PDF Reader must detect these structures automatically and produce dataset-specific schemas (e.g., rvu25a, opps25a) under /mnt/data/docs/guidelines/.

⸻

2. Problem Statement

Manual maintenance leads to:
	1.	Inconsistency between parser code and authoritative CMS specs.
	2.	Delays when CMS modifies field structures or indicators.
	3.	No provenance linking parser versions to their defining documents.

The system must automate schema extraction, validation, and change detection across all CMS PDF families.

⸻

3. Goals & Success Criteria

Goal	Metric	Target
Automate extraction of layout metadata	≥ 95 % of new RVU/MPFS PDFs parsed without manual edits	FY25
Generate machine-readable artifacts (schema, guidance, metadata)	100 % of processed releases	Continuous
Detect and block schema drift	100 % of breaking diffs stopped pre-release	Continuous
Maintain predictable runtime	≤ 5 min per 60-page PDF on Render	P95
Maintain anchor reliability	Avg ≥ 0.85 confidence; alert < 0.75	Continuous
Adoption	All ingestors using generated schemas	End Q2 FY25


⸻

4. User Stories
	•	Ingestion Engineer: Runs CLI post-scraper to generate validated schema + guidance artifacts.
	•	Parser Developer: Compares extracted schema with canonical registry to detect field shifts.
	•	QA Analyst: Uses diff_summary.md + metadata.json to confirm parser alignment before production.
	•	Support Engineer: References guidance.md to explain schema changes.
	•	AI Assistant: Surfaces enumerations + policy notes directly from guidance.json.

⸻

5. Functional Requirements

5.1 Extraction Pipeline
	•	Fallback chain: pdfplumber → pdfminer.six → pytesseract, logging which tool parsed each page.
	•	Detect anchors (DATA RECORD, ATTACHMENT A, etc.) via regex patterns defined in anchors/<release>.yaml.
	•	Extract fields: name, start–end, type, description, section; parse enumerations from attachments.
	•	Record metadata: page count, posted date, SHA256 checksum, fallback usage.

5.2 Guidance Artifact Generation
	•	Emit identical schema.json + schema.yaml using the format in artifacts/pdf_reader.md §6.3.
	•	Generate guidance.json + guidance.md summarizing dataset, formulas, and enumerations.
	•	Produce metadata.json (version, checksum, confidence, runtime stats).
	•	Optionally create diff_summary.md vs previous release.

5.3 CLI & Automation
	•	CLI entry: python -m cms_pricing.ingestion.docs.pdf_reader.
	•	Required: --source, --release-id, --out; optional: --anchors, --registry-path, --write-registry, --log-json, --ocr, --posted-at.
	•	Render cron triggers CLI after scraper lands bundle in /mnt/data/releases/<release>/; artifacts written to /mnt/data/docs/guidelines/<release>/.

5.4 Error Handling & Recovery
	•	Non-zero exit on missing anchors, checksum mismatch, or > ±1 char field shift.
	•	Emit pdf_guidance_extraction_failed event with reason + remediation.
	•	Safe reruns via atomic temp-dir overwrite.
	•	Warn if > 10 % pages require OCR.

5.5 Consumers
	•	Parsers load JSON schema directly.
	•	guidance_summary imports metadata for provenance.
	•	Observability tracks metrics and emits alerts.
	•	QA uses diffs to gate production ingestion.

⸻

6. Data Model & Outputs

Artifacts per release (example RVU25A):

/mnt/data/docs/guidelines/rvu25a/
 ├── rvu25a_schema.json
 ├── rvu25a_schema.yaml
 ├── rvu25a_guidance.json
 ├── rvu25a_guidance.md
 ├── rvu25a_metadata.json
 └── rvu25a_diff_summary.md

Metadata fields: extracted_at, extractor_version, checksum_sha256, page_count, anchor_confidence_avg, ocr_pages_used, runtime_seconds.
Schemas map directly to parser contracts in cms_pricing/validation/layout_registry.py.

⸻

7. Technical Architecture

Component	Role
Extractor Engine	Runs pdfplumber/pdfminer/pytesseract sequence.
Anchor Configs	Default + release-specific YAMLs.
Schema Writer	Serializes normalized field objects.
Diff Engine	Compares vs canonical registry and labels severity.
Render Integration	Cron post-scraper; atomic writes to persistent disk.

Performance: 20-page PDF < 3 min; 60-page ≤ 5 min; memory < 1 GB.
Extraction isolated in a dedicated worker to avoid pipeline impact.

⸻

8. Observability & Monitoring
	•	Events: pdf_guidance_extracted, pdf_guidance_extraction_failed, pdf_guidance_diff_detected.
	•	Metrics: duration, anchor confidence, missing anchors, fallback count, diff severity.
	•	Alerts: Slack/PagerDuty when confidence < 0.75 or breaking diff.
	•	Dashboards: Grafana/Metabase track releases processed, failures, runtime.

⸻

9. Dependencies
	•	Internal: guidance_summary, layout_registry, ingestion cron, observability pipeline.
	•	External: pdfplumber, pdfminer.six, pytesseract, python-Levenshtein, PyYAML.
	•	Infrastructure: Render persistent disk (/mnt/data/releases, /mnt/data/docs/guidelines); container image with OCR layer.
	•	Compliance: OSS license review and security scan (MIT/BSD/Apache 2.0).

⸻

10. QA & Validation
	•	Unit Tests: anchor detection, fallback logic, schema serialization, diff engine.
	•	Integration Tests: run against sample_data/rvu25a/RVU25A.pdf; verify artifacts vs golden fixtures.
	•	Acceptance: CLI --help works; required artifacts produced; diff engine blocks drift; metadata complete; events logged.
	•	Manual QA: validate first two live runs in staging; confirm cron execution.

⸻

11. Governance & Documentation
	•	Register tool in DOC-master-catalog-prd-v1.0.md.
	•	Update artifacts/pdf_reader.md when CLI or schema changes.
	•	Follow naming/version rules from STD-parser-contracts-prd-v2.0.md.
	•	Maintain runbook (docs/runbooks/pdf_reader.md) for operators.
	•	New PDF families must add anchor YAML + acceptance tests before onboarding.

⸻

12. Risks & Mitigations

Risk	Impact	Mitigation
OCR layer inflates container size	Slower deploys	Make OCR optional; cache Tesseract binaries
Anchor regex drift	Parse failures	Maintain per-release overrides + alerts
Registry divergence	Inconsistent data	Block diffs; require QA sign-off
Large PDFs slow cron	Delays	Cache pages; monitor runtime
Library CVEs	Security exposure	Dependabot/Snyk monitoring
Render disk corruption	Artifact loss	Checksums + rerun from PDF source


⸻

13. Timeline & Milestones

Phase	Deliverable	Target
P1	CLI + anchors + dependencies	Week 1
P2	Extraction + Guidance + Metadata	Weeks 2–3
P3	Diff engine + Failure policy	Week 3
P4	Observability + Render integration	Week 4
P5	QA + Docs + Catalog Entry	Week 5
Rollout	Enable cron + monitor	Week 6


⸻

14. Ownership & Change Control

Role	Responsibility
Ingestion Lead	Delivery & maintenance
QA Owner	Validation & approval
DevOps (Render)	Cron & storage reliability
Security	OSS scans & approvals
AI Team	Consumes guidance artifacts

All PRs altering schema or anchor logic require updated tests and QA re-approval.

⸻

15. Open Questions
	1.	Final anchor confidence thresholds — confirm with Observability.
	2.	Ownership of release-specific anchor YAMLs — ingestion vs parser teams.
	3.	Allow automatic registry updates (--write-registry) in CI or manual only?
	4.	Validate Tesseract language coverage for non-ASCII PDFs.

⸻

16. References
	•	artifacts/pdf_reader.md — baseline schema and equations.
	•	artifacts/pdf_reader_execution_plan.md — implementation sequence.
	•	STD-parser-contracts-prd-v2.0.md, STD-doc-governance-prd-v1.0.md, STD-scraper-prd-v1.0.md.
	•	cms_pricing/ingestion/docs/guidance_summary.py.
	•	sample_data/rvu25a/RVU25A.pdf — canonical test PDF.

⸻
