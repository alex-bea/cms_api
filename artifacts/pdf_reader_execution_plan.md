# PDF Reader Execution Plan (v1.0)

**References:**  
- `artifacts/pdf_reader.md` (baseline requirements & architecture)  
- `prds/STD-parser-contracts-prd-v2.0.md` / `prds/STD-parser-contracts-impl-v2.0.md`  
- `prds/STD-doc-governance-prd-v1.0.md`, `prds/STD-scraper-prd-v1.0.md`

---

## 1. Current State Snapshot

| Area | Observation |
|------|-------------|
| Tooling | No existing `pdf_reader` module/CLI; only guidance summary helper with placeholder extraction version (`1.0.0`). |
| Layout Registry | Fixed-width specs live in parser modules (`cms_pricing/ingestion/parsers/layout_registry.py`), not in a standalone guidance bundle; no automated diffing vs PDFs. |
| Metadata & Provenance | No persisted `metadata.json`, checksum tracking, anchor confidence logs, or event emission as required in @pdf_reader.md. |
| Render Integration | Directory expectations (`/mnt/data/releases` & `/mnt/data/docs/guidelines`) are documented but not implemented. |
| Tests | No unit or integration coverage for PDF extraction or schema diffing. |

---

## 2. Gap Analysis Summary

1. **Extraction Pipeline Missing** – Need orchestrated fallback chain (`pdfplumber → pdfminer.six → OCR`) with anchor detection, confidence scoring, and field/enumeration extraction per §12 of @pdf_reader.md.  
2. **Guidance Outputs Absent** – Must emit `schema.json/.yaml`, `guidance.json/.md`, `metadata.json`, and optional `diff_summary.md`, matching the layout schema in @pdf_reader.md §6.3.  
3. **CLI & Config** – Implement `python -m cms_pricing.ingestion.docs.pdf_reader` entry point with flags (`--source`, `--release-id`, `--out`, `--registry-path`, etc.).  
4. **Layout Diff & Registry Hooks** – Provide comparison against the canonical registry (currently in parser modules) and fail on breaking changes (anchor missing, field bounds shift >±1, checksum mismatch).  
5. **Observability/Telemetry** – Record structured logs/events (`pdf_guidance_extracted`) and escalate when OCR usage or anchor confidence thresholds are exceeded (per @pdf_reader.md §14).  
6. **Documentation & Governance** – Register the new tool/outputs in `DOC-master-catalog-prd-v1.0.md`, update `artifacts/pdf_reader.md` with final CLI details, and ensure doc naming/version rules are satisfied.

---

## 3. Implementation Work Breakdown

### Phase 1 – Foundations
- Add `cms_pricing/ingestion/docs/pdf_reader/` package with modules: `cli.py`, `extractor.py`, `anchors.py`, `schema_writer.py`, `metadata.py`, `diff.py`.
- Define anchor configuration format (`cms_pricing/ingestion/docs/anchors/<release>.yaml`) with default regex templates (`DATA RECORD`, `ATTACHMENT`, `FILE ORGANIZATION`).
- Wire CLI (argparse) supporting:
  - `--source`, `--release-id`, `--out`
  - optional `--anchors`, `--registry-path`, `--write-registry`, `--log-json`, `--ocr`
- Dependencies: add `pdfplumber`, `pdfminer.six`, `pytesseract`, and `python-Levenshtein` to `pyproject.toml`; introduce optional extras for OCR to keep base install slim; document Render Docker image updates/Tesseract layer.
- Acceptance criteria: CLI scaffolding runs `--help`; anchor config validated via unit test; dependency installation verified in local dev + Render build image.

### Phase 2 – Extraction & Guidance Emission
- Implement extractor chain:
  1. `pdfplumber` table extraction (structured layout).
  2. Fallback to `pdfminer.six` (text extraction) if confidence low.
  3. Escalate to OCR (`pytesseract`) with page-level metrics.
- Detect anchors; capture page/line, confidence, and log events.
- Produce layout model (fields, enums, attachments) and serialize to `schema.json` + `schema.yaml`.
- Generate `guidance.json` + `guidance.md`, including payment formulas, status/global period glossaries (reuse dictionaries from `guidance_summary`).
- Write `metadata.json` with checksum, page count, posted date (CLI override), anchor stats, fallback usage, tool version.
- Update `guidance_summary.GUIDANCE_EXTRACTION_TOOL_VERSION`.
- Acceptance criteria: running CLI against `sample_data/rvu25a/RVU25A.pdf` produces schema/guidance/metadata files with non-empty fields; JSON/YAML outputs round-trip; metadata captures fallback counts, checksum, anchor confidence.

### Phase 3 – Registry Diff & Failure Policy
- Implement diff engine that compares emitted schema with existing registry (for now `cms_pricing/ingestion/parsers/layout_registry.py`); output machine-readable summary + Markdown diff.
- Apply failure rules from @pdf_reader.md §9:
  - Exit non-zero on missing anchors, checksum mismatch, empty enumeration sets, or breaking layout shifts.
- Optionally support `--write-registry` to update registry (guarded by manual confirmation).
- Acceptance criteria: CLI exits non-zero when test fixture intentionally shifts field bounds; diff summary artifact generated; unit tests cover pass/fail scenarios; optional registry write guarded by prompt/flag.

### Phase 4 – Observability & Integration
- Emit structured event (`pdf_guidance_extracted`) containing release id, anchor confidence, fallback counts, diff status.
- Hook into ingestion pipeline (Render cron or GH Action) post-download.
- Ensure directories `/mnt/data/releases/<release>/` and `/mnt/data/docs/guidelines/<release>/` are created and permissions validated.
- Acceptance criteria: synthetic run logs structured event to logger/event bus; Render cron job script executes CLI successfully in staging, producing artifacts under expected directories; failure path triggers alert.

### Phase 5 – Testing & Documentation
- Unit tests for anchor detection, schema serialization, diff engine (pytest).
- Integration test: run CLI against sample PDF (`sample_data/rvu25a/RVU25A.pdf`), verify outputs, diff vs registry.
- Update `artifacts/pdf_reader.md` with CLI usage, expected outputs, and failure handling.
- Register tool in `DOC-master-catalog-prd-v1.0.md`, add any new RUN/STD docs if needed.
- Provide deployment checklist (Render dependency install, environment variables, OCR packages).
- Acceptance criteria: CI pipeline includes new tests; documentation PR merged; Master Catalog entry approved; deployment checklist validated in staging run.

---

## 4. Deliverables

| Deliverable | Description |
|-------------|-------------|
| `cms_pricing/ingestion/docs/pdf_reader/` package | Extraction + guidance emission code with CLI entry point. |
| Guidance artifacts | `schema.json`, `schema.yaml`, `guidance.json`, `guidance.md`, `metadata.json`, optional `diff_summary.md` per release. |
| Observability | Structured event logging & metrics; fallback/anchor thresholds enforced. |
| Tests | Unit + integration coverage merged into CI. |
| Documentation | Updated `artifacts/pdf_reader.md`, Master Catalog registration, developer guide additions. |

---

## 5. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| OCR dependencies increase build size | Package OCR separately; guard behind `--ocr` flag; cache Tesseract binaries on Render. |
| Layout registry drift during development | Use `--registry-path` to detect diff early; treat diff summary as part of PR review. |
| Anchor regex fragile across releases | Maintain override YAML per release; include confidence metrics for monitoring. |
| Performance on large PDFs | Cache parsed pages, use selective OCR (only when text extraction fails). |
| Library licensing / security | Run OSS review (`pdfplumber`, `pdfminer.six`, `pytesseract`) through existing compliance checklist; add Snyk/GH Dependabot watch. |

---

## 6. Next Steps
1. Scaffold package & CLI (Phase 1).  
2. Implement extraction/diff logic, deliver MVP for RVU25A.pdf (Phases 2–3).  
3. Wire observability + produce example guidance outputs for latest release.  
4. Add tests/documentation; update Master Catalog; handoff to ingestion teams.  

> Track progress against this plan within `github_tasks_plan.md` and cross-reference @pdf_reader.md for baseline requirements.

---

## 7. Operational Rollout & Backout
- **Pre-rollout checklist:** Render image rebuilt with new dependencies; cron job staged; alert routing tested; compliance review approved.
- **Rollout steps:** Enable cron in staging for one release cycle → validate artifacts → promote to production cron; ship sample guidance artifacts (`artifacts/rvu25a_guidance/`) in same PR for reference.
- **Backout:** Disable cron + feature flag, revert to previous guidance artifacts in `/mnt/data/docs/guidelines`, roll registry back via last known good commit, notify ingest teams.

## 8. Resourcing & Timeline
- **Engineering:** 1 backend engineer ~2 sprints for Phases 1–3; shared time from ingestion team for schema validation; observability owner for Phase 4 hooks.
- **Dependencies:** Access to Render ops for cron/deployment, DevOps for image rebuild, Security/Compliance for OSS review.
- **Milestones:** P1 Foundations (Week 1), P2–3 MVP (Week 2–3), Observability & rollout (Week 4), Full documentation + sign-off (Week 5).

## 9. Compliance & Sample Artifacts
- Run OSS license scan before merging dependency updates; document results in `docs/compliance/oss/pdf_reader.md`.
- Store first generated bundle under `artifacts/rvu25a_guidance/` (schema, guidance, metadata, diff) as golden fixtures for tests and reviewers.
