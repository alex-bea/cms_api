# Documentation Compliance Audit Expansion Plan

## Background
The current documentation audit suite focuses on structural hygiene: master catalog coverage, backlinks, cross-references, metadata alignment, changelog integrity, and companion consistency. Policy enforcement still relies on manual review. Recent updates to `STD-database-platform-prd-v1.0.md` and `STD-doc-governance-prd-v1.0.md` introduce explicit runbook and status requirements that should be validated automatically. This plan generalises the database compliance pilot into a governance-aware audit that scales to every standard and dependent document type.

## Goals
- Capture machine-readable compliance requirements from each governance standard (`STD-*` docs).
- Automatically verify that dependent documents (RUN, REF, PRD, DOC) exist and satisfy required sections, headers, statuses, and cross-references.
- Integrate the new compliance audit with the existing tooling (`run_all_audits.py`) so CI fails when policy gaps appear.
- Provide maintainers with actionable reports, remediation guidance, and unit tests covering edge cases.

## Deliverables
1. Requirements specification format (YAML/JSON or code constants) mapping each standard to the artefacts and checklist items it mandates.
2. Enhanced document parser utilities that extract headers, section headings, checklists, and governance call-outs from markdown.
3. New audit module (e.g., `tools/audit_doc_compliance.py`) that evaluates requirements against parsed documents and emits structured findings.
4. Unit tests and fixtures demonstrating compliant vs. non-compliant scenarios for each enforced standard.
5. Documentation updates for contributors (README + relevant PRDs) explaining how requirements are captured and how to interpret failures.

# Phased Implementation & Timeline

We will execute the rollout across five phases spanning approximately eight weeks. Weeks are counted from the formal kickoff once this plan is approved.

| Phase | Weeks | Focus |
|-------|-------|-------|
| Phase 1 | Weeks 1-2 | Requirements inventory & schema design (database stack pilot) |
| Phase 2 | Weeks 2-3 | Parser & utility enhancements |
| Phase 3 | Weeks 3-5 | Compliance audit engine MVP and integration |
| Phase 4 | Weeks 5-7 | Expansion to additional standards & tests |
| Phase 5 | Weeks 7-8 | Contributor experience, documentation, automation polish |

### Phase 1 — Requirements Inventory (Weeks 1-2)
- [ ] Catalogue compliance statements in `STD-database-platform-prd-v1.0.md` and the associated runbooks (`RUN-database-migrations`, `RUN-database-backup-dr`, `RUN-database-sanitization`).
- [ ] Define requirement schema (JSON/YAML or Python dataclass) capturing:
  - Required documents and expected `requires`/`Cross-References`.
  - Mandatory section headings or checklist items.
  - Expected status vocabulary or minimum version.
- [ ] Populate the schema for the database standard as the MVP data set.

**Exit Criteria:** Written requirement definitions committed for the database stack; pilot standards reviewed with doc owners.

### Phase 2 — Parser & Utility Enhancements (Weeks 2-3)
- [ ] Extend `tools/shared/prd_helpers.py` (or a new helper) to expose:
  - Header metadata (status, owners, change control, requires).
  - Table of contents / section headings (up to configurable depth).
  - Presence of checklists or call-out blocks (e.g., `##`, bullet lists).
- [ ] Ensure parsers handle archived docs, multi-line headers, and legacy formatting.
- [ ] Add unit coverage for parsing edge cases (missing status, malformed headers, etc.).

**Exit Criteria:** Parsing utilities return structured metadata for pilot docs with unit tests passing.

### Phase 3 — Compliance Audit Engine (Weeks 3-5)
- [ ] Create `tools/audit_doc_compliance.py` leveraging the requirement schema and parsing utilities.
- [ ] Implement severity levels:
  - **Error:** missing required document, missing section, wrong status.
  - **Warning:** optional guidance absent, deprecated wording (“Stub”) still present.
- [ ] Produce human-readable reports plus machine-friendly output for future dashboards.
- [ ] Wire the module into `tools/run_all_audits.py` and `run_all_audits.sh`.

**Exit Criteria:** MVP audit catches deliberate gaps in pilot runbooks; CI integration (optional warning mode allowed initially).

### Phase 4 — Standard Expansion (Weeks 5-7)
- [ ] Prioritise other governance standards (e.g., `STD-doc-governance`, `STD-api-security-and-auth`, `STD-observability-monitoring`) and encode their requirements.
- [ ] For each new standard:
  - Update requirement schema entries.
  - Add targeted fixtures/tests.
  - Document unique compliance notes in the standard or supporting README.
- [ ] Establish owner workflow for keeping requirement definitions in sync with standard revisions (e.g., PR checklist, governance review).

**Exit Criteria:** At least two additional standards enforced with passing fixtures; governance owners sign off on expanded coverage.

### Phase 5 — Contributor Experience & Automation (Weeks 7-8)
- [ ] Update `README.md` and `prds/STD-doc-governance-prd-v1.0.md` with instructions on maintaining compliance definitions and running the audit locally.
- [ ] Provide remediation guidance in audit output (e.g., link to relevant section in the governing STD).
- [ ] Consider optional `--strict` flag to convert warnings to errors once teams are ready.
- [ ] Generate weekly compliance summary artifacts (e.g., JSON report) for governance dashboards.

**Exit Criteria:** Documentation refreshed, audit output actionable, and governance summary artifact created.

## Risks & Mitigations
- **Requirements Drift:** Standards change without updating requirement schema.  
  _Mitigation:_ add a governance checklist item requiring schema updates when standards change; consider embedding requirement definitions directly in the standard via fenced YAML that can be parsed automatically.

- **Parser Fragility:** Markdown formatting variations break extraction.  
  _Mitigation:_ normalise inputs, include regression fixtures for known formats, and fall back to warnings instead of crashes.

- **False Positives During Adoption:** Legacy documents may lack new sections.  
  _Mitigation:_ introduce warning-only mode per standard, ramp to errors after remediation window.

## Success Metrics
- 0 undocumented policy violations in CI (audits green).
- Coverage extended to ≥80% of governance standards within two iterations.
- Reduction in manual governance review time as reported by reviewers.
- Contributors can remediate compliance findings within a single PR feedback cycle.

## Next Steps
1. Approve this plan with the governance/doc owners.
2. Create a GitHub task referencing this document.
3. Start Phase 1 inventory work and prepare fixtures for the database runbook pilot.
