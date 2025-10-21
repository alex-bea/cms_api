doc_type: DOC
normative: false
status: Draft
owners:
  - Data Platform Engineering
review_cadence: "Per parser milestone or quarterly (whichever comes first)"
requires:
  - STD-parser-contracts-prd-v2.0.md
  - RUN-parser-qa-runbook-prd-v1.0.md

# Parser Build Playbook

**Version:** v1.0 (Draft until OPPSCAP parser validation)\
**Audience:** Parser implementers, QA reviewers, AI assistants supporting parser work\
**Purpose:** Provide a repeatable checklist for building CMS dataset parsers end-to-end.

This document enumerates the required steps (and artefacts) for each parser effort. It complements the parser contracts standard, implementation guide, and QA runbook by consolidating the “build” workflow into a single reference.

---

## 1. Before You Start

1. **Read source documentation.** Obtain the official CMS release notes/PDF (e.g., `RVU25D.pdf`) and skim for schema quirks, quarter naming, and set-logic notes.
2. **Download authentic samples.** Retrieve TXT/CSV/XLSX/ZIP from CMS or the RVU bundle.
   - Store under `sample_data/<dataset>/<release>/`.
   - Capture URL, checksum, and fetch date in `prds/SRC-<dataset>.md`.
3. **Stage fixtures.** Copy or subset representative records into `tests/fixtures/<dataset>/`.
4. **Update Authority Matrix.** Record the authoritative format, parity targets, and thresholds (`planning/parsers/<dataset>/AUTHORITY_MATRIX.md`).

---

## 2. Schema & Layout Alignment

1. **Validate schema contract.**
   - Confirm existing schema matches real data; if not, draft a breaking-change note (`SCHEMA_BREAKING_CHANGE_ANALYSIS.md`), bump version, and update the contract JSON.

2. **Verify fixed-width layout (if applicable)** - ENHANCED 2025-10-21

   **Critical:** Layout positions must be manually verified against real files. Off-by-N errors are common and cause silent data corruption.
   
   **Verification Checklist:**
   
   - [ ] **Print sample line with positions**
     ```python
     sample_line = "10112       00    ALABAMA                                                1931"
     print(f"Line length: {len(sample_line)}")
     for i in range(0, len(sample_line), 10):
         print(f"Pos {i:3d}: {repr(sample_line[i:i+10])}")
     ```
   
   - [ ] **Extract EACH field and verify contents**
     ```python
     print(f"MAC (0-5):      {repr(sample_line[0:5])}")       # Should be '10112'
     print(f"Locality (12-14): {repr(sample_line[12:14])}")   # Should be '00'
     print(f"Name (17-57):   {repr(sample_line[17:57])}")     # Should be 'ALABAMA...'
     print(f"CF (70-74):     {repr(sample_line[70:74])}")     # Check: is this correct?
     print(f"CF (73-77):     {repr(sample_line[73-77])}")     # Or is it here?
     ```
   
   - [ ] **Test extraction on 3-5 sample lines**
     - First line (may be header)
     - Middle lines (typical data)
     - Last line (may be footer/total)
   
   - [ ] **Document actual positions in layout comments**
     ```python
     ANES_2025D_LAYOUT = {
         'version': 'v2025.4.1',  # Fixed CF positions after verification
         'columns': {
             'anesthesia_cf': {
                 'start': 73,  # VERIFIED: 1931 extracted correctly
                 'end': 77,    # NOT 70-74 as initially assumed
                 'type': 'decimal',
                 'nullable': False
             },
         }
     }
     ```
   
   - [ ] **Compare against PDF layout spec (if available)**
     - CMS PDFs may have 1-indexed positions (subtract 1 for Python 0-indexed)
     - Verify column widths match spec
   
   **Real-World Example - ANES Layout Bug (2025-10-21):**
   - **Initial spec:** CF at positions 70-74
   - **Actual data:** CF at positions 73-77 (off by 3!)
   - **Symptom:** Extracted "   1" instead of "1931"
   - **Detection:** Manual verification caught before production
   - **Time saved:** Prevented 2-4 hours debugging in production
   
   **Time Investment:** 10-15 minutes verification saves 2-4 hours debugging

   **After verification:**
   - Add/update `<DATASET>_<YEAR>_<QUARTER>_LAYOUT` in `cms_pricing/ingestion/parsers/layout_registry.py`
   - Mirror updates in test fixtures (`tests/fixtures/.../layout_registry.py`)
   - Document verification date and file used in layout comments

3. **Document findings** in the parser plan (`OPPSCAP_PARSER_PLAN.md` or equivalent) before implementation.

---

## 3. Implementation Checklist (11-Step Template)

Follow the parser template in `STD-parser-contracts-impl-v2.0.md`:

1. Validate required metadata (`validate_required_metadata`).
2. Detect format / route parser.
3. Detect encoding (head read + reset).
4. Parse into DataFrame (streaming reader where feasible).
5. Normalize headers using alias map.
6. Coerce types (vectorised operations preferred).
7. Run validation rules; collect rejects.
8. Inject metadata columns.
9. Sort by natural keys.
10. Compute `row_content_hash`.
11. Assemble metrics + rejects (`ParseResult`).

**Remember:** log key metrics (`expansion_methods`, `match_methods`, `rejects_by_reason`, `authority_fingerprint`) per the metrics contract.

---

## 4. Testing Requirements

1. **Golden tests:** TXT authority + CSV parity fixtures (`@pytest.mark.golden`).
2. **Edge tests:** Handle null modifiers, threshold warnings, etc.
3. **Negative tests:** Invalid codes, negative prices, duplicate NKs.
4. **Real-source parity:** TXT vs secondary format (≥98 % NK overlap or ≤1 % row delta) with variance artefacts.
5. **Determinism test:** Re-run parser and assert hashes/ordering stable.
6. **Metrics contract assertion:** Use `validate_metrics_contract` helper.

---

## 5. Documentation & Operational Updates

1. Update `CHANGELOG.md` with parser milestones.
2. Refresh `AUTHORITY_MATRIX.md`, `SCHEMA_BREAKING_CHANGE_ANALYSIS.md`, and parser plan checkboxes.
3. Mark progress in `github_tasks_plan.md`.
4. Ensure `prds/SRC-<dataset>.md` lists final download URLs and verification notes.

---

## 6. Sign-off & Status

1. **Tests green:** unit, integration, real-source parity.
2. **Review artefacts:** rejects, metrics, parity reports.
3. **Status update:** When a parser validates this playbook, bump document status to “Ready” and note in changelog.

---

```yaml
rule_id: DOC-PARSER-BUILD-R001
clause: parser_build_checklist
summary: "Complete pre-implementation prep, schema/layout alignment, template-based implementation, full test suite, and documentation updates for each parser effort."
status: draft
```

**Next review:** after OPPSCAP parser completion.
