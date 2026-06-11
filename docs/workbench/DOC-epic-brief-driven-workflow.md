# Epic Brief Driven Tracker Workflow

**Status:** Completed
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/epic-brief-driven-workflow.yaml`

## Related Governance

- `prds/STD-codex-v0-build-workflow-prd-v1.0.md`
- `prds/STD-codex-v0-build-workflow-impl-v1.0.md`
- `prds/DOC-master-catalog-prd-v1.0.md`
- `tools/work_tracker.py`
- `docs/workbench/DOC-codex-build-brief-planner-v0.md`

## Goal

Move tracker workflow toward Codex-v0 build briefs and epic briefs as the
planning unit, with queued task slices under each epic and staged harness
behavior that starts with approved plans and dry-run orchestration before
mutating state.

## Current State

The tracker already separates roadmaps, epics, and tasks, but tiny
implementation steps can still behave like the main unit of work. That creates
overhead: activate task, update task, validate tracker, rebuild views, open PR,
merge, and repeat.

The desired workflow separates two layers:

- epic brief: the goal Codex keeps working toward;
- tasks: ordered checkpoints under that goal, not mandatory stop points after
  every small slice.

Codex is the v0 planning and execution harness. A larger harness may come later,
but the repo should first make the plan-first workflow repeatable and
validatable.

## Scope

In scope:

- Codex build brief planner v0 rules and approval workflow.
- Epic brief semantics and task sizing rules.
- Governed PRD/STD placement for repeatable workflow rules and templates.
- Minimal heading-level validation for tracker-linked epic briefs.
- Dry-run orchestration before any tracker mutation automation.

Out of scope:

- Replacing Codex with Hermes, IronClaw, or another harness.
- Auto-executing implementation work from tracker state.
- Treating generated views, run evidence, or external memory as canonical
  tracker state.
- Broad refactors to the tracker YAML parser beyond the minimum needed
  validation.

## Acceptance Criteria

- Codex v0 build workflow rules live in a governed STD.
- Copy/paste build brief and epic brief templates live in a companion
  implementation guide.
- This workbench document remains the filled epic brief for the tracker epic,
  not the global template.
- Tracker validation catches missing required headings for documents that
  identify themselves as tracker-linked epic briefs.
- Existing non-brief plan/status docs can remain linked from epics without
  being forced into the epic brief template.
- Generated tracker views rebuild cleanly.

## Validation

- `.venv/bin/python scripts/governance/check-work-tracker.py`
- `.venv/bin/python scripts/governance/build-work-tracker.py`
- `.venv/bin/python -m pytest tests/tools/test_work_tracker.py -q`
- `.venv/bin/python -m tools.audit_doc_catalog`
- `.venv/bin/python -m pytest tests/prd_docs -q`

## Privacy / Data Boundaries

This workflow work uses public repository docs, tracker YAML, and local tests.
It does not ingest private finance, call, browser-state, credential, or
external-memory artifacts.

For future CMS API work, the sanitizer gate is normally a fast not-applicable
check because initial data sources are public CMS releases. It becomes blocking
if a brief introduces private customer/provider data, raw runtime outputs,
secrets, database dumps, browser state, or external-memory ingestion.

## PRD / STD Impact

Repeatable workflow rules and templates now belong in:

- `prds/STD-codex-v0-build-workflow-prd-v1.0.md`
- `prds/STD-codex-v0-build-workflow-impl-v1.0.md`

This workbench document is the epic-specific execution brief and rollout note.
It should not become the durable source of truth for global Codex workflow
rules.

## Known Risks

- The existing tracker parser is intentionally simple, so heading-level
  validation should stay conservative until the workflow proves useful.
- Enforcing the epic brief template on every existing epic would break useful
  status docs; validation should apply only to documents that identify
  themselves as tracker-linked epic briefs.
- A standalone harness could duplicate tracker parsing and state mutation. Any
  future harness script should reuse the existing tracker core.
- The active-task limit currently counts only `status: active`. Treating
  blocked or queued-for-merge work as WIP needs a separate policy decision.

## Stop Conditions

Stop a Codex run before continuing to the next queued slice when:

- validation fails;
- the next decision is product, privacy, data-ingestion, public API, or
  standards policy rather than implementation;
- external approval is required;
- destructive or external-network operations are required;
- a PR boundary is reached;
- the requested slice would exceed the approved scope;
- the tracker parent mapping or task handoff is ambiguous.

## Ordered Task Slices

1. Add the Codex build brief planner v0 prompt and approval workflow.
   - Status: done.
   - Evidence: `docs/workbench/DOC-codex-build-brief-planner-v0.md`.
2. Document epic brief semantics and task sizing.
   - Status: done.
   - Evidence: this epic brief.
3. Promote repeatable Codex v0 workflow rules and templates into governed PRD
   docs.
   - Status: done.
   - Evidence: `prds/STD-codex-v0-build-workflow-prd-v1.0.md` and
     `prds/STD-codex-v0-build-workflow-impl-v1.0.md`.
4. Add and validate minimal epic metadata.
   - Status: done.
   - Validation: heading-level checks for tracker-linked epic briefs.
5. Add `run-epic --dry-run`.
   - Status: done.
   - Validation: prints ordered queued slices and stop rules without mutation.
6. Add epic-run apply state mode.
   - Status: done.
   - Validation: activates one queued child task at a time and rebuilds views
     without exceeding active WIP.
7. Add epic-run validation execution.
   - Status: done.
   - Validation: runs allowlisted validation commands and stops on failure.
8. Pilot on one real epic before generalizing.
   - Status: done.
   - Validation: capture pilot results and update this brief with findings.

## Pilot Results

Pilot epic: `cms-rvu-ingestion`.

Commands exercised:

- `.venv/bin/python tools/work_tracker.py run-epic --dry-run --epic-id cms-rvu-ingestion --max-slices 2`
- `.venv/bin/python tools/work_tracker.py run-epic --apply-state --epic-id cms-rvu-ingestion`
- `.venv/bin/python tools/work_tracker.py run-epic --validate --epic-id cms-rvu-ingestion`
- `.venv/bin/python tools/work_tracker.py run-epic --apply-state --epic-id cms-rvu-ingestion`

Observed behavior:

- Dry-run selected `normalize-rvu-locality-for-geography-resolution` before
  `add-post-rvu-load-api-smoke-command`, matching task rank order.
- Apply-state activated `normalize-rvu-locality-for-geography-resolution`,
  updated generated tracker views, and left existing active RVU work intact.
- Validation mode returned success with no commands because
  `docs/workbench/DOC-cms-rvu-local-db-load-status.md` is a status document, not
  a tracker-linked epic brief with a `Validation` section.
- A second apply-state attempt stopped with `active task WIP is full: 3/3`,
  confirming the WIP gate blocks additional activation.

Rollout decision:

- Use `run-epic` as an opt-in Codex v0 tracker workflow.
- Do not make `run-epic --validate` the default quality gate for arbitrary
  epics until their plan docs include explicit `Validation` and
  `Stop Conditions` sections.
- Keep automatic implementation execution out of scope. The command may select
  and activate tracker slices, but Codex still performs implementation only
  after the user asks for that slice.

## Deferred Slices

- Full semantic parsing of epic brief bodies.
- Automatic migration of old status docs into epic briefs.
- Harness-managed implementation execution.
- IronClaw or other harness migration.

## Notes

The tracker should remain an orchestration layer. Implementation, review, and
privacy-sensitive decisions stay outside automation until dry-run behavior and
state mutation are reliable.
