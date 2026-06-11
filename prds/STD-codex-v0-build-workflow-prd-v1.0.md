# Codex V0 Build Workflow Standard (v1.0)

```yaml
doc_type: STD
normative: true
requires:
  - STD-doc-governance-prd-v1.0
  - STD-data-architecture-prd-v1.0
  - STD-api-architecture-prd-v1.0
  - STD-qa-testing-prd-v1.0
```

**Status:** Draft v1.0
**Owners:** Platform/Product Operations
**Consumers:** Codex AI Agent, Engineering, Product, QA
**Change control:** PR review

**Companion Docs:**
- `STD-codex-v0-build-workflow-impl-v1.0.md` — build brief and epic brief templates

**Cross-References:**
- `DOC-master-catalog-prd-v1.0.md` — Master system catalog and dependency map
- `STD-doc-governance-prd-v1.0.md` — governed document naming and registration

---

## 1. Purpose

This standard defines the Codex v0 workflow for planning and executing build work
in this repository.

Codex v0 means Codex is the active planning and execution harness. Repo-local
scripts may support tracker state, validation, and generated views, but no
larger harness replaces Codex without a future governed migration.

## 2. Artifact Roles

| Artifact | Purpose | Location |
|---|---|---|
| PRD / STD / RUN / REF | Durable product, data, API, standards, reference, or operational contract | `prds/` |
| Epic brief | Execution plan for one bounded initiative | `docs/workbench/` |
| Tracker record | Canonical status, rank, ownership, and handoff state | `state/work/` |
| Generated views | Readable tracker views rebuilt from `state/work/` | `docs/workbench/CURRENT.md`, `docs/workbench/ROADMAP.md` |

An epic brief is not automatically a PRD. If an epic defines a durable product,
data, API, operational, or workflow contract, that durable rule must be promoted
to the appropriate governed `prds/` artifact.

## 3. When To Use The Workflow

Use this workflow before implementation when a request:

- creates a new feature, workflow, API behavior, ingestion path, or internal tool;
- changes durable behavior described by a PRD, STD, RUN, or REF;
- requires tracker decomposition into roadmap, epic, or task slices;
- touches data ingestion, public API behavior, migrations, external systems, or
  privacy/sanitization boundaries;
- is broad enough that Codex would otherwise need to choose scope mid-run.

For a narrow bug fix with obvious scope, Codex may implement directly when the
user asks for implementation. It should still run the smallest relevant
validation and report the result.

## 4. Planning Order

Before Codex starts implementation on an epic, use this order:

1. Build brief intake: capture objective, current state, desired behavior,
   scope, constraints, testing policy, and requested output.
2. Repo inspection: inspect relevant files before proposing changes.
3. Privacy / sanitization gate: decide whether private data, raw outputs,
   finance/call artifacts, secrets, browser state, or external-memory ingestion
   are involved.
4. Scope pressure test: narrow over-scoped build-driving plans to the smallest
   coherent implementation slice, and identify deferred or killed slices.
5. Tracker mapping: propose roadmap, epic, queued task slices, and deferred
   slices.
6. Approval gate: stop and wait for approval or edits before implementation.
7. Implementation: after approval, edit code or docs, run narrow validation, and
   report commands and results.

For public CMS source data work, the privacy/sanitization gate is normally a
fast not-applicable check. It becomes blocking for private data, raw runtime
outputs, secrets, external-memory ingestion, or finance/call artifacts.

## 5. Approval Gate

For planned build work, Codex must not implement until the user approves or edits
the plan.

The approved implementation instruction should be explicit:

```text
Implement the approved plan. Keep the diff minimal. Run lint and the smallest relevant test suite. Report commands and results.
```

If the user has already provided an approved, narrow plan and asks for
implementation, Codex may proceed without re-planning.

## 6. Epic Brief Semantics

An epic brief is a tracker-linked workbench artifact that owns:

- the user or system outcome;
- the current-state problem;
- in-scope and out-of-scope behavior;
- acceptance criteria;
- validation commands;
- privacy, ingestion, and external-system boundaries;
- known risks;
- stop conditions;
- ordered child task slices.

The epic brief does not own live execution state. Live execution state belongs in
`state/work/tasks/*.yaml`, especially `status`, `current_task`, `next_action`,
and `resume_from`.

## 7. Task Sizing

Tasks are ordered checkpoints under an epic. They should be large enough to
represent a meaningful execution handoff, and small enough that Codex can
validate changed behavior with the narrowest relevant check.

Create a separate task when the slice has its own:

- acceptance criterion;
- validation command;
- privacy or ingestion boundary;
- migration, schema, or public API risk;
- PR boundary;
- external approval dependency;
- likely block/resume point.

Keep work inside the current task body or epic brief checklist when it is only:

- a local implementation sub-step;
- a refactor detail inside the same changed surface;
- a test or doc update required to prove the same acceptance criterion;
- a temporary note that does not need independent scheduling.

Queued child tasks may exceed three. The active-task limit is a concurrency guard,
not a cap on how many ordered task slices an epic may contain.

### Codex Sequencing And Parallelism

Same-epic implementation tasks are sequential by default. Codex should run one
write-capable task per epic unless tracker metadata explicitly marks the next
task as parallel-safe.

Tracker task records use these harness fields for active and queued tasks:

- `depends_on`: prerequisite task IDs that must be `done` before the task is
  runnable.
- `parallel_policy`: one of `sequential`, `read_only_parallel`, or
  `independent_write`.
- `codex_mode`: one of `local`, `worktree`, `cloud`, or `manual`.

`rank` remains the human-readable order, but `depends_on` is the harness-grade
dependency source. If a dependency edge and rank disagree, Codex should stop and
ask for tracker cleanup before implementation.

Use `read_only_parallel` only for read-heavy exploration, test runs, triage,
summarization, or other non-mutating work. Subagents may be used for these
read-only activities inside a task. They must not perform parallel write-heavy
implementation unless the task is explicitly marked for safe parallel work.

Use `independent_write` only when two implementation tasks can safely run at the
same time. The tasks must have completed dependencies and must not share exact,
parent, or child `related_paths`. Worktrees may isolate filesystem changes, but
they do not replace dependency ordering or path-conflict checks.

## 8. Stop Conditions

Stop before implementation, or before continuing to another queued slice, when:

- validation fails;
- the next decision is product, privacy, data-ingestion, public API, or standards
  policy rather than implementation;
- external approval is required;
- destructive or external-network operations are required;
- a PR boundary is reached;
- the requested slice would exceed the approved scope;
- the tracker parent mapping or task handoff is ambiguous;
- a durable contract needs a PRD, STD, RUN, or REF update before code can be
  considered correct.

## 9. Validation

For workflow or tracker changes, run:

```bash
.venv/bin/python scripts/governance/check-work-tracker.py
.venv/bin/python scripts/governance/build-work-tracker.py
```

For implementation changes, run the smallest relevant test suite that proves the
changed behavior. Prefer focused tests over broad suites unless the blast radius
requires broader validation.

## 10. Change Log

| Version | Date | Summary |
|---|---|---|
| 1.1 | 2026-06-11 | Added Codex sequencing fields and same-epic parallelism rules for tracker-driven harness execution. |
| 1.0 | 2026-06-11 | Initial Codex v0 build workflow standard with approval gate, epic brief semantics, task sizing, and stop conditions. |
