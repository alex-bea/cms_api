# Codex V0 Build Workflow Implementation Guide (v1.0)

```yaml
doc_type: STD
normative: false
requires:
  - STD-codex-v0-build-workflow-prd-v1.0
```

**Status:** Draft v1.0
**Owners:** Platform/Product Operations
**Consumers:** Codex AI Agent, Engineering, Product, QA
**Change control:** PR review
**Companion to:** `STD-codex-v0-build-workflow-prd-v1.0.md`

**Cross-References:**
- `prds/DOC-master-catalog-prd-v1.0.md`
- `prds/STD-codex-v0-build-workflow-prd-v1.0.md`

---

## 1. Purpose

This companion guide provides the copy/paste templates and output checklists for
the Codex v0 build workflow standard.

## 2. Codex Build Brief Template

```md
# Codex Build Brief: [Feature Name]

## Objective
Build [feature/workflow] so that [user/system] can [desired outcome]. This is for [backend workflow / internal tool / integration / AI feature / payments workflow].

## Current State
Today, [what happens now]. The problem is [specific failure, manual step, missing capability, or risk].

## Desired Behavior
When [trigger/event/user action] happens, the system should:
1. [Expected behavior]
2. [Expected behavior]
3. [Expected behavior]

## Scope
In scope:
- [Capability 1]
- [Capability 2]
- [Capability 3]

Out of scope:
- [Explicit non-goal]
- [Explicit non-goal]
- [Explicit non-goal]

## Relevant Context
Use the existing codebase, patterns, database schema, APIs, and conventions. Before proposing changes, inspect the relevant files and identify the smallest safe implementation path.

Relevant areas to inspect:
- [File/module/service]
- [File/module/service]
- [API/schema/job/queue/integration]

## Requirements
1. [Requirement. Must be testable.]
   - Acceptance: [observable pass condition]

2. [Requirement. Must be testable.]
   - Acceptance: [observable pass condition]

3. [Requirement. Must be testable.]
   - Acceptance: [observable pass condition]

## Privacy / Sanitization Boundary
- Does this touch private data, raw outputs, finance/call artifacts, secrets, browser state, or external-memory ingestion?
- If yes, name the sanitizer, denylist/allowlist, or manual approval boundary before implementation.

## PRD / STD Impact
- Does this define or change a durable product, data, API, operational, or workflow contract?
- If yes, name the PRD, STD, RUN, or REF that must be created or updated.

## Tracker Mapping
- Roadmap:
- Epic:
- Proposed task slices:
- Deferred slices:

## Constraints
- Keep the implementation minimal.
- Reuse existing patterns before adding new abstractions.
- Do not add new production dependencies without explicit approval.
- Do not create broad refactors.
- Do not change public APIs unless explicitly required.
- Preserve existing behavior outside this feature.

## Do Not Touch
- [Auth / billing / payments ledger / permissions / schemas / cron jobs / etc.]
- [Any fragile or unrelated module]
- [Any production config or secret handling]

## Testing Policy
Write minimal useful tests only. Prefer the smallest relevant test suite that proves the changed behavior. Do not add snapshot tests, broad mock-heavy tests, or tests for unchanged framework behavior.

Required tests:
- [One core happy path test]
- [One important failure/edge case, if relevant]
- [No test required if change is config/copy-only]

## Requested Codex Output
Do not implement yet. First produce:
1. Implementation plan with files likely to change.
2. Tracker tasks in checklist format.
3. QA checklist with manual and automated verification steps.
4. Risks, assumptions, and open questions only if blocking.
```

## 3. Epic Brief Template

Store filled epic briefs under `docs/workbench/` and link them from the relevant
epic record's `plan_path`.

```md
# [Epic Name]

**Status:** [Proposed | Active | Blocked | Completed]
**Updated:** YYYY-MM-DD
**Tracker link:** `state/work/epics/[epic-id].yaml`

## Related Governance

- PRD / STD / RUN / REF:
- Source docs:
- Related workbench docs:

## Goal
Build [capability] so that [user/system] can [outcome].

## Current State
Today, [what happens now]. The gap is [specific failure, manual step, missing capability, or risk].

## Scope
In scope:
- [Capability]
- [Capability]

Out of scope:
- [Explicit non-goal]
- [Explicit non-goal]

## Acceptance Criteria
- [Observable pass condition]
- [Observable pass condition]

## Validation
- [Command or manual check]
- [Command or manual check]

## Privacy / Data Boundaries
- [Public/private data boundary]
- [External-memory, ingestion, credential, or sanitizer boundary]

## PRD / STD Impact
- [No durable contract change]
- [Or: create/update prds/... before implementation is complete]

## Known Risks
- [Risk and mitigation]

## Stop Conditions
- [Condition requiring user decision, approval, or separate task]

## Ordered Task Slices
1. [Task slice and expected validation]
2. [Task slice and expected validation]
3. [Task slice and expected validation]

## Deferred Slices
- [Deferred slice and why it is not needed for the MVP]

## Notes
- [Rationale, rollout notes, or decision history]
```

## 4. Planner Output Checklist

When responding to a build brief, Codex should output:

1. implementation plan with likely file changes;
2. smallest safe implementation path;
3. tracker checklist with queued and deferred task slices;
4. QA checklist with automated and manual checks;
5. privacy/sanitization boundary, even when it is "not applicable";
6. PRD/STD impact, even when no durable contract changes are needed;
7. blocking questions only when a safe assumption would be risky.

## 5. Implementation Follow-Up

After the user approves or edits the plan, use:

```text
Implement the approved plan. Keep the diff minimal. Run lint and the smallest relevant test suite. Report commands and results.
```

## 6. Artifact Decision Matrix

| Need | Artifact |
|---|---|
| Durable product/API/data requirement | `prds/PRD-...-prd-vX.Y.md` |
| Durable engineering standard | `prds/STD-...-prd-vX.Y.md` |
| Operational procedure | `prds/RUN-...-prd-vX.Y.md` |
| Reference architecture or source map | `prds/REF-...-vX.Y.md` |
| One bounded implementation initiative | `docs/workbench/DOC-...md` epic brief |
| Live task status and handoff | `state/work/tasks/*.yaml` |
| Generated active/roadmap view | `docs/workbench/CURRENT.md`, `docs/workbench/ROADMAP.md` |

## 7. Change Log

| Version | Date | Summary |
|---|---|---|
| 1.0 | 2026-06-11 | Initial templates and checklists for Codex build briefs and epic briefs. |
