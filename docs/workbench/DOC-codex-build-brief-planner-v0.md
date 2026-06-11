# Codex Build Brief Planner V0

**Status:** Proposed
**Updated:** 2026-06-11
**Tracker link:** `state/work/tasks/add-codex-build-brief-planner-v0.yaml`

## Purpose

Use Codex as the v0 workflow harness for build planning before adding any larger
tracker harness. This workbench note records rollout status for the governed
workflow now defined in:

- `prds/STD-codex-v0-build-workflow-prd-v1.0.md`
- `prds/STD-codex-v0-build-workflow-impl-v1.0.md`

The governed standard owns repeatable rules. The companion implementation guide
owns the copy/paste build brief and epic brief templates.

## Layering

The v0 planner composes these responsibilities:

- Build brief intake: capture objective, current state, desired behavior, scope,
  requirements, constraints, testing policy, and output shape.
- Scope pressure test: classify the work as a build-driving plan and narrow it to
  the smallest useful slice when it is over-scoped.
- Tracker mapping: propose roadmap, epic, task slices, deferred slices, and the
  minimum state updates needed.
- Sanitizer boundary: identify private data, raw outputs, finance/call artifacts,
  secrets, browser state, or external-memory ingestion before implementation.
- Approval gate: stop after the plan and wait for approval before changing code.

Codex remains the v0 harness. Repo-local scripts and future adapters may support
tracker state, validation, and run envelopes, but they do not replace Codex until
a separate migration explicitly accepts another harness.

## Skill Layer Order

Before Codex starts implementation on an epic, use the workflow layers in this
order:

1. Codex Build Brief Planner V0: intake the brief, inspect repo context, and
   produce a plan only.
2. Privacy / Sanitization Gate: decide whether private data, raw outputs,
   finance/call artifacts, secrets, browser state, or external-memory ingestion
   are involved. For public CMS data work, this is normally a fast not-applicable
   check.
3. Scope pressure test: narrow over-scoped build-driving plans to the smallest
   useful slice, and separate deferred or killed slices.
4. Plan scaffolding: convert the approved plan into roadmap, epic, task, and
   deferred-slice mapping.
5. Tracker maintenance: write canonical `state/work` records, rebuild generated
   views, and validate tracker state.
6. Approval follow-up: wait for the explicit implementation instruction.
7. Codex implementation: edit code or docs, run narrow validation, and report
   commands and results.

The first five layers may be a single Codex planning response for a small change.
For broad epics, keep the approval gate explicit before implementation.

## Template Source

Use `prds/STD-codex-v0-build-workflow-impl-v1.0.md` for the current:

- Codex build brief template;
- epic brief template;
- planner output checklist;
- artifact decision matrix.

## Approval Follow-Up

After the plan is approved or edited, use a second instruction:

```text
Implement the approved plan. Keep the diff minimal. Run lint and the smallest relevant test suite. Report commands and results.
```

## Planner Output Contract

The planning response should include:

1. files likely to change and why;
2. smallest safe implementation path;
3. tracker checklist with queued slices and deferred slices;
4. QA checklist with automated and manual checks;
5. privacy/sanitization boundary if relevant;
6. blocking questions only when a safe assumption would be risky.

The planner should not write code during this phase.

## Stop Conditions

Stop before implementation when:

- the request touches private finance/call/runtime data without an approved
  sanitizer or manual boundary;
- the requested behavior would require a new production dependency;
- the plan changes a public API or schema without explicit approval;
- the smallest useful slice cannot be identified;
- the tracker parent mapping is ambiguous;
- destructive or external-network operations are required.
