# Work Tracker Standard

**Status:** Active
**Owner:** ClearBill CMS API maintainers
**Updated:** 2026-06-10

## Purpose

This repo uses a lightweight work tracker to preserve implementation status across
Codex sessions, pull requests, and local validation runs.

The tracker answers four questions:

- What workstreams are active?
- What initiatives belong to each workstream?
- What exact task should an agent resume?
- What evidence shows what has already been completed?

## Source Of Truth

Canonical tracker state lives in `state/work/`.

Generated human views live in `docs/workbench/` and are rebuilt from the state files.
Do not hand-edit generated views.

```
state/work/
  roadmaps/
  epics/
  tasks/

docs/workbench/
  ROADMAP.md
  CURRENT.md
```

Long-form status notes and run evidence can live in `docs/workbench/DOC-*.md`.
Those docs are evidence and context. The current work state remains in
`state/work/`.

## Record Levels

Use `roadmap` for a durable workstream, such as CMS data pipeline hardening.

Use `epic` for a bounded initiative under a roadmap, such as getting live CMS RVU
data through ingest, publish, and pricing selection.

Use `task` for one actionable execution slice with a clear resume point.

## Required Fields

Every roadmap, epic, and task record uses this base shape:

```yaml
id: cms-data-pipeline
title: CMS Data Pipeline
status: active
rank: 1
team: system
owner_mode: shared
updated_at: "2026-06-10"
plan_path: null
related_paths:
  - "cms_pricing/ingestion"
linked_beads:
linked_outputs:
summary: "Short context for humans and agents."
```

Epics also include `parent_id`, pointing to a roadmap. Tasks include `parent_id`,
pointing to an epic.

Active, blocked, and queued-for-merge tasks must include:

- `current_task`
- `next_action`
- `resume_from`

## Status Values

Allowed statuses:

- `queued`
- `active`
- `blocked`
- `queued_for_merge`
- `parked`
- `icebox`
- `done`

Allowed teams:

- `system`
- `data`
- `api`
- `ops`
- `shared`

Allowed owner modes:

- `alex`
- `agent`
- `shared`

## Operating Rule

Code PRs ship code. End-of-day tracker sync PRs reconcile tracker truth.

During the day, agents may update tracker YAML locally for working context, but
ordinary implementation PRs should not stage these files:

- `state/work/**`
- `state/plans/accepted.yaml`
- `docs/workbench/CURRENT.md`
- `docs/workbench/ROADMAP.md`

If a PR changes tracker or governance behavior, use a dedicated tracker PR.

## End-Of-Day Sync

At end of day, create one tracker sync pass:

1. Reconcile local task additions, status changes, evidence links, and resume
   fields.
2. Resolve duplicate task IDs and duplicate ranks.
3. Run merge queue dry run.
4. Rebuild tracker views once.
5. Push a tracker-only PR.

Commands:

```bash
python scripts/governance/check-work-tracker.py
python scripts/governance/process_merge_queue.py --dry-run --json
python scripts/governance/build-work-tracker.py
git diff --check
python tools/work_tracker.py check-views
```

## Conflict Avoidance

New task IDs should be globally specific and action-object scoped, such as
`load-latest-cms-rvu-local-db`, not generic names such as `add-health-command`.

If duplicate ranks appear, keep main branch ordering stable and move the new
local task to the next open rank in that epic.

If `CURRENT.md` or `ROADMAP.md` conflicts, resolve tracker YAML first, then
regenerate the views.

If multiple agents touched the same epic, reconcile ranks and statuses in the
end-of-day tracker PR before pushing.

This keeps chat history from becoming the only record of what happened.
