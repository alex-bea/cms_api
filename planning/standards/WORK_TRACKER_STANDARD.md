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

Active and blocked tasks must include:

- `current_task`
- `next_action`
- `resume_from`

## Status Values

Allowed statuses:

- `queued`
- `active`
- `blocked`
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

When a meaningful work session finishes, update the tracker before handing back:

1. Mark completed tasks `done`.
2. Add or update the next active task.
3. Link the run evidence or status doc under `linked_outputs`.
4. Rebuild generated views with `python tools/work_tracker.py build`.
5. Validate with `python tools/work_tracker.py check`.

This keeps chat history from becoming the only record of what happened.
