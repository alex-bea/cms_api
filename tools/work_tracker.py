#!/usr/bin/env python3
"""Validate and render the repo-native CMS API work tracker."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = Path("state/work")
ROADMAPS_DIR = STATE_DIR / "roadmaps"
EPICS_DIR = STATE_DIR / "epics"
TASKS_DIR = STATE_DIR / "tasks"
ROADMAP_VIEW = Path("docs/workbench/ROADMAP.md")
CURRENT_VIEW = Path("docs/workbench/CURRENT.md")

STATUSES = {
    "queued",
    "active",
    "blocked",
    "queued_for_merge",
    "parked",
    "icebox",
    "done",
}
OWNER_MODES = {"alex", "agent", "shared"}
TEAMS = {"system", "data", "api", "ops", "shared"}
ACTIVE_TASK_LIMIT = 3
RUN_EPIC_STOP_STATUSES = {"blocked", "queued_for_merge"}
VALIDATION_PYTHON_EXECUTABLES = {".venv/bin/python", "python", "python3"}
VALIDATION_SCRIPT_PREFIXES = ("scripts/governance/",)
VALIDATION_MODULES = {"pytest", "tools.audit_doc_catalog"}
VALIDATION_PATH_PREFIXES = ("tests/",)
STATUS_ORDER = {
    "active": 0,
    "blocked": 1,
    "queued_for_merge": 2,
    "queued": 3,
    "parked": 4,
    "icebox": 5,
    "done": 6,
}
KIND_ORDER = ("roadmaps", "epics", "tasks")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
EPIC_BRIEF_REQUIRED_HEADINGS = (
    "Related Governance",
    "Goal",
    "Current State",
    "Scope",
    "Acceptance Criteria",
    "Validation",
    "Privacy / Data Boundaries",
    "PRD / STD Impact",
    "Known Risks",
    "Stop Conditions",
    "Ordered Task Slices",
)
HEADING_PATTERN = re.compile(r"^#{2,6}\s+(.+?)\s*$")


@dataclass
class ValidationResult:
    errors: list[str]
    warnings: list[str]


@dataclass
class TrackerData:
    root: Path
    roadmaps: list[dict[str, Any]]
    epics: list[dict[str, Any]]
    tasks: list[dict[str, Any]]

    @property
    def roadmaps_by_id(self) -> dict[str, dict[str, Any]]:
        return {record["id"]: record for record in self.roadmaps}

    @property
    def epics_by_id(self) -> dict[str, dict[str, Any]]:
        return {record["id"]: record for record in self.epics}


class EpicRunStateError(ValueError):
    """Raised when run-epic state cannot be safely advanced."""


def parse_scalar(raw: str) -> Any:
    value = raw.strip()
    if value in {"", "null", "None", "~"}:
        return None
    if value == "[]":
        return []
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    if value.isdigit():
        return int(value)
    return value


def load_tracker_file(path: Path) -> dict[str, Any]:
    record: dict[str, Any] = {}
    current_list_key: str | None = None
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if raw_line.startswith("  - "):
            if current_list_key is None:
                raise ValueError(
                    f"{path}:{line_number}: list item without a list field"
                )
            record[current_list_key].append(parse_scalar(raw_line[4:]))
            continue
        if raw_line.startswith(" "):
            raise ValueError(f"{path}:{line_number}: unsupported indentation")
        if ":" not in raw_line:
            raise ValueError(f"{path}:{line_number}: expected 'key: value'")

        key, value = raw_line.split(":", 1)
        key = key.strip()
        parsed = parse_scalar(value)
        if parsed is None and not value.strip():
            parsed = []
            current_list_key = key
        else:
            current_list_key = key if isinstance(parsed, list) else None
        record[key] = parsed

    record["_path"] = path
    return record


def load_records(root: Path, directory: Path) -> list[dict[str, Any]]:
    full_dir = root / directory
    if not full_dir.exists():
        return []
    return [load_tracker_file(path) for path in sorted(full_dir.glob("*.yaml"))]


def load_tracker(root: Path | None = None) -> TrackerData:
    base = root or REPO_ROOT
    return TrackerData(
        root=base,
        roadmaps=load_records(base, ROADMAPS_DIR),
        epics=load_records(base, EPICS_DIR),
        tasks=load_records(base, TASKS_DIR),
    )


def resolve_repo_path(root: Path, value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    return path if path.is_absolute() else root / path


def relative_repo_path(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def render_tracker_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(value)
    text = str(value)
    if re.match(r"^[A-Za-z0-9_-]+$", text):
        return text
    escaped = text.replace('"', '\\"')
    return f'"{escaped}"'


def update_tracker_fields(path: Path, fields: dict[str, Any]) -> None:
    pending = dict(fields)
    lines = path.read_text(encoding="utf-8").splitlines()
    updated_lines: list[str] = []
    for line in lines:
        key = line.split(":", 1)[0].strip() if ":" in line and line[:1] != " " else None
        if key in pending:
            updated_lines.append(f"{key}: {render_tracker_scalar(pending.pop(key))}")
        else:
            updated_lines.append(line)

    for key, value in pending.items():
        updated_lines.append(f"{key}: {render_tracker_scalar(value)}")

    path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def path_link(root: Path, view_path: Path, target: str | None) -> str:
    if not target:
        return "`None`"
    resolved = resolve_repo_path(root, target)
    if resolved is None:
        return f"`{target}`"
    rel = os.path.relpath(resolved, (root / view_path).parent)
    return f"[`{target}`]({rel})"


def validate_common(
    root: Path, kind: str, record: dict[str, Any], errors: list[str]
) -> None:
    required = (
        "id",
        "title",
        "status",
        "rank",
        "team",
        "owner_mode",
        "updated_at",
        "plan_path",
        "related_paths",
        "linked_beads",
        "linked_outputs",
    )
    label = str(record.get("_path", f"{kind}:{record.get('id', '?')}"))
    for field in required:
        if field not in record:
            errors.append(f"{label}: missing required field '{field}'")

    record_id = str(record.get("id", "")).strip()
    if record_id and not SLUG_PATTERN.match(record_id):
        errors.append(f"{label}: id must be lowercase kebab-case")

    status = str(record.get("status", "")).strip()
    if status not in STATUSES:
        errors.append(f"{label}: invalid status '{status}'")

    owner_mode = str(record.get("owner_mode", "")).strip()
    if owner_mode not in OWNER_MODES:
        errors.append(f"{label}: invalid owner_mode '{owner_mode}'")

    team = str(record.get("team", "")).strip()
    if team not in TEAMS:
        errors.append(f"{label}: invalid team '{team}'")

    rank = record.get("rank")
    if not isinstance(rank, int) or rank < 1:
        errors.append(f"{label}: rank must be a positive integer")

    try:
        date.fromisoformat(str(record.get("updated_at", "")))
    except ValueError:
        errors.append(f"{label}: updated_at must be YYYY-MM-DD")

    plan_path = record.get("plan_path")
    if plan_path:
        resolved = resolve_repo_path(root, str(plan_path))
        if resolved is None or not resolved.exists():
            errors.append(f"{label}: plan_path does not exist: {plan_path}")

    for field in ("related_paths", "linked_beads", "linked_outputs"):
        value = record.get(field)
        if not isinstance(value, list):
            errors.append(f"{label}: '{field}' must be a list")
            continue
        for item in value:
            resolved = resolve_repo_path(root, str(item))
            if resolved is None or not resolved.exists():
                errors.append(f"{label}: referenced path does not exist: {item}")


def validate_unique_ids(
    kind: str, records: list[dict[str, Any]], errors: list[str]
) -> None:
    seen: set[str] = set()
    for record in records:
        record_id = str(record.get("id", "")).strip()
        if not record_id:
            continue
        if record_id in seen:
            errors.append(
                f"{record.get('_path')}: duplicate {kind[:-1]} id '{record_id}'"
            )
        seen.add(record_id)


def validate_ranks(
    records: list[dict[str, Any]], lane_label: str, errors: list[str]
) -> None:
    ranks: dict[int, str] = {}
    for record in records:
        if record.get("status") == "done":
            continue
        rank = record.get("rank")
        if not isinstance(rank, int):
            continue
        if rank in ranks:
            errors.append(
                f"{record.get('_path')}: duplicate active rank {rank} in {lane_label}"
            )
        ranks[rank] = str(record.get("_path"))


def markdown_headings(text: str) -> set[str]:
    headings = set()
    for line in text.splitlines():
        match = HEADING_PATTERN.match(line)
        if match:
            headings.add(match.group(1).strip())
    return headings


def markdown_section_lines(text: str, heading: str) -> list[str]:
    lines = text.splitlines()
    collected: list[str] = []
    in_section = False
    for line in lines:
        match = HEADING_PATTERN.match(line)
        if match:
            current_heading = match.group(1).strip()
            if in_section and current_heading != heading:
                break
            in_section = current_heading == heading
            continue
        if not in_section:
            continue

        stripped = line.strip()
        if not stripped or stripped == "```":
            continue
        if stripped.startswith(("- ", "* ")):
            stripped = stripped[2:].strip()
            collected.append(stripped)
        elif re.match(r"^\d+\.\s+", stripped):
            stripped = re.sub(r"^\d+\.\s+", "", stripped).strip()
            collected.append(stripped)
        elif collected:
            collected[-1] = f"{collected[-1]} {stripped}"
        else:
            collected.append(stripped)
    return collected


def is_epic_brief(text: str, epic_id: str) -> bool:
    return f"state/work/epics/{epic_id}.yaml" in text


def validate_epic_brief(
    root: Path, epic: dict[str, Any], errors: list[str]
) -> None:
    plan_path = epic.get("plan_path")
    if not plan_path:
        return
    resolved = resolve_repo_path(root, str(plan_path))
    if resolved is None or not resolved.exists() or resolved.suffix.lower() != ".md":
        return

    text = resolved.read_text(encoding="utf-8")
    epic_id = str(epic.get("id", "")).strip()
    if not epic_id or not is_epic_brief(text, epic_id):
        return

    headings = markdown_headings(text)
    missing = [
        heading
        for heading in EPIC_BRIEF_REQUIRED_HEADINGS
        if heading not in headings
    ]
    if missing:
        errors.append(
            f"{epic.get('_path')}: epic brief {plan_path} missing required heading(s): "
            f"{', '.join(missing)}"
        )


def validate_tracker(tracker: TrackerData) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    for kind in KIND_ORDER:
        records = getattr(tracker, kind)
        validate_unique_ids(kind, records, errors)
        for record in records:
            validate_common(tracker.root, kind, record, errors)

    roadmap_ids = set(tracker.roadmaps_by_id)
    epic_ids = set(tracker.epics_by_id)

    epics_by_roadmap: dict[str, list[dict[str, Any]]] = {}
    for epic in tracker.epics:
        parent_id = str(epic.get("parent_id", "")).strip()
        if not parent_id:
            errors.append(f"{epic.get('_path')}: missing required field 'parent_id'")
        elif parent_id not in roadmap_ids:
            errors.append(
                f"{epic.get('_path')}: parent roadmap '{parent_id}' does not exist"
            )
        validate_epic_brief(tracker.root, epic, errors)
        epics_by_roadmap.setdefault(parent_id, []).append(epic)

    active_tasks = []
    tasks_by_epic: dict[str, list[dict[str, Any]]] = {}
    for task in tracker.tasks:
        parent_id = str(task.get("parent_id", "")).strip()
        if not parent_id:
            errors.append(f"{task.get('_path')}: missing required field 'parent_id'")
        elif parent_id not in epic_ids:
            errors.append(
                f"{task.get('_path')}: parent epic '{parent_id}' does not exist"
            )
        tasks_by_epic.setdefault(parent_id, []).append(task)
        if task.get("status") in {"active", "blocked", "queued_for_merge"}:
            active_tasks.append(task)
            for field in ("current_task", "next_action", "resume_from"):
                if not str(task.get(field, "")).strip():
                    errors.append(
                        f"{task.get('_path')}: {field} is required for active/blocked/queued_for_merge tasks"
                    )

    if (
        len([task for task in active_tasks if task.get("status") == "active"])
        > ACTIVE_TASK_LIMIT
    ):
        errors.append(
            f"state/work/tasks: active task count exceeds limit {ACTIVE_TASK_LIMIT}"
        )

    validate_ranks(tracker.roadmaps, "roadmaps", errors)
    for roadmap_id, epics in epics_by_roadmap.items():
        validate_ranks(epics, f"epics/{roadmap_id}", errors)
    for epic_id, tasks in tasks_by_epic.items():
        validate_ranks(tasks, f"tasks/{epic_id}", errors)

    if not tracker.roadmaps:
        warnings.append("No roadmap records found")
    if not tracker.tasks:
        warnings.append("No task records found")

    return ValidationResult(errors=errors, warnings=warnings)


def sort_by_rank(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda record: (
            int(record.get("rank", 9999)),
            STATUS_ORDER.get(str(record.get("status", "")), 99),
            str(record.get("title", "")).lower(),
        ),
    )


def task_counts(tasks: list[dict[str, Any]]) -> str:
    counts = {status: 0 for status in STATUSES}
    for task in tasks:
        counts[str(task.get("status"))] = counts.get(str(task.get("status")), 0) + 1
    ordered = [
        "active",
        "blocked",
        "queued_for_merge",
        "queued",
        "parked",
        "icebox",
        "done",
    ]
    parts = [f"{counts[status]} {status}" for status in ordered if counts.get(status)]
    return ", ".join(parts) if parts else "0 tasks"


def build_roadmap_view(tracker: TrackerData) -> str:
    epics_by_roadmap: dict[str, list[dict[str, Any]]] = {
        record["id"]: [] for record in tracker.roadmaps
    }
    tasks_by_epic: dict[str, list[dict[str, Any]]] = {
        record["id"]: [] for record in tracker.epics
    }
    for epic in tracker.epics:
        epics_by_roadmap.setdefault(epic["parent_id"], []).append(epic)
    for task in tracker.tasks:
        tasks_by_epic.setdefault(task["parent_id"], []).append(task)

    lines = [
        "# CMS API Work Roadmap",
        "",
        "_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._",
        "",
    ]
    for roadmap in sort_by_rank(tracker.roadmaps):
        lines.extend(
            [
                f"## [{roadmap['rank']}] {roadmap['title']}",
                "",
                f"- Status: `{roadmap['status']}`",
                f"- Team: `{roadmap['team']}`",
                f"- Owner mode: `{roadmap['owner_mode']}`",
                f"- Updated: `{roadmap['updated_at']}`",
                f"- Plan: {path_link(tracker.root, ROADMAP_VIEW, roadmap.get('plan_path'))}",
            ]
        )
        summary = str(roadmap.get("summary", "")).strip()
        if summary:
            lines.append(f"- Summary: {summary}")
        lines.extend(["", "### Epics", ""])

        epics = sort_by_rank(epics_by_roadmap.get(roadmap["id"], []))
        if not epics:
            lines.extend(["- None.", ""])
            continue

        for epic in epics:
            epic_tasks = sort_by_rank(tasks_by_epic.get(epic["id"], []))
            lines.append(
                f"- [{epic['rank']}] {epic['title']} - `{epic['status']}` "
                f"({task_counts(epic_tasks)})"
            )
            lines.append(f"  Team: `{epic['team']}`")
            lines.append(
                f"  Plan: {path_link(tracker.root, ROADMAP_VIEW, epic.get('plan_path'))}"
            )
            summary = str(epic.get("summary", "")).strip()
            if summary:
                lines.append(f"  Summary: {summary}")
            current = [
                task["title"]
                for task in epic_tasks
                if task.get("status") in {"active", "blocked", "queued_for_merge"}
            ]
            if current:
                lines.append(f"  Current tasks: {', '.join(current)}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_task_block(tracker: TrackerData, task: dict[str, Any]) -> list[str]:
    epic = tracker.epics_by_id[task["parent_id"]]
    roadmap = tracker.roadmaps_by_id[epic["parent_id"]]
    plan_path = (
        task.get("plan_path") or epic.get("plan_path") or roadmap.get("plan_path")
    )
    lines = [
        f"### [{roadmap['rank']}.{epic['rank']}.{task['rank']}] {task['title']}",
        "",
        f"- Status: `{task['status']}`",
        f"- Roadmap: `{roadmap['title']}`",
        f"- Epic: `{epic['title']}`",
        f"- Team: `{task['team']}`",
        f"- Owner mode: `{task['owner_mode']}`",
        f"- Updated: `{task['updated_at']}`",
        f"- Plan: {path_link(tracker.root, CURRENT_VIEW, plan_path)}",
        f"- Current task: {task.get('current_task', '') or 'None'}",
        f"- Next action: {task.get('next_action', '') or 'None'}",
        f"- Resume from: {task.get('resume_from', '') or 'None'}",
    ]
    linked_outputs = task.get("linked_outputs", []) or []
    if linked_outputs:
        links = ", ".join(
            path_link(tracker.root, CURRENT_VIEW, str(path)) for path in linked_outputs
        )
        lines.append(f"- Linked outputs: {links}")
    return lines + [""]


def build_current_view(tracker: TrackerData, warnings: list[str] | None = None) -> str:
    def task_sort_key(task: dict[str, Any]) -> tuple[int, int, int, str]:
        epic = tracker.epics_by_id[task["parent_id"]]
        roadmap = tracker.roadmaps_by_id[epic["parent_id"]]
        return (
            int(roadmap.get("rank", 9999)),
            int(epic.get("rank", 9999)),
            int(task.get("rank", 9999)),
            str(task.get("title", "")).lower(),
        )

    active_tasks = sorted(
        [task for task in tracker.tasks if task.get("status") == "active"],
        key=task_sort_key,
    )
    blocked_tasks = sorted(
        [task for task in tracker.tasks if task.get("status") == "blocked"],
        key=task_sort_key,
    )
    queued_for_merge_tasks = sorted(
        [task for task in tracker.tasks if task.get("status") == "queued_for_merge"],
        key=task_sort_key,
    )

    lines = [
        "# CMS API Current Work",
        "",
        "_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._",
        "",
        f"- Active task WIP: **{len(active_tasks)}/{ACTIVE_TASK_LIMIT}**",
        "",
    ]
    if warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    lines.extend(["## Active Tasks", ""])
    if active_tasks:
        for task in active_tasks:
            lines.extend(render_task_block(tracker, task))
    else:
        lines.extend(["- None.", ""])

    lines.extend(["## Blocked Tasks", ""])
    if blocked_tasks:
        for task in blocked_tasks:
            lines.extend(render_task_block(tracker, task))
    else:
        lines.extend(["- None.", ""])

    lines.extend(["## Queued For Merge", ""])
    if queued_for_merge_tasks:
        for task in queued_for_merge_tasks:
            lines.extend(render_task_block(tracker, task))
    else:
        lines.extend(["- None.", ""])

    return "\n".join(lines).rstrip() + "\n"


def write_views(tracker: TrackerData, warnings: list[str] | None = None) -> list[Path]:
    roadmap_path = tracker.root / ROADMAP_VIEW
    current_path = tracker.root / CURRENT_VIEW
    roadmap_path.parent.mkdir(parents=True, exist_ok=True)
    roadmap_path.write_text(build_roadmap_view(tracker), encoding="utf-8")
    current_path.write_text(
        build_current_view(tracker, warnings=warnings), encoding="utf-8"
    )
    return [roadmap_path, current_path]


def check_views_command(root: Path) -> int:
    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    if result.errors:
        for error in result.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    expected = {
        ROADMAP_VIEW: build_roadmap_view(tracker),
        CURRENT_VIEW: build_current_view(tracker, warnings=result.warnings),
    }
    drifted: list[Path] = []
    for rel_path, expected_text in expected.items():
        path = root / rel_path
        actual_text = path.read_text(encoding="utf-8") if path.exists() else None
        if actual_text != expected_text:
            drifted.append(rel_path)

    if drifted:
        for rel_path in drifted:
            print(
                f"ERROR: generated tracker view is stale: {rel_path}", file=sys.stderr
            )
        print("Run: python scripts/governance/build-work-tracker.py", file=sys.stderr)
        return 1

    print("Generated tracker views are current.")
    return 0


def check_command(root: Path) -> int:
    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    for error in result.errors:
        print(f"ERROR: {error}", file=sys.stderr)
    for warning in result.warnings:
        print(f"WARN: {warning}")
    if result.errors:
        return 1
    print("Work tracker is valid.")
    return 0


def build_command(root: Path) -> int:
    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    if result.errors:
        for error in result.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    written = write_views(tracker, warnings=result.warnings)
    for path in written:
        print(f"Wrote {path.relative_to(root)}")
    for warning in result.warnings:
        print(f"WARN: {warning}")
    return 0


def task_payload(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": task.get("id"),
        "title": task.get("title"),
        "rank": task.get("rank"),
        "status": task.get("status"),
        "team": task.get("team"),
        "owner_mode": task.get("owner_mode"),
        "plan_path": task.get("plan_path"),
        "current_task": task.get("current_task"),
        "next_action": task.get("next_action"),
        "resume_from": task.get("resume_from"),
    }


def build_epic_run_dry_run_payload(
    tracker: TrackerData, epic_id: str, max_slices: int | None = None
) -> dict[str, Any]:
    epic = tracker.epics_by_id.get(epic_id)
    if epic is None:
        raise KeyError(epic_id)

    queued_tasks = sort_by_rank(
        [
            task
            for task in tracker.tasks
            if task.get("parent_id") == epic_id and task.get("status") == "queued"
        ]
    )
    selected_tasks = queued_tasks[:max_slices] if max_slices is not None else queued_tasks

    validation_commands: list[str] = []
    stop_conditions: list[str] = []
    plan_path = epic.get("plan_path")
    if plan_path:
        resolved = resolve_repo_path(tracker.root, str(plan_path))
        if resolved is not None and resolved.exists() and resolved.suffix.lower() == ".md":
            text = resolved.read_text(encoding="utf-8")
            validation_commands = markdown_section_lines(text, "Validation")
            stop_conditions = markdown_section_lines(text, "Stop Conditions")

    active_count = len([task for task in tracker.tasks if task.get("status") == "active"])
    return {
        "dry_run": True,
        "epic": {
            "id": epic.get("id"),
            "title": epic.get("title"),
            "status": epic.get("status"),
            "rank": epic.get("rank"),
            "plan_path": plan_path,
        },
        "active_task_wip": {
            "current": active_count,
            "limit": ACTIVE_TASK_LIMIT,
        },
        "max_slices": max_slices,
        "total_queued_tasks": len(queued_tasks),
        "selected_task_count": len(selected_tasks),
        "tasks": [task_payload(task) for task in selected_tasks],
        "validation_commands": validation_commands,
        "stop_conditions": stop_conditions,
        "mutations": [],
    }


def blocking_epic_run_tasks(tracker: TrackerData, epic_id: str) -> list[dict[str, Any]]:
    return sort_by_rank(
        [
            task
            for task in tracker.tasks
            if task.get("parent_id") == epic_id
            and task.get("status") in RUN_EPIC_STOP_STATUSES
        ]
    )


def validation_commands_for_epic(tracker: TrackerData, epic: dict[str, Any]) -> list[str]:
    plan_path = epic.get("plan_path")
    if not plan_path:
        return []
    resolved = resolve_repo_path(tracker.root, str(plan_path))
    if resolved is None or not resolved.exists() or resolved.suffix.lower() != ".md":
        return []
    commands = markdown_section_lines(
        resolved.read_text(encoding="utf-8"), "Validation"
    )
    return [
        command[1:-1].strip()
        if command.startswith("`") and command.endswith("`")
        else command
        for command in commands
    ]


def validate_validation_command(root: Path, command: str) -> list[str]:
    try:
        args = shlex.split(command)
    except ValueError as exc:
        raise EpicRunStateError(f"invalid validation command syntax: {command}") from exc

    if not args:
        raise EpicRunStateError("validation command is empty")

    executable = args[0]
    if executable not in VALIDATION_PYTHON_EXECUTABLES:
        raise EpicRunStateError(
            f"validation command executable is not allowlisted: {executable}"
        )

    if len(args) < 2:
        raise EpicRunStateError(f"validation command has no target: {command}")

    if args[1] == "-m":
        if len(args) < 3 or args[2] not in VALIDATION_MODULES:
            module = args[2] if len(args) >= 3 else ""
            raise EpicRunStateError(
                f"validation module is not allowlisted: {module}"
            )
        for arg in args[3:]:
            if arg.startswith("-"):
                continue
            if arg.startswith(VALIDATION_PATH_PREFIXES):
                resolved = resolve_repo_path(root, arg)
                if resolved is None or not resolved.exists():
                    raise EpicRunStateError(
                        f"validation path does not exist: {arg}"
                    )
                continue
            if re.match(r"^[A-Za-z0-9_.:-]+$", arg):
                continue
            raise EpicRunStateError(
                f"validation argument is not allowlisted: {arg}"
            )
        return args

    target = args[1]
    if not target.startswith(VALIDATION_SCRIPT_PREFIXES):
        raise EpicRunStateError(
            f"validation script is not allowlisted: {target}"
        )
    resolved_target = resolve_repo_path(root, target)
    if resolved_target is None or not resolved_target.exists():
        raise EpicRunStateError(f"validation script does not exist: {target}")
    for arg in args[2:]:
        if not re.match(r"^[-A-Za-z0-9_./:=]+$", arg):
            raise EpicRunStateError(
                f"validation argument is not allowlisted: {arg}"
            )
    return args


def run_validation_command(root: Path, command: str) -> dict[str, Any]:
    args = validate_validation_command(root, command)
    completed = subprocess.run(
        args,
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "command": command,
        "return_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def build_epic_run_validation_payload(root: Path, epic_id: str) -> dict[str, Any]:
    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    if result.errors:
        raise EpicRunStateError("; ".join(result.errors))

    epic = tracker.epics_by_id.get(epic_id)
    if epic is None:
        raise KeyError(epic_id)

    payload = build_epic_run_dry_run_payload(tracker, epic_id)
    payload["dry_run"] = False
    payload["validate"] = True
    payload["results"] = []
    payload["success"] = True

    for command in validation_commands_for_epic(tracker, epic):
        command_result = run_validation_command(root, command)
        payload["results"].append(command_result)
        if command_result["return_code"] != 0:
            payload["success"] = False
            break

    return payload


def build_epic_run_apply_state_payload(
    root: Path, epic_id: str, today: date | None = None
) -> dict[str, Any]:
    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    if result.errors:
        raise EpicRunStateError("; ".join(result.errors))
    if epic_id not in tracker.epics_by_id:
        raise KeyError(epic_id)

    blocking_tasks = blocking_epic_run_tasks(tracker, epic_id)
    if blocking_tasks:
        blocked = ", ".join(
            f"{task.get('id')} ({task.get('status')})" for task in blocking_tasks
        )
        raise EpicRunStateError(
            f"epic has blocked or merge-boundary task(s): {blocked}"
        )

    active_count = len([task for task in tracker.tasks if task.get("status") == "active"])
    if active_count >= ACTIVE_TASK_LIMIT:
        raise EpicRunStateError(
            f"active task WIP is full: {active_count}/{ACTIVE_TASK_LIMIT}"
        )

    payload = build_epic_run_dry_run_payload(tracker, epic_id, max_slices=1)
    payload["dry_run"] = False
    payload["apply_state"] = True
    payload["mutations"] = []
    payload["written_views"] = []

    if not payload["tasks"]:
        payload["message"] = "No queued child tasks to activate."
        return payload

    selected = payload["tasks"][0]
    task = next(task for task in tracker.tasks if task.get("id") == selected["id"])
    task_path = Path(task["_path"])
    updated_at = (today or date.today()).isoformat()
    update_tracker_fields(task_path, {"status": "active", "updated_at": updated_at})

    tracker_after = load_tracker(root)
    result_after = validate_tracker(tracker_after)
    if result_after.errors:
        raise EpicRunStateError("; ".join(result_after.errors))

    written = write_views(tracker_after, warnings=result_after.warnings)
    payload["tasks"][0]["status"] = "active"
    payload["tasks"][0]["updated_at"] = updated_at
    payload["active_task_wip"]["current"] = active_count + 1
    payload["mutations"] = [
        {
            "path": relative_repo_path(root, task_path),
            "field": "status",
            "from": "queued",
            "to": "active",
        },
        {
            "path": relative_repo_path(root, task_path),
            "field": "updated_at",
            "to": updated_at,
        },
    ]
    payload["written_views"] = [relative_repo_path(root, path) for path in written]
    return payload


def render_epic_run_dry_run(payload: dict[str, Any]) -> str:
    epic = payload["epic"]
    wip = payload["active_task_wip"]
    lines = [
        f"Epic run dry run: {epic['title']} ({epic['id']})",
        f"Status: {epic['status']}",
        f"Plan: {epic.get('plan_path') or 'None'}",
        f"Active task WIP: {wip['current']}/{wip['limit']}",
        (
            "Queued child tasks selected: "
            f"{payload['selected_task_count']}/{payload['total_queued_tasks']}"
        ),
        "",
        "Task sequence:",
    ]

    tasks = payload["tasks"]
    if tasks:
        for index, task in enumerate(tasks, start=1):
            lines.append(
                f"{index}. [{task['rank']}] {task['id']} - {task['title']}"
            )
            next_action = task.get("next_action")
            if next_action:
                lines.append(f"   Next action: {next_action}")
    else:
        lines.append("- None.")

    lines.extend(["", "Validation commands:"])
    if payload["validation_commands"]:
        lines.extend(f"- {command}" for command in payload["validation_commands"])
    else:
        lines.append("- None.")

    lines.extend(["", "Stop conditions:"])
    if payload["stop_conditions"]:
        lines.extend(f"- {condition}" for condition in payload["stop_conditions"])
    else:
        lines.append("- None.")

    lines.extend(["", "Mutations: none."])
    return "\n".join(lines).rstrip() + "\n"


def render_epic_run_apply_state(payload: dict[str, Any]) -> str:
    epic = payload["epic"]
    wip = payload["active_task_wip"]
    lines = [
        f"Epic run apply-state: {epic['title']} ({epic['id']})",
        f"Status: {epic['status']}",
        f"Plan: {epic.get('plan_path') or 'None'}",
        f"Active task WIP: {wip['current']}/{wip['limit']}",
        "",
    ]

    tasks = payload["tasks"]
    if tasks:
        task = tasks[0]
        lines.extend(
            [
                "Activated task:",
                f"- [{task['rank']}] {task['id']} - {task['title']}",
            ]
        )
        next_action = task.get("next_action")
        if next_action:
            lines.append(f"  Next action: {next_action}")
    else:
        lines.append(payload.get("message", "No queued child tasks to activate."))

    lines.extend(["", "Mutations:"])
    if payload["mutations"]:
        for mutation in payload["mutations"]:
            if "from" in mutation:
                lines.append(
                    f"- {mutation['path']}: {mutation['field']} "
                    f"{mutation['from']} -> {mutation['to']}"
                )
            else:
                lines.append(
                    f"- {mutation['path']}: {mutation['field']} -> {mutation['to']}"
                )
    else:
        lines.append("- None.")

    lines.extend(["", "Generated views:"])
    if payload["written_views"]:
        lines.extend(f"- {path}" for path in payload["written_views"])
    else:
        lines.append("- None.")

    return "\n".join(lines).rstrip() + "\n"


def render_command_output(label: str, text: str) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []
    lines = [f"  {label}:"]
    for line in stripped.splitlines():
        lines.append(f"    {line}")
    return lines


def render_epic_run_validation(payload: dict[str, Any]) -> str:
    epic = payload["epic"]
    lines = [
        f"Epic run validation: {epic['title']} ({epic['id']})",
        f"Status: {epic['status']}",
        f"Plan: {epic.get('plan_path') or 'None'}",
        "",
        "Validation results:",
    ]

    results = payload["results"]
    if not results:
        lines.append("- None.")
    for index, result in enumerate(results, start=1):
        lines.append(
            f"{index}. {result['command']} -> exit {result['return_code']}"
        )
        lines.extend(render_command_output("stdout", result.get("stdout", "")))
        lines.extend(render_command_output("stderr", result.get("stderr", "")))
        if result["return_code"] != 0:
            lines.append("  Stopped after failure.")
            break

    lines.append("")
    lines.append(f"Success: {str(payload['success']).lower()}")
    return "\n".join(lines).rstrip() + "\n"


def run_epic_command(
    root: Path,
    epic_id: str | None,
    dry_run: bool,
    max_slices: int | None,
    json_output: bool,
    apply_state: bool = False,
    validate: bool = False,
) -> int:
    if int(dry_run) + int(apply_state) + int(validate) != 1:
        print(
            "ERROR: run-epic requires exactly one of --dry-run, --apply-state, or --validate.",
            file=sys.stderr,
        )
        return 2
    if not epic_id:
        print("ERROR: run-epic requires --epic-id.", file=sys.stderr)
        return 2
    if max_slices is not None and max_slices < 1:
        print("ERROR: --max-slices must be a positive integer.", file=sys.stderr)
        return 2
    if apply_state and max_slices is not None:
        print("ERROR: --max-slices is only valid with --dry-run.", file=sys.stderr)
        return 2
    if validate and max_slices is not None:
        print("ERROR: --max-slices is only valid with --dry-run.", file=sys.stderr)
        return 2

    tracker = load_tracker(root)
    result = validate_tracker(tracker)
    for error in result.errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if result.errors:
        return 1

    try:
        if validate:
            payload = build_epic_run_validation_payload(root, epic_id=epic_id)
        elif apply_state:
            payload = build_epic_run_apply_state_payload(root, epic_id=epic_id)
        else:
            payload = build_epic_run_dry_run_payload(
                tracker, epic_id=epic_id, max_slices=max_slices
            )
    except KeyError:
        print(f"ERROR: epic does not exist: {epic_id}", file=sys.stderr)
        return 1
    except EpicRunStateError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    elif validate:
        print(render_epic_run_validation(payload), end="")
    elif apply_state:
        print(render_epic_run_apply_state(payload), end="")
    else:
        print(render_epic_run_dry_run(payload), end="")
    if validate and not payload["success"]:
        return 1
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("check", "build", "check-views", "run-epic"),
        help="Validate tracker state, rebuild generated views, or inspect an epic run.",
    )
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    parser.add_argument("--epic-id", help="Epic id for run-epic.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print run-epic sequence without mutating tracker state.",
    )
    parser.add_argument(
        "--apply-state",
        action="store_true",
        help="Activate the next queued child task and rebuild generated views.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run allowlisted validation commands from the epic brief.",
    )
    parser.add_argument(
        "--max-slices",
        type=int,
        help="Limit the number of queued child tasks shown for run-epic.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    if args.command == "check":
        return check_command(root)
    if args.command == "build":
        return build_command(root)
    if args.command == "check-views":
        return check_views_command(root)
    if args.command == "run-epic":
        return run_epic_command(
            root,
            epic_id=args.epic_id,
            dry_run=args.dry_run,
            max_slices=args.max_slices,
            json_output=args.json,
            apply_state=args.apply_state,
            validate=args.validate,
        )
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
