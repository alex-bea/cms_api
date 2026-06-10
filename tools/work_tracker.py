#!/usr/bin/env python3
"""Validate and render the repo-native CMS API work tracker."""

from __future__ import annotations

import argparse
import os
import re
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

STATUSES = {"queued", "active", "blocked", "parked", "icebox", "done"}
OWNER_MODES = {"alex", "agent", "shared"}
TEAMS = {"system", "data", "api", "ops", "shared"}
ACTIVE_TASK_LIMIT = 3
STATUS_ORDER = {
    "active": 0,
    "blocked": 1,
    "queued": 2,
    "parked": 3,
    "icebox": 4,
    "done": 5,
}
KIND_ORDER = ("roadmaps", "epics", "tasks")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


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
        if task.get("status") in {"active", "blocked"}:
            active_tasks.append(task)
            for field in ("current_task", "next_action", "resume_from"):
                if not str(task.get(field, "")).strip():
                    errors.append(
                        f"{task.get('_path')}: {field} is required for active/blocked tasks"
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
    ordered = ["active", "blocked", "queued", "parked", "icebox", "done"]
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
                if task.get("status") in {"active", "blocked"}
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("check", "build"),
        help="Validate tracker state or rebuild generated views.",
    )
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    if args.command == "check":
        return check_command(root)
    if args.command == "build":
        return build_command(root)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
