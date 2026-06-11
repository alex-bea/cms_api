import json
from pathlib import Path
from textwrap import dedent

from tools.work_tracker import (
    build_epic_run_dry_run_payload,
    build_current_view,
    build_roadmap_view,
    check_views_command,
    load_tracker,
    run_epic_command,
    validate_tracker,
    write_views,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")


def _seed_minimal_tracker(root: Path) -> None:
    _write(root / "docs/workbench/DOC-status.md", "# Status\n")
    _write(root / "cms_pricing/ingestion/.keep", "")
    _write(
        root / "state/work/roadmaps/data-pipeline.yaml",
        """
        id: data-pipeline
        title: Data Pipeline
        status: active
        rank: 1
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: null
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        summary: "Pipeline work."
        """,
    )
    _write(
        root / "state/work/epics/rvu.yaml",
        """
        id: rvu
        parent_id: data-pipeline
        title: RVU
        status: active
        rank: 1
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        summary: "RVU work."
        """,
    )
    _write(
        root / "state/work/tasks/use-rvu.yaml",
        """
        id: use-rvu
        parent_id: rvu
        title: Use RVU
        status: active
        rank: 1
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        current_task: "Run pricing smoke."
        next_action: "Call pricing endpoint."
        resume_from: "Use loaded RVU data."
        """,
    )


def _write_task(
    root: Path,
    task_id: str,
    parent_id: str,
    title: str,
    status: str,
    rank: int,
) -> None:
    _write(
        root / f"state/work/tasks/{task_id}.yaml",
        f"""
        id: {task_id}
        parent_id: {parent_id}
        title: {title}
        status: {status}
        rank: {rank}
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        current_task: "{title} task."
        next_action: "Do {title.lower()}."
        resume_from: "Start {title.lower()}."
        """,
    )


def _set_task_status(root: Path, task_id: str, status: str) -> None:
    task_path = root / f"state/work/tasks/{task_id}.yaml"
    task_path.write_text(
        task_path.read_text(encoding="utf-8").replace(
            "status: active", f"status: {status}"
        ),
        encoding="utf-8",
    )


def _write_epic_brief_with_validation(root: Path, commands: list[str]) -> None:
    lines = [
        "# RVU",
        "",
        "**Tracker link:** `state/work/epics/rvu.yaml`",
        "",
        "## Related Governance",
        "## Goal",
        "## Current State",
        "## Scope",
        "## Acceptance Criteria",
        "## Validation",
        *(f"- `{command}`" for command in commands),
        "## Privacy / Data Boundaries",
        "## PRD / STD Impact",
        "## Known Risks",
        "## Stop Conditions",
        "## Ordered Task Slices",
    ]
    path = root / "docs/workbench/DOC-status.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_fake_python(root: Path) -> None:
    fake_python = root / ".venv/bin/python"
    _write(
        fake_python,
        """
        #!/usr/bin/env python3
        import pathlib
        import sys

        args = sys.argv[1:]
        pathlib.Path("validation.log").open("a", encoding="utf-8").write(
            " ".join(args) + "\\n"
        )
        print("ran " + " ".join(args))
        if any("fail.py" in arg for arg in args):
            print("failed command", file=sys.stderr)
            raise SystemExit(7)
        """,
    )
    fake_python.chmod(0o755)


def _write_governance_scripts(root: Path, names: list[str]) -> None:
    for name in names:
        _write(root / f"scripts/governance/{name}", "# placeholder\n")


def test_tracker_validates_and_renders_temp_state(tmp_path):
    _seed_minimal_tracker(tmp_path)

    tracker = load_tracker(tmp_path)
    result = validate_tracker(tracker)

    assert result.errors == []
    roadmap = build_roadmap_view(tracker)
    current = build_current_view(tracker)
    assert "Data Pipeline" in roadmap
    assert "Use RVU" in current


def test_write_views_creates_generated_files(tmp_path):
    _seed_minimal_tracker(tmp_path)
    tracker = load_tracker(tmp_path)

    written = write_views(tracker)

    assert {path.relative_to(tmp_path).as_posix() for path in written} == {
        "docs/workbench/ROADMAP.md",
        "docs/workbench/CURRENT.md",
    }
    assert "Generated from `state/work/`" in (
        tmp_path / "docs/workbench/CURRENT.md"
    ).read_text(encoding="utf-8")


def test_check_views_detects_generated_view_drift(tmp_path):
    _seed_minimal_tracker(tmp_path)
    tracker = load_tracker(tmp_path)
    write_views(tracker)
    (tmp_path / "docs/workbench/CURRENT.md").write_text("stale\n", encoding="utf-8")

    assert check_views_command(tmp_path) == 1


def test_queued_for_merge_tasks_are_valid_and_rendered(tmp_path):
    _seed_minimal_tracker(tmp_path)
    task_path = tmp_path / "state/work/tasks/use-rvu.yaml"
    task_path.write_text(
        task_path.read_text(encoding="utf-8").replace(
            "status: active", "status: queued_for_merge"
        ),
        encoding="utf-8",
    )

    tracker = load_tracker(tmp_path)
    result = validate_tracker(tracker)

    assert result.errors == []
    current = build_current_view(tracker)
    assert "## Queued For Merge" in current
    assert "Use RVU" in current


def test_epic_plan_path_without_epic_brief_marker_is_not_heading_validated(tmp_path):
    _seed_minimal_tracker(tmp_path)

    tracker = load_tracker(tmp_path)
    result = validate_tracker(tracker)

    assert result.errors == []


def test_epic_brief_requires_standard_headings(tmp_path):
    _seed_minimal_tracker(tmp_path)
    _write(
        tmp_path / "docs/workbench/DOC-status.md",
        """
        # RVU

        **Tracker link:** `state/work/epics/rvu.yaml`

        ## Goal

        Do the work.
        """,
    )

    tracker = load_tracker(tmp_path)
    result = validate_tracker(tracker)

    assert len(result.errors) == 1
    assert result.errors[0].endswith(
        "state/work/epics/rvu.yaml: epic brief docs/workbench/DOC-status.md missing required heading(s): "
        "Related Governance, Current State, Scope, Acceptance Criteria, Validation, "
        "Privacy / Data Boundaries, PRD / STD Impact, Known Risks, Stop Conditions, Ordered Task Slices"
    )


def test_epic_brief_with_required_headings_is_valid(tmp_path):
    _seed_minimal_tracker(tmp_path)
    _write(
        tmp_path / "docs/workbench/DOC-status.md",
        """
        # RVU

        **Tracker link:** `state/work/epics/rvu.yaml`

        ## Related Governance
        ## Goal
        ## Current State
        ## Scope
        ## Acceptance Criteria
        ## Validation
        ## Privacy / Data Boundaries
        ## PRD / STD Impact
        ## Known Risks
        ## Stop Conditions
        ## Ordered Task Slices
        """,
    )

    tracker = load_tracker(tmp_path)
    result = validate_tracker(tracker)

    assert result.errors == []


def test_run_epic_dry_run_payload_orders_and_limits_queued_tasks(tmp_path):
    _seed_minimal_tracker(tmp_path)
    _write(
        tmp_path / "state/work/tasks/third.yaml",
        """
        id: third
        parent_id: rvu
        title: Third
        status: queued
        rank: 3
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        current_task: "Third task."
        next_action: "Do third."
        resume_from: "Start third."
        """,
    )
    _write(
        tmp_path / "state/work/tasks/second.yaml",
        """
        id: second
        parent_id: rvu
        title: Second
        status: queued
        rank: 2
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        current_task: "Second task."
        next_action: "Do second."
        resume_from: "Start second."
        """,
    )

    tracker = load_tracker(tmp_path)
    payload = build_epic_run_dry_run_payload(tracker, "rvu", max_slices=1)

    assert payload["dry_run"] is True
    assert payload["mutations"] == []
    assert payload["total_queued_tasks"] == 2
    assert payload["selected_task_count"] == 1
    assert [task["id"] for task in payload["tasks"]] == ["second"]


def test_run_epic_dry_run_json_output(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write(
        tmp_path / "state/work/tasks/next.yaml",
        """
        id: next
        parent_id: rvu
        title: Next
        status: queued
        rank: 2
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        current_task: "Next task."
        next_action: "Do next."
        resume_from: "Start next."
        """,
    )

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=True,
        max_slices=None,
        json_output=True,
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["epic"]["id"] == "rvu"
    assert payload["tasks"][0]["id"] == "next"
    assert payload["mutations"] == []


def test_run_epic_apply_state_activates_next_task_and_rebuilds_views(
    tmp_path, capsys
):
    _seed_minimal_tracker(tmp_path)
    _set_task_status(tmp_path, "use-rvu", "done")
    _write_task(tmp_path, "next", "rvu", "Next", "queued", 2)

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=True,
    )

    assert exit_code == 0
    assert "Activated task:" in capsys.readouterr().out
    task_text = (tmp_path / "state/work/tasks/next.yaml").read_text(
        encoding="utf-8"
    )
    assert "status: active" in task_text
    assert "Next" in (tmp_path / "docs/workbench/CURRENT.md").read_text(
        encoding="utf-8"
    )
    assert (tmp_path / "docs/workbench/ROADMAP.md").exists()
    assert validate_tracker(load_tracker(tmp_path)).errors == []


def test_run_epic_apply_state_refuses_full_active_wip(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write(
        tmp_path / "state/work/epics/other.yaml",
        """
        id: other
        parent_id: data-pipeline
        title: Other
        status: active
        rank: 2
        team: data
        owner_mode: shared
        updated_at: "2026-06-10"
        plan_path: "docs/workbench/DOC-status.md"
        related_paths:
          - "cms_pricing/ingestion"
        linked_beads:
        linked_outputs:
          - "docs/workbench/DOC-status.md"
        summary: "Other work."
        """,
    )
    _write_task(tmp_path, "other-one", "other", "Other One", "active", 1)
    _write_task(tmp_path, "other-two", "other", "Other Two", "active", 2)
    _write_task(tmp_path, "next", "rvu", "Next", "queued", 2)

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=True,
    )

    assert exit_code == 1
    assert "active task WIP is full: 3/3" in capsys.readouterr().err
    assert "status: queued" in (
        tmp_path / "state/work/tasks/next.yaml"
    ).read_text(encoding="utf-8")


def test_run_epic_apply_state_refuses_blocked_or_merge_boundary_tasks(
    tmp_path, capsys
):
    _seed_minimal_tracker(tmp_path)
    _write_task(tmp_path, "blocked", "rvu", "Blocked", "blocked", 2)
    _write_task(tmp_path, "next", "rvu", "Next", "queued", 3)

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=True,
    )

    assert exit_code == 1
    assert "epic has blocked or merge-boundary task(s)" in capsys.readouterr().err
    assert "status: queued" in (
        tmp_path / "state/work/tasks/next.yaml"
    ).read_text(encoding="utf-8")


def test_run_epic_apply_state_refuses_queued_for_merge_tasks(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write_task(tmp_path, "merge-ready", "rvu", "Merge Ready", "queued_for_merge", 2)
    _write_task(tmp_path, "next", "rvu", "Next", "queued", 3)

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=True,
    )

    assert exit_code == 1
    assert "merge-ready (queued_for_merge)" in capsys.readouterr().err
    assert "status: queued" in (
        tmp_path / "state/work/tasks/next.yaml"
    ).read_text(encoding="utf-8")


def test_run_epic_validate_executes_allowlisted_commands(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write_fake_python(tmp_path)
    _write_governance_scripts(tmp_path, ["ok.py"])
    _write_epic_brief_with_validation(
        tmp_path,
        [".venv/bin/python scripts/governance/ok.py"],
    )

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=False,
        validate=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "scripts/governance/ok.py -> exit 0" in output
    assert "Success: true" in output
    assert (tmp_path / "validation.log").read_text(encoding="utf-8") == (
        "scripts/governance/ok.py\n"
    )


def test_run_epic_validate_stops_on_first_failed_command(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write_fake_python(tmp_path)
    _write_governance_scripts(tmp_path, ["ok.py", "fail.py", "after.py"])
    _write_epic_brief_with_validation(
        tmp_path,
        [
            ".venv/bin/python scripts/governance/ok.py",
            ".venv/bin/python scripts/governance/fail.py",
            ".venv/bin/python scripts/governance/after.py",
        ],
    )

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=False,
        validate=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "scripts/governance/fail.py -> exit 7" in output
    assert "Stopped after failure." in output
    assert "Success: false" in output
    assert (tmp_path / "validation.log").read_text(encoding="utf-8") == (
        "scripts/governance/ok.py\nscripts/governance/fail.py\n"
    )


def test_run_epic_validate_rejects_unallowlisted_commands(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)
    _write_epic_brief_with_validation(
        tmp_path,
        ["bash scripts/governance/ok.sh"],
    )

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
        apply_state=False,
        validate=True,
    )

    assert exit_code == 1
    assert "validation command executable is not allowlisted: bash" in (
        capsys.readouterr().err
    )


def test_run_epic_requires_one_mode(tmp_path, capsys):
    _seed_minimal_tracker(tmp_path)

    exit_code = run_epic_command(
        tmp_path,
        epic_id="rvu",
        dry_run=False,
        max_slices=None,
        json_output=False,
    )

    assert exit_code == 2
    assert "requires exactly one of --dry-run, --apply-state, or --validate" in (
        capsys.readouterr().err
    )


def test_repo_tracker_state_is_valid():
    repo_root = Path(__file__).resolve().parents[2]
    tracker = load_tracker(repo_root)

    result = validate_tracker(tracker)

    assert result.errors == []
