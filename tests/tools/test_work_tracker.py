from pathlib import Path
from textwrap import dedent

from tools.work_tracker import (
    build_current_view,
    build_roadmap_view,
    load_tracker,
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


def test_repo_tracker_state_is_valid():
    repo_root = Path(__file__).resolve().parents[2]
    tracker = load_tracker(repo_root)

    result = validate_tracker(tracker)

    assert result.errors == []
