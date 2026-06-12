from __future__ import annotations

from types import SimpleNamespace

import tools.todo_lint as todo_lint


def test_staged_clean_python_passes(tmp_path, monkeypatch):
    clean = tmp_path / "app.py"
    clean.write_text("# TODO(alex, GH-123): tracked task\n", encoding="utf-8")

    monkeypatch.setattr(todo_lint, "ROOT", tmp_path)

    def fake_run(command, **kwargs):
        assert command == [
            "git",
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=ACMR",
        ]
        assert kwargs["cwd"] == tmp_path
        return SimpleNamespace(stdout="app.py\n")

    monkeypatch.setattr(todo_lint.subprocess, "run", fake_run)

    assert todo_lint.main(["--staged"]) == 0


def test_staged_naked_todo_fails(tmp_path, monkeypatch):
    todo = tmp_path / "app.py"
    todo.write_text("# TO" "DO fix this\n", encoding="utf-8")

    monkeypatch.setattr(todo_lint, "ROOT", tmp_path)
    monkeypatch.setattr(
        todo_lint.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="app.py\n"),
    )

    assert todo_lint.main(["--staged"]) == 1


def test_changed_from_scans_only_changed_python(tmp_path, monkeypatch):
    clean = tmp_path / "app.py"
    clean.write_text("print('ok')\n", encoding="utf-8")
    legacy = tmp_path / "legacy.py"
    legacy.write_text("# TO" "DO old debt\n", encoding="utf-8")

    monkeypatch.setattr(todo_lint, "ROOT", tmp_path)

    def fake_run(command, **kwargs):
        assert command == [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=ACMR",
            "origin/main...HEAD",
        ]
        assert kwargs["cwd"] == tmp_path
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        return SimpleNamespace(stdout="app.py\n")

    monkeypatch.setattr(todo_lint.subprocess, "run", fake_run)

    assert todo_lint.main(["--changed-from", "origin/main"]) == 0


def test_unstaged_todo_does_not_block_staged_scan(tmp_path, monkeypatch):
    clean = tmp_path / "app.py"
    clean.write_text("print('ok')\n", encoding="utf-8")
    legacy = tmp_path / "legacy.py"
    legacy.write_text("# TO" "DO old debt\n", encoding="utf-8")

    monkeypatch.setattr(todo_lint, "ROOT", tmp_path)
    monkeypatch.setattr(
        todo_lint.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="app.py\n"),
    )

    assert todo_lint.main(["--staged"]) == 0
