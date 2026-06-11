from __future__ import annotations

from types import SimpleNamespace

import tools.md_checkbox_scan as scanner


def test_staged_clean_markdown_passes(tmp_path, monkeypatch):
    clean = tmp_path / "docs" / "note.md"
    clean.parent.mkdir()
    clean.write_text("- [x] done\n", encoding="utf-8")

    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    def fake_run(command, **kwargs):
        assert command == [
            "git",
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=ACMR",
        ]
        assert kwargs["cwd"] == tmp_path
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        return SimpleNamespace(stdout="docs/note.md\n")

    monkeypatch.setattr(scanner.subprocess, "run", fake_run)

    assert scanner.main(["--staged"]) == 0


def test_staged_unchecked_markdown_fails(tmp_path, monkeypatch):
    todo = tmp_path / "docs" / "todo.md"
    todo.parent.mkdir()
    todo.write_text("- [ ] migrate this\n", encoding="utf-8")

    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    monkeypatch.setattr(
        scanner.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="docs/todo.md\n"),
    )

    assert scanner.main(["--staged"]) == 1


def test_unstaged_legacy_checkbox_does_not_block_staged_scan(tmp_path, monkeypatch):
    clean = tmp_path / "docs" / "clean.md"
    clean.parent.mkdir()
    clean.write_text("No checkbox here.\n", encoding="utf-8")
    legacy = tmp_path / "artifacts" / "legacy.md"
    legacy.parent.mkdir()
    legacy.write_text("- [ ] old unchecked item\n", encoding="utf-8")

    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    monkeypatch.setattr(
        scanner.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="docs/clean.md\n"),
    )

    assert scanner.main(["--staged"]) == 0


def test_template_allowlist_passes_with_unchecked_checkbox(tmp_path, monkeypatch):
    template = tmp_path / "prds" / "_templates" / "SRC-TEMPLATE.md"
    template.parent.mkdir(parents=True)
    template.write_text("- [ ] allowed template placeholder\n", encoding="utf-8")

    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    monkeypatch.setattr(
        scanner.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="prds/_templates/SRC-TEMPLATE.md\n"
        ),
    )

    assert scanner.main(["--staged"]) == 0
