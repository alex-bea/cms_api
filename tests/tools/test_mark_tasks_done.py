from tools import mark_tasks_done as mtd


def test_extract_section_and_issue_refs():
    changelog = """# Changelog

## [Unreleased]
- Added feature Foo #101
- Fixed bug GH-202

## [1.0.0] - 2025-10-01
- Nothing here
"""
    section, base_line = mtd.extract_section(changelog, "Unreleased")
    assert "Added feature Foo" in section
    refs = mtd.extract_issue_references(section, base_line)
    numbers = sorted(ref.number for ref in refs)
    assert numbers == [101, 202]
    assert refs[0].line_text.startswith("- Added feature Foo")
