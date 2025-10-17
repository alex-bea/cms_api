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


def test_extract_issue_refs_from_commits():
    log = """abcd123
Add new feature (#333)
More details
----END-COMMIT----
efgh456
Fix something GH-444
----END-COMMIT----
"""
    refs = mtd.extract_issue_references(log, base_line=0)
    numbers = sorted(ref.number for ref in refs)
    assert numbers == [333, 444]
