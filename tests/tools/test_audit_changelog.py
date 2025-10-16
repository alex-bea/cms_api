from tools.audit_changelog import parse_changelog


def test_parse_changelog_allows_leading_v_version():
    content = """# Changelog

## [v0.1.0-phase0] - 2025-10-16
### Added
- Something
"""
    parsed = parse_changelog(content)
    versions = parsed["versions"]
    assert versions
    assert versions[0]["version"] == "v0.1.0-phase0"
