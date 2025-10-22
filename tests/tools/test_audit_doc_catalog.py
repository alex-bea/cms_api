import logging
import re

from tools.audit_doc_catalog import (
    CHANGE_LOG_HEADING,
    CHANGE_LOG_SUMMARY_PREFIX,
    CROSS_REF_HEADER,
    MASTER_BACKLINK_BULLET,
    find_table_data_range,
    update_master_catalog_text,
    ensure_master_backlink,
    extract_doc_from_row,
)


def _get_logger():
    logger = logging.getLogger("test.audit_doc_catalog")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


SAMPLE_MASTER = """## 1. Architectural Standards (`STD-*`)

| Document | Status | Owner | Last Reviewed | Notes |
|---|---|---|---|---|
| `STD-existing-prd-v1.0.md` | Adopted | Platform | 2025-01-01 | Existing entry |

---

## 9. Change Log

| Version | Date | Summary | PR |
|---|---|---|---|
| 1.0.0 | 2025-01-01 | Initial catalog | #TBD |
"""


def test_update_master_catalog_inserts_stub_and_change_log():
    doc_name = "STD-new-standard-prd-v1.0.md"
    result = update_master_catalog_text(SAMPLE_MASTER, [doc_name], _get_logger())

    assert result.updated_text is not None
    assert result.inserted_docs == [doc_name]
    assert result.change_log_updated is True

    lines = result.updated_text.splitlines()
    data_range = find_table_data_range(lines, "## 1. Architectural Standards (`STD-*`)")
    assert data_range is not None
    start, end = data_range
    data_lines = lines[start:end]
    docs_in_section = [extract_doc_from_row(line) for line in data_lines]
    assert docs_in_section == sorted(docs_in_section)
    assert any(doc_name in line and "Status TBD" in line for line in data_lines)

    change_log_range = find_table_data_range(lines, CHANGE_LOG_HEADING)
    assert change_log_range is not None
    cl_start, _ = change_log_range
    first_change_log_row = lines[cl_start]
    assert CHANGE_LOG_SUMMARY_PREFIX in first_change_log_row
    assert doc_name in first_change_log_row
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", first_change_log_row)


def test_update_master_catalog_skips_unknown_doc_type():
    doc_name = "XYZ-unknown.md"
    result = update_master_catalog_text(SAMPLE_MASTER, [doc_name], _get_logger())

    assert result.updated_text is None
    assert result.inserted_docs == []
    assert result.skipped_docs == [doc_name]
    assert result.change_log_updated is False


def test_ensure_master_backlink_inserts_section_when_missing():
    original = """# Sample Doc

**Status:** Draft
**Owners:** Team

---

Body content.
"""
    updated = ensure_master_backlink(original)
    assert updated is not None
    assert CROSS_REF_HEADER in updated
    assert MASTER_BACKLINK_BULLET in updated
    # Cross-reference block should appear before the first horizontal rule
    cross_refs_index = updated.splitlines().index(CROSS_REF_HEADER)
    dash_index = updated.splitlines().index("---")
    assert cross_refs_index < dash_index
    assert updated.endswith("\n")


def test_ensure_master_backlink_appends_to_existing_section():
    original = """# Sample Doc

**Cross-References:**
- `prds/OTHER-doc.md`

---
"""
    updated = ensure_master_backlink(original)
    assert updated is not None
    lines = updated.splitlines()
    cross_refs_index = lines.index(CROSS_REF_HEADER)
    bullets = []
    idx = cross_refs_index + 1
    while idx < len(lines) and lines[idx].startswith("-"):
        bullets.append(lines[idx])
        idx += 1
    assert bullets[-1] == MASTER_BACKLINK_BULLET
