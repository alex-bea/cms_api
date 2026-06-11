from datetime import datetime

from cms_pricing.ingestion.scrapers.cms_rvu_scraper import RVUFileInfo
from scripts.load_latest_cms_rvu_local import (
    _release_id_for,
    select_release_files,
    source_file_from_rvu_info,
    source_version_for_release,
)


def _file(year: int, quarter: str, revision: str | None = None) -> RVUFileInfo:
    return RVUFileInfo(
        url=f"https://example.test/rvu{year}{quarter}.zip",
        filename=f"rvu{year}{quarter}.zip",
        content_type="application/zip",
        year=year,
        quarter=quarter,
        revision=revision,
        posted_at="2026-06-01",
        file_size="1 MB",
        detail_url="https://example.test/detail",
        display_name="RVU test",
        version=f"{year}{quarter}",
        file_type="zip",
        metadata={"extra": "value"},
        size_bytes=1024,
        checksum="abc123",
        last_modified=datetime(2026, 6, 1),
    )


def test_select_release_files_latest_uses_highest_year_and_quarter():
    files = [_file(2025, "D"), _file(2026, "A"), _file(2026, "C")]

    selected = select_release_files(files, "latest")

    assert [_release_id_for(file_info) for file_info in selected] == ["rvu_2026_C"]


def test_select_release_files_accepts_standard_and_compact_aliases():
    files = [_file(2026, "A"), _file(2026, "C")]

    assert [_release_id_for(file_info) for file_info in select_release_files(files, "rvu_2026_C")] == [
        "rvu_2026_C"
    ]
    assert [_release_id_for(file_info) for file_info in select_release_files(files, "rvu26c")] == [
        "rvu_2026_C"
    ]
    assert [_release_id_for(file_info) for file_info in select_release_files(files, "26c")] == [
        "rvu_2026_C"
    ]


def test_source_file_from_rvu_info_preserves_download_metadata():
    source_file = source_file_from_rvu_info(_file(2026, "C"))

    assert source_file.filename == "rvu2026C.zip"
    assert source_file.file_type == "zip"
    assert source_file.expected_size_bytes == 1024
    assert source_file.metadata["year"] == 2026
    assert source_file.metadata["quarter"] == "C"
    assert source_file.metadata["extra"] == "value"


def test_source_version_for_release_matches_rvu_loader_truncation():
    assert source_version_for_release("rvu_2026_C") == "rvu_2026_C"
    assert source_version_for_release("rvu_2026_C2") == "rvu_2026_C"
