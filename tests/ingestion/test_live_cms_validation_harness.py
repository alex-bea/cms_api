import os
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from cms_pricing.ingestion.scrapers.cms_rvu_scraper import RVUFileInfo
from scripts.validate_live_cms_ingest import (
    LiveCMSValidationConfig,
    RecordingSnapshotService,
    main,
    run_live_validation,
    select_release_files,
    source_file_from_rvu_info,
)


def _rvu_file(
    *,
    year: int,
    quarter: str,
    revision: str | None = None,
    filename: str | None = None,
) -> RVUFileInfo:
    suffix = f"{quarter}{revision or ''}"
    return RVUFileInfo(
        url=f"https://www.cms.gov/files/zip/rvu{year % 100:02d}{suffix.lower()}.zip",
        filename=filename or f"RVU{year % 100:02d}{suffix}.zip",
        content_type="application/zip",
        year=year,
        quarter=quarter,
        revision=revision,
        posted_at=f"{year}-01-01",
        file_size="1 MB",
        detail_url=f"https://www.cms.gov/rvu-{year}-{suffix.lower()}",
        display_name=f"RVU {year} {suffix}",
        version=f"{year}{suffix}",
        file_type="zip",
        metadata={"source": "test"},
        size_bytes=1234,
        checksum="abc123",
        last_modified=datetime(year, 1, 1, tzinfo=timezone.utc),
    )


def test_select_release_files_latest_uses_highest_year_quarter_and_revision():
    files = [
        _rvu_file(year=2024, quarter="D"),
        _rvu_file(year=2025, quarter="A"),
        _rvu_file(year=2025, quarter="B"),
        _rvu_file(year=2025, quarter="B", revision="1"),
    ]

    selected = select_release_files(files, "latest")

    assert selected == [files[-1]]


def test_select_release_files_accepts_standard_and_compact_release_ids():
    files = [
        _rvu_file(year=2025, quarter="A", filename="RVU25A.zip"),
        _rvu_file(year=2025, quarter="B", filename="RVU25B.zip"),
    ]

    assert select_release_files(files, "rvu_2025_A") == [files[0]]
    assert select_release_files(files, "rvu25b") == [files[1]]
    assert select_release_files(files, "25a") == [files[0]]


def test_source_file_from_rvu_info_preserves_download_metadata():
    file_info = _rvu_file(year=2025, quarter="A")

    source_file = source_file_from_rvu_info(file_info)

    assert source_file.url == file_info.url
    assert source_file.filename == file_info.filename
    assert source_file.content_type == "application/zip"
    assert source_file.expected_size_bytes == 1234
    assert source_file.last_modified == file_info.last_modified
    assert source_file.checksum == "abc123"
    assert source_file.file_type == "zip"
    assert source_file.metadata["detail_url"] == file_info.detail_url
    assert source_file.metadata["year"] == 2025
    assert source_file.metadata["quarter"] == "A"
    assert source_file.metadata["source"] == "test"


def test_recording_snapshot_service_supports_transactional_publish_contract():
    service = RecordingSnapshotService()

    with service.db.begin():
        service.register_snapshot(
            dataset_id="rvu_items",
            release_id="rvu_2025_A",
            digest="abc123",
            effective_from=date(2025, 1, 1),
            curated_path="/tmp/rvu.parquet",
            autocommit=False,
        )

    assert service.registered == [
        {
            "dataset_id": "rvu_items",
            "release_id": "rvu_2025_A",
            "digest": "abc123",
            "effective_from": "2025-01-01",
            "effective_to": None,
            "manifest_url": None,
            "curated_path": "/tmp/rvu.parquet",
            "autocommit": False,
        }
    ]


def test_live_cms_script_requires_explicit_enablement(monkeypatch, capsys):
    monkeypatch.delenv("ENABLE_LIVE_CMS", raising=False)

    exit_code = main(["--dataset", "rvu"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Live CMS validation is disabled" in captured.err


@pytest.mark.live_cms
@pytest.mark.ingestor
@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("ENABLE_LIVE_CMS") != "1",
    reason="set ENABLE_LIVE_CMS=1 to download and validate live CMS RVU files",
)
async def test_live_cms_rvu_latest_release_downloads_and_publishes(tmp_path: Path):
    start_year = int(os.getenv("LIVE_CMS_START_YEAR", str(date.today().year)))
    end_year = int(os.getenv("LIVE_CMS_END_YEAR", str(start_year)))

    report = await run_live_validation(
        LiveCMSValidationConfig(
            dataset="rvu",
            start_year=start_year,
            end_year=end_year,
            release=os.getenv("LIVE_CMS_RELEASE", "latest"),
            output_dir=tmp_path / "live_cms_validation",
            min_total_records=1,
        )
    )

    assert report["status"] == "success"
    assert report["discovery"]["selected_files"]
    assert report["stages"]["publish"]["record_count"] > 0
    assert report["stages"]["publish"]["snapshot_registrations"]
