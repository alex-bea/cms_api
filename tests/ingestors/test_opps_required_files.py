from datetime import date, datetime
from pathlib import Path

import pytest

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor, OPPSBatchInfo
from cms_pricing.ingestion.scrapers.cms_opps_scraper import ScrapedFileInfo
from cms_pricing.ingestion.services.ingestor_artifact_profile import ArtifactProfile


class _StaticProfileService:
    def __init__(self, profile: ArtifactProfile):
        self._profile = profile

    def resolve(self, *args, **kwargs):
        return self._profile


def _make_file(file_type: str) -> ScrapedFileInfo:
    return ScrapedFileInfo(
        url=f"https://example.com/{file_type}.csv",
        filename=f"{file_type}.csv",
        file_type=file_type,
        batch_id="opps_2025q1_r01",
        discovered_at=datetime.utcnow(),
        source_page="https://example.com",
        metadata={"year": 2025, "quarter": 1},
        local_path=None,
        checksum=None,
        downloaded_at=None,
    )


def _make_batch(file_types, release_number=1) -> OPPSBatchInfo:
    files = [_make_file(ft) for ft in file_types]
    return OPPSBatchInfo(
        batch_id="opps_2025q1_r01",
        year=2025,
        quarter=1,
        release_number=release_number,
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 3, 31),
        files=files,
        discovered_at=datetime.utcnow(),
        downloaded_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_required_addenda_enforced(tmp_path):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    profile = ArtifactProfile(
        name="quarterly",
        required=["addendum_a", "addendum_b"],
        optional=[],
        allow_missing_required=False,
    )
    ingestor.artifact_profile_service = _StaticProfileService(profile)
    batch = _make_batch(["addendum_a"])
    result = await ingestor._validate_required_files(batch)
    assert result["passed"] is False
    assert any("addendum_b" in err for err in result["errors"])


@pytest.mark.asyncio
async def test_optional_addenda_warn_only(tmp_path):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    profile = ArtifactProfile(
        name="quarterly",
        required=["addendum_a", "addendum_b"],
        optional=["addendum_q"],
        allow_missing_required=False,
    )
    ingestor.artifact_profile_service = _StaticProfileService(profile)
    batch = _make_batch(["addendum_a", "addendum_b"])
    result = await ingestor._validate_required_files(batch)
    assert result["passed"] is True
    assert any("addendum_q" in warning for warning in result["warnings"])


@pytest.mark.asyncio
async def test_sandbox_leniency(tmp_path):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    profile = ArtifactProfile(
        name="quarterly",
        required=["addendum_a", "addendum_b"],
        optional=[],
        allow_missing_required=True,
    )
    ingestor.artifact_profile_service = _StaticProfileService(profile)
    ingestor.local_sample_dir = Path(tmp_path)
    batch = _make_batch(["addendum_a"])
    result = await ingestor._validate_required_files(batch)
    assert result["passed"] is True
    assert not result["errors"]
    assert any("sandbox" in warning.lower() for warning in result["warnings"])
