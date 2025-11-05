import json
from datetime import date
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import pytest

from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor
from cms_pricing.ingestion.services.conversion_factor_fetcher import ConversionFactorMetadata
from cms_pricing.services.dataset_snapshot_service import SnapshotMetadata


class StubSnapshotService:
    """In-memory snapshot service for tests."""

    def __init__(self, snapshots: Dict[str, SnapshotMetadata]):
        self._snapshots = snapshots
        self.registered: List[Dict[str, Any]] = []

    def get_latest_snapshot(self, dataset_id: str) -> SnapshotMetadata:
        return self._snapshots[dataset_id]

    def register_snapshot(
        self,
        dataset_id: str,
        release_id: str,
        digest: str,
        effective_from: date,
        effective_to: Optional[date] = None,
        manifest_url: Optional[str] = None,
        curated_path: Optional[str] = None,
    ):
        self.registered.append(
            {
                "dataset_id": dataset_id,
                "release_id": release_id,
                "digest": digest,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "manifest_url": manifest_url,
                "curated_path": curated_path,
            }
        )

    def close(self) -> None:
        pass


class StubConversionFactorFetcher:
    """Return pre-seeded conversion factor metadata."""

    def __init__(self, metadata: ConversionFactorMetadata):
        self.metadata = metadata
        self.calls: List[Dict[str, Any]] = []

    async def ensure_conversion_factor(
        self,
        year: int,
        source_url: Optional[str] = None,
        manual_override_path: Optional[str] = None,
        expected_checksum: Optional[str] = None,
    ) -> ConversionFactorMetadata:
        self.calls.append(
            {
                "year": year,
                "source_url": source_url,
                "manual_override_path": manual_override_path,
                "expected_checksum": expected_checksum,
            }
        )
        return self.metadata


def _sample_rvu_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hcpcs_code": "A1000",
                "modifier": "",
                "description": "Test procedure",
                "status_code": "A",
                "global_days": "000",
                "work_rvu": 1.0,
                "pe_rvu_nonfac": 0.5,
                "pe_rvu_fac": 0.4,
                "mp_rvu": 0.1,
                "na_indicator": "N",
                "opps_cap_applicable": "N",
                "bilateral_ind": "0",
                "multiple_proc_ind": "0",
                "assistant_surg_ind": "0",
                "co_surg_ind": "0",
                "team_surg_ind": "0",
                "total_nonfac": 1.6,
                "total_fac": 1.5,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
            }
        ]
    )


def _sample_gpci_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "mac": "00101",
                "state": "CA",
                "locality_id": "01",
                "locality_name": "California Test Locality",
                "work_gpci": 1.0,
                "pe_gpci": 0.8,
                "mp_gpci": 0.3,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
            }
        ]
    )


@pytest.fixture
def mpfs_test_environment(tmp_path) -> Dict[str, Any]:
    """Prepare MPFS ingestor with stub snapshot service and CF fetcher."""
    rvu_df = _sample_rvu_df()
    gpci_df = _sample_gpci_df()

    rvu_path = tmp_path / "rvu.parquet"
    gpci_path = tmp_path / "gpci.parquet"
    rvu_df.to_parquet(rvu_path, index=False)
    gpci_df.to_parquet(gpci_path, index=False)

    cf_path = tmp_path / "cf.txt"
    cf_path.write_text("CY2025 MPFS Conversion Factor: 35.5000")

    snapshots = {
        "rvu_items": SnapshotMetadata(
            dataset_id="rvu_items",
            release_id="rvu_2025D",
            digest="rvu_digest",
            effective_from=date(2025, 1, 1),
            effective_to=date(2025, 12, 31),
            manifest_url=None,
            path=str(rvu_path),
        ),
        "gpci_indices": SnapshotMetadata(
            dataset_id="gpci_indices",
            release_id="gpci_2025A",
            digest="gpci_digest",
            effective_from=date(2025, 1, 1),
            effective_to=date(2025, 12, 31),
            manifest_url=None,
            path=str(gpci_path),
        ),
    }

    snapshot_service = StubSnapshotService(snapshots)
    cf_metadata = ConversionFactorMetadata(
        year=2025,
        path=str(cf_path),
        checksum="cf_digest",
        source_url="file://cf.txt",
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 12, 31),
    )
    cf_fetcher = StubConversionFactorFetcher(cf_metadata)

    output_dir = tmp_path / "output"
    ingestor = MPFSIngestor(
        output_dir=str(output_dir),
        snapshot_service=snapshot_service,
        cf_fetcher=cf_fetcher,
    )

    return {
        "ingestor": ingestor,
        "snapshot_service": snapshot_service,
        "cf_fetcher": cf_fetcher,
        "rvu_df": rvu_df,
        "gpci_df": gpci_df,
        "cf_value": 35.5,
        "output_dir": output_dir,
    }


@pytest.mark.asyncio
async def test_mpfs_ingestor_full_pipeline(mpfs_test_environment):
    env = mpfs_test_environment
    ingestor: MPFSIngestor = env["ingestor"]

    result = await ingestor.ingest(2025)

    # Curated view summary
    summary = result["curated_views"]
    assert "mpfs_payment_curated" in summary
    assert summary["mpfs_payment_curated"]["rows"] == 1

    # Manifest produced with expected entries
    manifest_path = Path(result["manifest_path"])
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert set(manifest.keys()) == {
        "mpfs_payment_curated",
        "mpfs_rvu",
        "mpfs_gpci",
        "mpfs_cf_vintage",
        "mpfs_indicators_all",
        "mpfs_locality",
        "mpfs_link_keys",
    }

    # Validate payment computation
    payment_df = pd.read_parquet(manifest_path.parent / "mpfs_payment_curated.parquet")
    expected_nonfac = (1.0 * 1.0 + 0.5 * 0.8 + 0.1 * 0.3) * env["cf_value"]
    expected_fac = (1.0 * 1.0 + 0.4 * 0.8 + 0.1 * 0.3) * env["cf_value"]
    assert pytest.approx(payment_df.loc[0, "payment_nonfacility"], rel=1e-6) == expected_nonfac
    assert pytest.approx(payment_df.loc[0, "payment_facility"], rel=1e-6) == expected_fac

    # Snapshot registrations recorded
    snapshot_service: StubSnapshotService = env["snapshot_service"]
    assert len(snapshot_service.registered) == len(manifest)
    dataset_ids = {entry["dataset_id"] for entry in snapshot_service.registered}
    assert dataset_ids == set(manifest.keys())

    # Observability report reflects processed rows
    report = result["observability_report"]
    assert report.volume_metrics.rows_processed == 1 * 1
    assert report.quality_metrics.quality_score == pytest.approx(1.0)
