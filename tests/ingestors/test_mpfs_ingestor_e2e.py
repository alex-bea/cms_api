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

    def __init__(self, snapshots: Dict[str, Dict[str, SnapshotMetadata]]):
        self._snapshots = snapshots
        self.registered: List[Dict[str, Any]] = []
        self.calls: List[Dict[str, Any]] = []

    def get_latest_snapshot(
        self,
        dataset_id: str,
        valuation_date: Optional[date] = None,
        release_id: Optional[str] = None,
    ) -> Optional[SnapshotMetadata]:
        self.calls.append({"dataset_id": dataset_id, "release_id": release_id})
        dataset_snapshots = self._snapshots.get(dataset_id)
        if dataset_snapshots is None:
            return None

        if isinstance(dataset_snapshots, dict):
            if release_id:
                return dataset_snapshots.get(release_id)
            # Default to first entry in insertion order
            return next(iter(dataset_snapshots.values()), None)

        # Fallback for legacy single snapshot entries
        if release_id and getattr(dataset_snapshots, "release_id", None) != release_id:
            return None
        return dataset_snapshots

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


class StubConfigService:
    """Simple stub for MPFSConfigService."""

    def __init__(self, default: Optional[Dict[str, str]] = None):
        self.default = default
        self.overrides: Dict[str, Optional[Dict[str, str]]] = {}
        self.calls: List[str] = []

    def set_override(self, release_id: str, override: Optional[Dict[str, str]]) -> None:
        self.overrides[release_id] = override

    def get_cf_overrides(self, release_id: str) -> Optional[Dict[str, str]]:
        self.calls.append(release_id)
        if release_id in self.overrides:
            return self.overrides[release_id]
        return self.default


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
                "conversion_factor": 35.5,
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


def _quarterly_rvu_df() -> pd.DataFrame:
    """RVU dataframe with multiple CF periods to exercise quarterly logic."""
    base = _sample_rvu_df().iloc[0].to_dict()
    base["hcpcs_code"] = "A1000"
    base["modifier"] = ""
    quarter_rows = [
        {
            **base,
            "conversion_factor": 36.0,
            "effective_start": date(2025, 4, 1),
            "effective_end": date(2025, 6, 30),
        },
        {
            **base,
            "conversion_factor": 37.0,
            "effective_start": date(2025, 7, 1),
            "effective_end": date(2025, 12, 31),
        },
    ]
    return pd.DataFrame(quarter_rows)


def _build_test_environment(
    tmp_path: Path,
    override_cf_value: Optional[float] = None,
    *,
    base_release_suffix: str = "D",
    additional_releases: Optional[Dict[str, Dict[str, Any]]] = None,
    base_rvu_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Helper to construct a test environment with optional manual CF override."""
    rvu_df = base_rvu_df or _sample_rvu_df()
    gpci_df = _sample_gpci_df()

    def _write_snapshot(
        dataset: str,
        release_suffix: str,
        df: pd.DataFrame,
        filename: str,
        effective_from: date = date(2025, 1, 1),
        effective_to: date = date(2025, 12, 31),
    ) -> SnapshotMetadata:
        path = tmp_path / filename
        df.to_parquet(path, index=False)
        release_id = f"{dataset}_{2025}_{release_suffix}"
        digest = f"{dataset}_{release_suffix.lower()}_digest"
        return SnapshotMetadata(
            dataset_id="rvu_items" if dataset == "rvu" else "gpci_indices",
            release_id=release_id,
            digest=digest,
            effective_from=effective_from,
            effective_to=effective_to,
            manifest_url=None,
            path=str(path),
        )

    snapshots: Dict[str, Dict[str, SnapshotMetadata]] = {
        "rvu_items": {},
        "gpci_indices": {},
    }

    base_rvu_snapshot = _write_snapshot("rvu", base_release_suffix, rvu_df, f"rvu_{base_release_suffix}.parquet")
    base_gpci_snapshot = _write_snapshot("gpci", base_release_suffix, gpci_df, f"gpci_{base_release_suffix}.parquet")
    snapshots["rvu_items"][base_rvu_snapshot.release_id] = base_rvu_snapshot
    snapshots["gpci_indices"][base_gpci_snapshot.release_id] = base_gpci_snapshot

    if additional_releases:
        for suffix, cfg in additional_releases.items():
            cfg_rvu_df = cfg.get("rvu_df", rvu_df)
            cfg_gpci_df = cfg.get("gpci_df", gpci_df)
            cfg_effective_from = cfg.get("effective_from", date(2025, 1, 1))
            cfg_effective_to = cfg.get("effective_to", date(2025, 12, 31))

            rvu_snapshot = _write_snapshot(
                "rvu",
                suffix,
                cfg_rvu_df,
                f"rvu_{suffix}.parquet",
                cfg_effective_from,
                cfg_effective_to,
            )
            gpci_snapshot = _write_snapshot(
                "gpci",
                suffix,
                cfg_gpci_df,
                f"gpci_{suffix}.parquet",
                cfg_effective_from,
                cfg_effective_to,
            )
            snapshots["rvu_items"][rvu_snapshot.release_id] = rvu_snapshot
            snapshots["gpci_indices"][gpci_snapshot.release_id] = gpci_snapshot

    snapshot_service = StubSnapshotService(snapshots)
    config_service = StubConfigService()

    if override_cf_value is not None:
        cf_path = tmp_path / "override_cf.txt"
        cf_path.write_text(f"Override CF: {override_cf_value:.4f}")
        cf_metadata = ConversionFactorMetadata(
            year=2025,
            path=str(cf_path),
            checksum="override_checksum",
            source_url="file://override_cf.txt",
            effective_from=date(2025, 1, 1),
            effective_to=date(2025, 12, 31),
        )
        config_service.default = {
            "manual_override_path": str(cf_path),
            "expected_checksum": None,
        }
        expected_cf = override_cf_value
    else:
        cf_metadata = ConversionFactorMetadata(
            year=2025,
            path=str(tmp_path / "unused_cf.txt"),
            checksum="unused_checksum",
            source_url="file://unused_cf.txt",
            effective_from=date(2025, 1, 1),
            effective_to=date(2025, 12, 31),
        )
        expected_cf = 35.5

    cf_fetcher = StubConversionFactorFetcher(cf_metadata)

    output_dir = tmp_path / "output"
    ingestor = MPFSIngestor(
        output_dir=str(output_dir),
        snapshot_service=snapshot_service,
        cf_fetcher=cf_fetcher,
        config_service=config_service,
    )

    return {
        "ingestor": ingestor,
        "snapshot_service": snapshot_service,
        "cf_fetcher": cf_fetcher,
        "config_service": config_service,
        "rvu_df": rvu_df,
        "gpci_df": gpci_df,
        "derived_cf": 35.5,
        "expected_cf": expected_cf,
        "override_cf": override_cf_value,
        "output_dir": output_dir,
        "base_rvu_release_id": base_rvu_snapshot.release_id,
        "base_gpci_release_id": base_gpci_snapshot.release_id,
        "snapshots": snapshots,
    }


@pytest.fixture
def mpfs_test_environment(tmp_path) -> Dict[str, Any]:
    return _build_test_environment(tmp_path)


@pytest.fixture
def mpfs_manual_override_environment(tmp_path) -> Dict[str, Any]:
    return _build_test_environment(tmp_path, override_cf_value=40.25)


@pytest.fixture
def mpfs_quarter_environment(tmp_path) -> Dict[str, Any]:
    base_rvu = _sample_rvu_df()
    base_rvu.loc[0, "conversion_factor"] = 35.25
    q2_rvu = _quarterly_rvu_df()

    env = _build_test_environment(
        tmp_path,
        base_release_suffix="A",
        base_rvu_df=base_rvu,
        additional_releases={
            "B": {
                "rvu_df": q2_rvu,
                "effective_from": date(2025, 4, 1),
                "effective_to": date(2025, 12, 31),
            }
        },
    )
    env["quarter_rvu_df"] = q2_rvu
    return env


@pytest.mark.asyncio
async def test_mpfs_ingestor_full_pipeline(mpfs_test_environment):
    env = mpfs_test_environment
    ingestor: MPFSIngestor = env["ingestor"]

    result = await ingestor.ingest(2025)

    cf_fetcher: StubConversionFactorFetcher = env["cf_fetcher"]
    assert cf_fetcher.calls == []
    config_service: StubConfigService = env["config_service"]
    assert config_service.calls

    # Curated view summary
    summary = result["curated_views"]
    assert "mpfs_payment_curated" in summary
    assert summary["mpfs_payment_curated"]["rows"] == 1
    assert summary["mpfs_cf_vintage"]["rows"] == 1

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
    expected_nonfac = (1.0 * 1.0 + 0.5 * 0.8 + 0.1 * 0.3) * env["expected_cf"]
    expected_fac = (1.0 * 1.0 + 0.4 * 0.8 + 0.1 * 0.3) * env["expected_cf"]
    assert pytest.approx(payment_df.loc[0, "payment_nonfacility"], rel=1e-6) == expected_nonfac
    assert pytest.approx(payment_df.loc[0, "payment_facility"], rel=1e-6) == expected_fac

    cf_df = pd.read_parquet(manifest_path.parent / "mpfs_cf_vintage.parquet")
    assert len(cf_df) == 1
    assert pytest.approx(cf_df.loc[0, "cf_value"], rel=1e-6) == env["expected_cf"]
    assert cf_df.loc[0, "cf_type"] == "physician"
    assert pd.to_datetime(cf_df.loc[0, "effective_start"]).date() == date(2025, 1, 1)
    assert pd.to_datetime(cf_df.loc[0, "effective_end"]).date() == date(2025, 12, 31)

    # Snapshot registrations recorded
    snapshot_service: StubSnapshotService = env["snapshot_service"]
    assert len(snapshot_service.registered) == len(manifest)
    dataset_ids = {entry["dataset_id"] for entry in snapshot_service.registered}
    assert dataset_ids == set(manifest.keys())

    # Observability report reflects processed rows
    report = result["observability_report"]
    assert report.volume_metrics.rows_processed == 1 * 1
    assert report.quality_metrics.quality_score == pytest.approx(1.0)

    # Metadata reflects derivation strategy and snapshot provenance
    metadata = result["metadata"]
    assert metadata["conversion_factor_strategy"] == "derive_from_rvu"
    assert metadata["snapshot_release_ids"]["rvu_items"] == env["base_rvu_release_id"]


@pytest.mark.asyncio
async def test_mpfs_ingestor_manual_override_uses_fetcher(mpfs_manual_override_environment):
    env = mpfs_manual_override_environment
    ingestor: MPFSIngestor = env["ingestor"]

    result = await ingestor.ingest(2025)

    cf_fetcher: StubConversionFactorFetcher = env["cf_fetcher"]
    assert cf_fetcher.calls
    config_service: StubConfigService = env["config_service"]
    assert config_service.calls

    manifest_path = Path(result["manifest_path"])
    cf_df = pd.read_parquet(manifest_path.parent / "mpfs_cf_vintage.parquet")
    assert len(cf_df) == 1
    assert pytest.approx(cf_df.loc[0, "cf_value"], rel=1e-6) == env["expected_cf"]

    payment_df = pd.read_parquet(manifest_path.parent / "mpfs_payment_curated.parquet")
    expected_nonfac = (1.0 * 1.0 + 0.5 * 0.8 + 0.1 * 0.3) * env["expected_cf"]
    expected_fac = (1.0 * 1.0 + 0.4 * 0.8 + 0.1 * 0.3) * env["expected_cf"]
    assert pytest.approx(payment_df.loc[0, "payment_nonfacility"], rel=1e-6) == expected_nonfac
    assert pytest.approx(payment_df.loc[0, "payment_facility"], rel=1e-6) == expected_fac

    # Confirm override actually changed the value relative to derived CF
    assert env["override_cf"] != env["derived_cf"]

    # Metadata reflects download strategy
    metadata = result["metadata"]
    assert metadata["conversion_factor_strategy"] == "download"


@pytest.mark.asyncio
async def test_mpfs_ingestor_selects_release_by_quarter(mpfs_quarter_environment):
    env = mpfs_quarter_environment
    ingestor: MPFSIngestor = env["ingestor"]

    result = await ingestor.ingest(2025, quarter="Q2")

    snapshot_service: StubSnapshotService = env["snapshot_service"]
    assert any(
        call["dataset_id"] == "rvu_items" and call["release_id"] == "rvu_2025_B"
        for call in snapshot_service.calls
    )
    assert any(
        call["dataset_id"] == "gpci_indices" and call["release_id"] == "gpci_2025_B"
        for call in snapshot_service.calls
    )

    manifest_path = Path(result["manifest_path"])
    cf_df = pd.read_parquet(manifest_path.parent / "mpfs_cf_vintage.parquet")
    assert len(cf_df) == 2
    assert set(cf_df["cf_value"].round(2).tolist()) == {36.0, 37.0}
    assert set(pd.to_datetime(cf_df["effective_start"]).dt.date.tolist()) == {
        date(2025, 4, 1),
        date(2025, 7, 1),
    }
    assert set(pd.to_datetime(cf_df["effective_end"]).dt.date.tolist()) == {
        date(2025, 6, 30),
        date(2025, 12, 31),
    }

    metadata = result["metadata"]
    assert metadata["conversion_factor_strategy"] == "derive_from_rvu"
    assert metadata["snapshot_release_ids"]["rvu_items"] == "rvu_2025_B"
    assert metadata["snapshot_release_ids"]["gpci_indices"] == "gpci_2025_B"
    assert metadata["target_release_suffix"] == "B"
    assert metadata["requested_release_param"] == "Q2"
    assert metadata["requested_rvu_release_id"] == "rvu_2025_B"

    payment_df = pd.read_parquet(manifest_path.parent / "mpfs_payment_curated.parquet")
    assert not payment_df.empty
    assert pytest.approx(payment_df.loc[0, "conversion_factor"], rel=1e-6) == 36.0
