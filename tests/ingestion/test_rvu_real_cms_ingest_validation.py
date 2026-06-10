from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from cms_pricing.ingestion.contracts.ingestor_spec import SourceFile
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor


class StubSnapshotService:
    def __init__(self) -> None:
        self.registered: List[Dict[str, Any]] = []
        self.db = StubSnapshotDb()

    def register_snapshot(
        self,
        dataset_id: str,
        release_id: str,
        digest: str,
        effective_from: date,
        effective_to: Optional[date] = None,
        manifest_url: Optional[str] = None,
        curated_path: Optional[str] = None,
        autocommit: bool = True,
    ) -> None:
        self.registered.append(
            {
                "dataset_id": dataset_id,
                "release_id": release_id,
                "digest": digest,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "manifest_url": manifest_url,
                "curated_path": curated_path,
                "autocommit": autocommit,
            }
        )

    def close(self) -> None:
        pass


class StubSnapshotDb:
    def begin(self) -> "StubSnapshotDb":
        return self

    def __enter__(self) -> "StubSnapshotDb":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        return False


def _source_file(path: Path) -> SourceFile:
    content_type_by_suffix = {
        ".csv": "text/csv",
        ".pdf": "application/pdf",
    }
    suffix = path.suffix.lower()
    return SourceFile(
        url=f"file://{path.resolve()}",
        filename=path.name,
        content_type=content_type_by_suffix.get(suffix, "application/octet-stream"),
        file_type=suffix.lstrip("."),
        expected_size_bytes=path.stat().st_size,
    )


@pytest.mark.parametrize(
    ("release_id", "expected"),
    [
        ("rvu_2025_A", date(2025, 1, 1)),
        ("rvu_2025_B", date(2025, 4, 1)),
        ("rvu_2026_C", date(2026, 7, 1)),
        ("rvu_2026_D", date(2026, 10, 1)),
        ("rvu26c", date(2026, 7, 1)),
        ("rvu_2026_Q4", date(2026, 10, 1)),
    ],
)
def test_rvu_release_id_maps_to_snapshot_effective_date(release_id, expected):
    assert RVUIngestor._release_effective_from(release_id) == expected


def test_snapshot_registration_uses_release_effective_date_not_run_date(tmp_path):
    ingestor = RVUIngestor(str(tmp_path / "ingested"))
    try:
        ingestor.snapshot_service.close()
    except Exception:
        pass
    snapshot_service = StubSnapshotService()
    ingestor.snapshot_service = snapshot_service
    ingestor._snapshot_service_managed_session = False

    parquet_path = tmp_path / "pprrvu_2026-06-10.parquet"
    parquet_path.write_bytes(b"placeholder")

    ingestor._register_dataset_snapshots(
        publish_result={
            "curated_tables": {"pprrvu": parquet_path},
            "file_digests": {"pprrvu": "digest"},
            "export_artifacts": {"manifest": str(tmp_path / "manifest.json")},
        },
        release_id="rvu_2026_C",
        vintage_date="2026-06-10",
    )

    assert snapshot_service.registered == [
        {
            "dataset_id": "rvu_items",
            "release_id": "rvu_2026_C",
            "digest": "digest",
            "effective_from": date(2026, 7, 1),
            "effective_to": None,
            "manifest_url": str(tmp_path / "manifest.json"),
            "curated_path": str(parquet_path),
            "autocommit": False,
        }
    ]


@pytest.mark.ingestor
@pytest.mark.asyncio
async def test_real_cms_rvu_sample_files_validate_normalize_and_publish(tmp_path):
    sample_dir = Path("sample_data/rvu25a")
    source_paths = [
        sample_dir / "PPRRVU25_JAN.csv",
        sample_dir / "GPCI2025.csv",
        sample_dir / "OPPSCAP_JAN.csv",
        sample_dir / "ANES2025.csv",
        sample_dir / "25LOCCO.csv",
        sample_dir / "RVU25A.pdf",
    ]
    for source_path in source_paths:
        assert source_path.exists(), f"Missing checked-in CMS sample: {source_path}"

    ingestor = RVUIngestor(str(tmp_path / "ingested"))
    try:
        ingestor.snapshot_service.close()
    except Exception:
        pass
    snapshot_service = StubSnapshotService()
    ingestor.snapshot_service = snapshot_service
    ingestor._snapshot_service_managed_session = False

    release_id = "rvu_2025_A"
    batch_id = "real-cms-rvu-validation"
    source_files = [_source_file(path) for path in source_paths]

    land = await ingestor._land_stage(
        release_id=release_id,
        batch_id=batch_id,
        source_files=source_files,
    )
    assert land["status"] == "success"
    assert land["files_downloaded"] == 6
    assert len(land["manifest"]["files"]) == 5
    assert land["docs_manifest_path"] is not None
    assert Path(land["docs_manifest_path"]).exists()
    assert land["guidance_documents"][0]["filename"] == "RVU25A.pdf"

    raw_batch = land["raw_batch"]
    validate = await ingestor._validate_stage(raw_batch)
    assert validate["status"] == "success"
    assert validate["quality_score"] == 100.0
    assert validate["rejected_records"] == 0

    normalize = await ingestor._normalize_stage(validate, raw_batch)
    assert normalize["status"] == "success"
    assert normalize["dataset_row_counts"] == {
        "pprrvu": 18865,
        "gpci": 109,
        "oppscap": 16100,
        "anescf": 109,
        "localitycounty": 109,
    }

    schema_validation = normalize["schema_validation"]
    assert schema_validation["quality_score"] == 1.0
    assert schema_validation["rejected_records"] == 0
    assert schema_validation["errors"] == []
    assert set(schema_validation["validation_results"]) == {
        "pprrvu",
        "gpci",
        "oppscap",
        "anescf",
        "localitycounty",
    }
    assert all(
        result["valid"]
        for result in schema_validation["validation_results"].values()
    )

    pprrvu = normalize["dataframes"]["pprrvu"]
    assert set(pd.to_datetime(pprrvu["effective_from"]).dt.date) == {date(2025, 1, 1)}
    assert (
        pd.to_numeric(pprrvu["conversion_factor"], errors="coerce")
        .dropna()
        .unique()
        .tolist()
    ) == [32.3465]

    assert normalize["metadata"]["parser_rejects"] == {"oppscap": 1}
    reject_files = sorted(
        (tmp_path / "ingested" / "stage" / "cms_rvu" / release_id / "reject").glob(
            "oppscap_rejects_*.parquet"
        )
    )
    assert len(reject_files) == 1
    rejects = pd.read_parquet(reject_files[0])
    assert len(rejects) == 1
    assert rejects["_dataset"].tolist() == ["oppscap"]
    assert rejects["_release_id"].tolist() == [release_id]

    enrich = await ingestor._enrich_stage(normalize)
    assert enrich["status"] == "success"
    assert enrich["record_count"] == 35292

    publish = await ingestor._publish_stage(enrich)
    assert publish["status"] == "success"
    assert publish["record_count"] == 35292
    assert publish["database_load_results"] == {}

    curated_tables = publish["curated_tables"]
    for dataset in ["pprrvu", "gpci", "oppscap", "anescf", "localitycounty"]:
        assert dataset in curated_tables
        parquet_path = Path(curated_tables[dataset])
        assert parquet_path.exists()
        assert len(pd.read_parquet(parquet_path)) == normalize["dataset_row_counts"][dataset]

    assert {entry["dataset_id"] for entry in snapshot_service.registered} == {
        "rvu_items",
        "gpci_indices",
        "oppscap",
        "anescf",
        "localitycounty",
    }
    assert {entry["release_id"] for entry in snapshot_service.registered} == {
        "rvu_2025_A",
        "gpci_2025_A",
        "oppscap_2025_A",
        "anescf_2025_A",
        "locality_2025_A",
    }
    assert {entry["effective_from"] for entry in snapshot_service.registered} == {
        date(2025, 1, 1)
    }
