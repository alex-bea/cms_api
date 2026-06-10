import hashlib
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from cms_pricing.ingestion.datasets.mpfs_builder import (
    MPFSNormalizedInputs,
    build_curated_views,
)
from cms_pricing.ingestion.parsers.gpci_parser import (
    SCHEMA_ID as GPCI_SCHEMA_ID,
    parse_gpci,
)
from cms_pricing.ingestion.parsers.pprrvu_parser import (
    SCHEMA_ID as PPRRVU_SCHEMA_ID,
    parse_pprrvu,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.ingestor
def test_real_cms_pprrvu_and_gpci_samples_build_mpfs_payments():
    pprrvu_path = Path("sample_data/rvu25a/PPRRVU25_JAN.csv")
    gpci_path = Path("sample_data/rvu25a/GPCI2025.csv")

    assert pprrvu_path.exists(), "Expected checked-in CMS PPRRVU sample"
    assert gpci_path.exists(), "Expected checked-in CMS GPCI sample"

    pprrvu_metadata = {
        "release_id": "mpfs_2025_A_real_sample",
        "product_year": "2025",
        "quarter_vintage": "2025Q1",
        "vintage_date": datetime(2025, 1, 1),
        "file_sha256": _sha256(pprrvu_path),
        "source_uri": str(pprrvu_path),
        "schema_id": PPRRVU_SCHEMA_ID,
        "layout_version": "v2025.1.0",
    }
    gpci_metadata = {
        "release_id": "gpci_2025_A_real_sample",
        "schema_id": GPCI_SCHEMA_ID,
        "product_year": "2025",
        "quarter_vintage": "2025Q1",
        "vintage_date": datetime(2025, 1, 1),
        "file_sha256": _sha256(gpci_path),
        "source_uri": str(gpci_path),
        "source_release": "RVU25A",
    }

    with pprrvu_path.open("rb") as pprrvu_file:
        pprrvu_result = parse_pprrvu(pprrvu_file, pprrvu_path.name, pprrvu_metadata)
    with gpci_path.open("rb") as gpci_file:
        gpci_result = parse_gpci(gpci_file, gpci_path.name, gpci_metadata)

    assert len(pprrvu_result.data) > 18_000
    assert pprrvu_result.rejects.empty
    assert 100 <= len(gpci_result.data) <= 120
    assert gpci_result.rejects.empty

    rvu_subset = pprrvu_result.data[
        pprrvu_result.data["hcpcs"].isin(["93000", "99213"])
    ].copy()
    assert set(rvu_subset["hcpcs"]) == {"93000", "99213"}

    cf_value = (
        pd.to_numeric(rvu_subset["conversion_factor"], errors="coerce")
        .dropna()
        .iloc[0]
    )
    assert cf_value == pytest.approx(32.3465, rel=1e-6)

    views = build_curated_views(
        MPFSNormalizedInputs(
            rvu=rvu_subset,
            gpci=gpci_result.data,
            conversion_factor=pd.DataFrame(
                [
                    {
                        "year": 2025,
                        "cf_type": "physician",
                        "cf_value": cf_value,
                        "effective_start": date(2025, 1, 1),
                        "effective_end": date(2025, 12, 31),
                    }
                ]
            ),
            release_id="mpfs_2025_A_real_sample",
            vintage_date=date(2025, 1, 1),
        )
    )

    payment = views["mpfs_payment_curated"]
    assert len(payment) == len(rvu_subset) * len(gpci_result.data)
    assert not payment[
        [
            "work_rvu",
            "pe_rvu_nonfac",
            "pe_rvu_fac",
            "mp_rvu",
            "work_gpci",
            "pe_gpci",
            "mp_gpci",
            "conversion_factor",
        ]
    ].isna().any().any()

    sample_payment = payment[
        (payment["hcpcs_code"] == "99213") & (payment["locality_id"] == "05")
    ].iloc[0]
    expected_nonfacility = (
        sample_payment["work_rvu"] * sample_payment["work_gpci"]
        + sample_payment["pe_rvu_nonfac"] * sample_payment["pe_gpci"]
        + sample_payment["mp_rvu"] * sample_payment["mp_gpci"]
    ) * sample_payment["conversion_factor"]
    expected_facility = (
        sample_payment["work_rvu"] * sample_payment["work_gpci"]
        + sample_payment["pe_rvu_fac"] * sample_payment["pe_gpci"]
        + sample_payment["mp_rvu"] * sample_payment["mp_gpci"]
    ) * sample_payment["conversion_factor"]

    assert sample_payment["payment_nonfacility"] == pytest.approx(expected_nonfacility, rel=1e-9)
    assert sample_payment["payment_facility"] == pytest.approx(expected_facility, rel=1e-9)
    assert sample_payment["payment_nonfacility"] > sample_payment["payment_facility"]
