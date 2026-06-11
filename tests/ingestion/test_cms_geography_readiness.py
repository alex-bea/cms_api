from collections import Counter
from datetime import date
from pathlib import Path

from cms_pricing.ingestion.parsers.cms_geography import (
    ParsedGeographyRow,
    SourceStats,
)
from cms_pricing.ingestion.validators.cms_geography_readiness import (
    CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    StateLocalityPair,
    validate_gpci_join_readiness,
    validate_smoke_proof_path,
    validate_source_readiness,
)


def _probe_row(
    *,
    state: str = "CA",
    locality_id: str = "05",
    carrier: str = "01112",
    effective_to: date | None = None,
) -> ParsedGeographyRow:
    return ParsedGeographyRow(
        source_file="ZIP5_OCT2025.txt",
        source_line=1,
        zip5="94110",
        plus4=None,
        has_plus4=0,
        state=state,
        carrier=carrier,
        locality_id=locality_id,
        rural_flag=None,
        plus_four_flag="0",
        part_b_payment_indicator="A",
        year_quarter="20254",
        effective_from=date(2025, 10, 1),
        effective_to=effective_to,
    )


def _stats(
    *,
    zip5_rows: int = 42_956,
    zip9_rows: int = 1_076_014,
    rejected_rows: int = 0,
    duplicate_source_keys: int = 0,
    locality_00_rows: int = 39_476,
    probe_row: ParsedGeographyRow | None = None,
    effective_to: date | None = None,
) -> SourceStats:
    probe = probe_row or _probe_row(effective_to=effective_to)
    return SourceStats(
        source_zip=Path("/tmp/zip-locality.zip"),
        dataset_id="ZIP_LOCALITY",
        dataset_digest="digest",
        source_files=["ZIP5_OCT2025.txt", "ZIP9_OCT2025.txt"],
        probe_zip="94110",
        zip5_rows=zip5_rows,
        zip9_rows=zip9_rows,
        rejected_rows=rejected_rows,
        duplicate_source_keys=duplicate_source_keys,
        locality_counts=Counter({"00": locality_00_rows, "05": 100}),
        year_quarter_counts=Counter({"20254": zip5_rows + zip9_rows}),
        zip5_key_count=zip5_rows,
        zip9_key_count=zip9_rows,
        effective_from=date(2025, 10, 1),
        effective_to=effective_to,
        probe_rows=[probe],
    )


def test_source_readiness_accepts_checkpoint_thresholds():
    report = validate_source_readiness(
        _stats(),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
        valuation_date=date(2026, 7, 1),
        require_valuation_date_coverage=True,
    )

    assert report["status"] == "ok"
    assert report["failed_gates"] == []


def test_source_readiness_fails_rejects_and_duplicate_keys():
    report = validate_source_readiness(
        _stats(rejected_rows=1, duplicate_source_keys=1),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    )

    assert report["status"] == "blocked"
    assert "source_package_clean" in report["failed_gates"]


def test_source_readiness_fails_row_regressions_and_locality_00_loss():
    report = validate_source_readiness(
        _stats(zip5_rows=42_955, zip9_rows=1_076_013, locality_00_rows=0),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    )

    assert report["status"] == "blocked"
    assert "row_count_regression" in report["failed_gates"]
    assert "locality_00_preserved" in report["failed_gates"]


def test_source_readiness_fails_probe_mismatch():
    report = validate_source_readiness(
        _stats(probe_row=_probe_row(state="CA", locality_id="18")),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    )

    assert report["status"] == "blocked"
    assert "probe_zip_match" in report["failed_gates"]


def test_source_readiness_blocks_strict_gap_and_accepts_open_ended_latest():
    strict_report = validate_source_readiness(
        _stats(effective_to=date(2025, 12, 31)),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
        valuation_date=date(2026, 7, 1),
        require_valuation_date_coverage=True,
    )
    open_ended_report = validate_source_readiness(
        _stats(effective_to=None),
        thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
        valuation_date=date(2026, 7, 1),
        require_valuation_date_coverage=True,
    )

    assert "valuation_date_coverage" in strict_report["failed_gates"]
    assert open_ended_report["status"] == "ok"


def test_gpci_join_readiness_accepts_direct_and_governed_mapped_pairs():
    report = validate_gpci_join_readiness(
        [
            StateLocalityPair("CA", "05"),
            StateLocalityPair("AS", "01"),
            StateLocalityPair("VA", "01"),
        ],
        [
            StateLocalityPair("CA", "05"),
            StateLocalityPair("HI", "01"),
            StateLocalityPair("DC", "01"),
        ],
    )

    assert report["status"] == "ok"
    assert report["direct_join_count"] == 1
    assert report["mapped_join_count"] == 2
    assert report["missing_join_count"] == 0


def test_gpci_join_readiness_reports_classified_misses():
    report = validate_gpci_join_readiness(
        [StateLocalityPair("CA", "18")],
        [StateLocalityPair("CA", "05")],
    )

    assert report["status"] == "blocked"
    assert report["missing_join_count"] == 1
    assert report["missing_examples"] == [
        {
            "state": "CA",
            "locality_id": "18",
            "state_candidates": ["CA"],
            "locality_candidates": ["18"],
        }
    ]


def test_smoke_proof_path_refuses_seed_helper_dependency():
    accepted = validate_smoke_proof_path("post_rvu_load_api_smoke")
    refused = validate_smoke_proof_path("scripts/seed_post_rvu_load_local.py")

    assert accepted["status"] == "ok"
    assert refused["status"] == "blocked"
    assert refused["stop_condition"] == "seed_helper_proof_path_refused"
