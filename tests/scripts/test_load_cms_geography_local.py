from datetime import date
import zipfile

import pytest

from cms_pricing.ingestion.parsers.cms_geography import (
    GeographyLoadError,
    build_source_report,
    parse_zip5_line,
    parse_zip9_line,
    quarter_window,
    scan_source_zip,
    valuation_date_covered,
)


def _fixed_line(
    *,
    state: str = "CA",
    zip5: str = "94110",
    carrier: str = "01112",
    locality: str = "05",
    rural: str = " ",
    plus_four_flag: str = "0",
    plus4: str = "    ",
    part_b: str = "A",
    year_quarter: str = "20254",
    zip9: bool = False,
) -> str:
    chars = [" "] * 80
    chars[0:2] = list(state)
    chars[2:7] = list(zip5)
    chars[7:12] = list(carrier)
    chars[12:14] = list(locality)
    chars[14:15] = list(rural)
    chars[20:21] = list(plus_four_flag)
    if zip9:
        chars[21:25] = list(plus4)
        chars[31:32] = list(part_b)
    else:
        chars[22:23] = list(part_b)
    chars[75:80] = list(year_quarter)
    return "".join(chars)


def _write_source_zip(path, zip5_lines, zip9_lines):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("ZIP5_OCT2025.txt", "\n".join(zip5_lines) + "\n")
        archive.writestr("ZIP5lyout.txt", "layout")
        archive.writestr("ZIP9_OCT2025.txt", "\n".join(zip9_lines) + "\n")
        archive.writestr("ZIP9lyout.txt", "layout")


def test_quarter_window_maps_cms_year_quarter():
    assert quarter_window("20254") == (date(2025, 10, 1), date(2025, 12, 31))
    assert quarter_window("20261") == (date(2026, 1, 1), date(2026, 3, 31))


def test_parse_zip5_line_preserves_source_strings_and_locality_00():
    row = parse_zip5_line(
        _fixed_line(
            state="NY",
            zip5="00901",
            carrier="00882",
            locality="00",
            rural="R",
            plus_four_flag="1",
        ),
        source_file="ZIP5_OCT2025.txt",
        source_line=1,
    )

    assert row.zip5 == "00901"
    assert row.plus4 is None
    assert row.has_plus4 == 0
    assert row.state == "NY"
    assert row.carrier == "00882"
    assert row.locality_id == "00"
    assert row.rural_flag == "R"
    assert row.plus_four_flag == "1"
    assert row.effective_from == date(2025, 10, 1)
    assert row.effective_to == date(2025, 12, 31)


def test_parse_zip9_line_preserves_plus4_and_leading_zeroes():
    row = parse_zip9_line(
        _fixed_line(
            state="MA",
            zip5="01434",
            carrier="10112",
            locality="01",
            rural="B",
            plus_four_flag="1",
            plus4="0001",
            zip9=True,
        ),
        source_file="ZIP9_OCT2025.txt",
        source_line=1,
    )

    assert row.zip5 == "01434"
    assert row.plus4 == "0001"
    assert row.has_plus4 == 1
    assert row.state == "MA"
    assert row.carrier == "10112"
    assert row.locality_id == "01"
    assert row.rural_flag == "B"


def test_scan_source_zip_reports_counts_probe_and_valuation_gap(tmp_path):
    source_zip = tmp_path / "zip-locality.zip"
    _write_source_zip(
        source_zip,
        [
            _fixed_line(
                state="CA", zip5="94110", carrier="01112", locality="05"
            ),
            _fixed_line(
                state="NY", zip5="10001", carrier="31102", locality="00"
            ),
        ],
        [
            _fixed_line(
                state="CA",
                zip5="94110",
                carrier="01112",
                locality="05",
                plus_four_flag="1",
                plus4="0007",
                zip9=True,
            )
        ],
    )

    stats = scan_source_zip(
        source_zip, dataset_digest="digest", probe_zip="94110"
    )

    assert stats.zip5_rows == 2
    assert stats.zip9_rows == 1
    assert stats.zip5_key_count == 2
    assert stats.zip9_key_count == 1
    assert stats.locality_counts["00"] == 1
    assert stats.probe_rows[0].state == "CA"
    assert stats.probe_rows[0].locality_id == "05"
    assert valuation_date_covered(stats, date(2026, 7, 1)) is False

    report = build_source_report(
        stats,
        source_url="https://example.test/cms-geography.zip",
        expected_probe_state="CA",
        expected_probe_locality="05",
        expected_probe_carrier="01112",
        valuation_date=date(2026, 7, 1),
        require_valuation_date_coverage=True,
    )

    assert report["status"] == "blocked"
    assert (
        report["stop_condition"]
        == "source_effective_window_does_not_cover_valuation_date"
    )
    assert report["source"]["source_files"] == [
        "ZIP5_OCT2025.txt",
        "ZIP9_OCT2025.txt",
    ]
    assert report["counts"]["rows_total"] == 3
    assert report["counts"]["locality_00_rows"] == 1
    assert report["probe"]["expected_found"] is True


def test_scan_source_zip_can_mark_latest_source_open_ended(tmp_path):
    source_zip = tmp_path / "zip-locality.zip"
    _write_source_zip(
        source_zip,
        [
            _fixed_line(
                state="CA", zip5="94110", carrier="01112", locality="05"
            )
        ],
        [
            _fixed_line(
                state="CA",
                zip5="94110",
                carrier="01112",
                locality="05",
                plus_four_flag="1",
                plus4="0007",
                zip9=True,
            )
        ],
    )

    stats = scan_source_zip(
        source_zip,
        dataset_digest="digest",
        probe_zip="94110",
        open_ended_latest=True,
    )

    assert stats.effective_from == date(2025, 10, 1)
    assert stats.effective_to is None
    assert valuation_date_covered(stats, date(2026, 7, 1)) is True


def test_scan_source_zip_fails_on_duplicate_active_keys(tmp_path):
    source_zip = tmp_path / "zip-locality.zip"
    duplicate = _fixed_line(
        state="CA", zip5="94110", carrier="01112", locality="05"
    )
    _write_source_zip(source_zip, [duplicate, duplicate], [])

    with pytest.raises(GeographyLoadError, match="duplicate source keys"):
        scan_source_zip(source_zip, dataset_digest="digest", probe_zip="94110")
