import asyncio
from datetime import date
import zipfile

import pytest

from cms_pricing.ingestion.ingestors import CMSZipLocalityProductionIngester
from cms_pricing.ingestion.parsers.cms_geography import (
    GeographyLoadError,
    scan_source_zip,
)
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.models.geography import Geography


def _fixed_line(
    *,
    state: str = "CA",
    zip5: str = "94110",
    carrier: str = "01112",
    locality: str = "05",
    rural: str = " ",
    plus_four_flag: str = "0",
    plus4: str = "    ",
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
        chars[31:32] = list("A")
    else:
        chars[22:23] = list("A")
    chars[75:80] = list(year_quarter)
    return "".join(chars)


def _write_source_zip(path):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "ZIP5_OCT2025.txt",
            "\n".join(
                [
                    _fixed_line(),
                    _fixed_line(
                        state="NY",
                        zip5="10001",
                        carrier="31102",
                        locality="00",
                    ),
                ]
            )
            + "\n",
        )
        archive.writestr(
            "ZIP9_OCT2025.txt",
            _fixed_line(
                plus_four_flag="1",
                plus4="0007",
                zip9=True,
            )
            + "\n",
        )


def _source_stats(tmp_path):
    source_zip = tmp_path / "zip-locality.zip"
    _write_source_zip(source_zip)
    stats = scan_source_zip(
        source_zip,
        dataset_digest="digest",
        open_ended_latest=True,
    )
    return source_zip, stats


class _BulkSession:
    def __init__(self):
        self.bulk_calls = []

    def bulk_insert_mappings(self, model, batch):
        self.bulk_calls.append((model, list(batch)))


class _SnapshotSession:
    def __init__(self):
        self.snapshots = {}

    def get(self, model, key):
        assert model is DatasetSnapshot
        return self.snapshots.get(key)

    def add(self, snapshot):
        self.snapshots[(snapshot.dataset_id, snapshot.release_id)] = snapshot


def test_normalize_uses_landed_source_stats_instead_of_database(tmp_path):
    source_zip, stats = _source_stats(tmp_path)
    ingester = CMSZipLocalityProductionIngester(output_dir=str(tmp_path))
    ingester.current_source_zip_path = source_zip
    ingester.current_source_stats = stats

    normalized = asyncio.run(ingester._normalize_data(db=object()))

    assert normalized["geography"]["source_zip"] == source_zip
    assert normalized["geography"]["stats"] is stats


def test_insert_geography_rows_preserves_zip5_zip9_and_locality_00(tmp_path):
    source_zip, stats = _source_stats(tmp_path)
    session = _BulkSession()
    ingester = CMSZipLocalityProductionIngester(
        output_dir=str(tmp_path), batch_size=2, open_ended_latest=True
    )

    inserted = ingester._insert_geography_rows(session, source_zip, stats)

    assert inserted == 3
    assert all(call[0] is Geography for call in session.bulk_calls)
    rows = [row for _, batch in session.bulk_calls for row in batch]
    assert {row["zip5"] for row in rows} == {"94110", "10001"}
    assert any(
        row["plus4"] == "0007" and row["has_plus4"] == 1
        for row in rows
    )
    assert any(row["locality_id"] == "00" for row in rows)
    assert {row["dataset_digest"] for row in rows} == {"digest"}


def test_existing_overlap_requires_explicit_replace(tmp_path, monkeypatch):
    _, stats = _source_stats(tmp_path)
    ingester = CMSZipLocalityProductionIngester(
        output_dir=str(tmp_path), replace_existing=False
    )
    counts = iter([0, 10, 10])
    monkeypatch.setattr(
        ingester,
        "_count_geography_rows",
        lambda db, filter_clause: next(counts),
    )

    with pytest.raises(GeographyLoadError, match="replace_existing=True"):
        ingester._inspect_existing_geography(db=object(), stats=stats)


def test_same_digest_existing_rows_are_reused(tmp_path, monkeypatch):
    _, stats = _source_stats(tmp_path)
    ingester = CMSZipLocalityProductionIngester(output_dir=str(tmp_path))
    counts = iter([3, 3, 0])
    monkeypatch.setattr(
        ingester,
        "_count_geography_rows",
        lambda db, filter_clause: next(counts),
    )

    existing = ingester._inspect_existing_geography(db=object(), stats=stats)

    assert existing["action"] == "reuse_existing"
    assert existing["same_digest_count"] == 3


def test_ensure_snapshot_registers_zip_locality_snapshot(tmp_path):
    _, stats = _source_stats(tmp_path)
    session = _SnapshotSession()
    ingester = CMSZipLocalityProductionIngester(output_dir=str(tmp_path))
    ingester.current_release_id = "zip_locality_2025_Q4"

    result = ingester._ensure_snapshot(session, stats)

    assert result == {
        "dataset_id": "ZIP_LOCALITY",
        "release_id": "zip_locality_2025_Q4",
        "action": "inserted",
    }
    snapshot = session.snapshots[("ZIP_LOCALITY", "zip_locality_2025_Q4")]
    assert snapshot.digest == "digest"
    assert snapshot.effective_from == date(2025, 10, 1)
    assert snapshot.effective_to is None
