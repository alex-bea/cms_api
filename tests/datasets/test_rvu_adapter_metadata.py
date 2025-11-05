import datetime

from cms_pricing.ingestion.datasets.rvu_adapter import _build_parser_metadata
from cms_pricing.ingestion.datasets.rvu_spec import RVU_DATASETS


def test_build_parser_metadata_infers_october_quarter():
    spec = RVU_DATASETS["pprrvu"]
    metadata = _build_parser_metadata(
        dataset_key="pprrvu",
        spec=spec,
        release_id="rvu_2025_oct",
        source_file=None,
        inner_filename="PPRRVU2025_Oct.txt",
        file_bytes=b"dummy",
        batch_id=None,
        derive_release_context=None,
    )

    assert metadata["product_year"] == "2025"
    assert metadata["quarter_vintage"] == "2025Q4"
    assert metadata["layout_version"] == "v2025D.0"
    assert isinstance(metadata["vintage_date"], datetime.datetime)


def test_build_parser_metadata_infers_january_quarter():
    spec = RVU_DATASETS["pprrvu"]
    metadata = _build_parser_metadata(
        dataset_key="pprrvu",
        spec=spec,
        release_id="rvu_2025_jan",
        source_file=None,
        inner_filename="PPRRVU25_JAN.txt",
        file_bytes=b"dummy",
        batch_id=None,
        derive_release_context=None,
    )

    assert metadata["product_year"] == "2025"
    assert metadata["quarter_vintage"] == "2025Q1"
    assert metadata["layout_version"] == "v2025A.0"
    assert isinstance(metadata["vintage_date"], datetime.datetime)


def test_build_parser_metadata_overrides_annual_metadata():
    spec = RVU_DATASETS["pprrvu"]

    def derived_context(filename, release_id):
        return {
            "product_year": "2025",
            "quarter_vintage": "2025_annual",
            "vintage_date": datetime.datetime(2025, 1, 1),
            "release_letter": "A",
        }

    metadata = _build_parser_metadata(
        dataset_key="pprrvu",
        spec=spec,
        release_id="rvu_2025_override",
        source_file=None,
        inner_filename="PPRRVU2025_Oct.txt",
        file_bytes=b"dummy",
        batch_id=None,
        derive_release_context=derived_context,
    )

    assert metadata["quarter_vintage"] == "2025Q4"
    assert metadata["layout_version"] == "v2025D.0"
