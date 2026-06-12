from datetime import datetime
from pathlib import Path

import pytest

from cms_pricing.ingestion.ingestors.opps_ingestor import (
    TABLE_OPPS_APC_PAYMENT,
    TABLE_OPPS_HCPCS_CROSSWALK,
    OPPSIngestor,
)
from cms_pricing.ingestion.scrapers.cms_opps_scraper import ScrapedFileInfo


def _file_info(path: Path, file_type: str) -> ScrapedFileInfo:
    return ScrapedFileInfo(
        url=f"https://www.cms.gov/files/zip/{path.name}",
        filename=path.name,
        file_type=file_type,
        batch_id="opps_2026q2_r1",
        discovered_at=datetime.utcnow(),
        source_page="https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates",
        metadata={"year": 2026, "quarter": 2},
        local_path=path,
    )


@pytest.mark.asyncio
async def test_parse_addendum_a_skips_cms_preamble_and_normalizes_money(tmp_path):
    source = tmp_path / "addendum_a.csv"
    source.write_text(
        "\n".join(
            [
                ",Addendum A.- OPPS APCs for CY 2026,,,,",
                ",CMS preamble,,,,",
                "APC,Group Title,SI,Relative Weight,Payment Rate,Note",
                '0701,Sr89 strontium,K,1.2345,"$4,146.335",',
            ]
        ),
        encoding="utf-8",
    )

    ingestor = OPPSIngestor(output_dir=tmp_path / "out")
    result = await ingestor._parse_addendum_a(_file_info(source, "addendum_a"))

    assert result.to_dict("records") == [
        {
            "apc_code": "0701",
            "apc_description": "Sr89 strontium",
            "payment_rate_usd": result.loc[0, "payment_rate_usd"],
            "relative_weight": result.loc[0, "relative_weight"],
            "packaging_flag": None,
        }
    ]
    assert str(result.loc[0, "payment_rate_usd"]) == "4146.335"
    assert str(result.loc[0, "relative_weight"]) == "1.2345"


@pytest.mark.asyncio
async def test_parse_addendum_b_preserves_two_character_status_indicators(tmp_path):
    source = tmp_path / "addendum_b.csv"
    source.write_text(
        "\n".join(
            [
                ",Addendum B.-- OPPS Payment by HCPCS Code for CY 2026,,,,",
                ",CMS preamble,,,,",
                "HCPCS Code,Short Descriptor,SI,APC,Payment Rate",
                'C1600,Test device,Q1,5115,"$123.45"',
            ]
        ),
        encoding="utf-8",
    )

    ingestor = OPPSIngestor(output_dir=tmp_path / "out")
    result = await ingestor._parse_addendum_b(_file_info(source, "addendum_b"))

    assert result.to_dict("records") == [
        {
            "hcpcs_code": "C1600",
            "modifier": None,
            "status_indicator": "Q1",
            "apc_code": "5115",
            "payment_context": "Test device",
        }
    ]


@pytest.mark.asyncio
async def test_parse_addendum_zip_prefers_csv_and_uses_contract_table_names(tmp_path):
    import zipfile

    source = tmp_path / "opps.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "508 Version April 2026 Web Addendum A/2026 April Web Addendum A.csv",
            "APC,Group Title,SI,Relative Weight,Payment Rate\n0701,Sr89,K,1.0,$2.00\n",
        )
        archive.writestr(
            "508 Version April 2026 Web Addendum B/2026 April Web Addendum B.csv",
            "HCPCS Code,Short Descriptor,SI,APC\nC1600,Test device,S1,0701\n",
        )

    ingestor = OPPSIngestor(output_dir=tmp_path / "out")
    parsed = await ingestor._parse_zip_file(_file_info(source, "addendum_zip"))

    assert set(parsed) == {TABLE_OPPS_APC_PAYMENT, TABLE_OPPS_HCPCS_CROSSWALK}
    assert len(parsed[TABLE_OPPS_APC_PAYMENT]) == 1
    assert parsed[TABLE_OPPS_HCPCS_CROSSWALK].loc[0, "status_indicator"] == "S1"
