import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor


def _make_ingestor(tmp_path: Path) -> tuple[RVUIngestor, MagicMock]:
    """Create an RVUIngestor with a mocked DB session so loader helpers can be exercised in isolation."""
    session = MagicMock()
    nested = MagicMock()
    nested.__enter__.return_value = None
    nested.__exit__.return_value = False
    session.begin_nested.return_value = nested

    ingestor = RVUIngestor(output_dir=str(tmp_path), db_session=session)
    return ingestor, session


def _extract_records(session: MagicMock) -> list[dict]:
    """Return the list of records passed to bulk_insert_mappings."""
    assert session.bulk_insert_mappings.called, "bulk_insert_mappings was not invoked"
    return session.bulk_insert_mappings.call_args[0][1]


def test_gpci_loader_aliases(tmp_path):
    ingestor, session = _make_ingestor(tmp_path)
    df = pd.DataFrame(
        [
            {
                "mac": "01112",
                "state": "ca",
                "locality_code": "05",
                "locality_name": "SAN FRANCISCO",
                "gpci_work": 1.088,
                "gpci_pe": 1.419,
                "gpci_mp": 0.445,
            }
        ]
    )

    inserted = ingestor._load_gpci_data(df, uuid.uuid4(), batch_id="test")
    assert inserted == 1
    records = _extract_records(session)
    assert records[0]["work_gpci"] == 1.088
    assert records[0]["pe_gpci"] == 1.419
    assert records[0]["mp_gpci"] == 0.445


def test_oppscap_loader_aliases(tmp_path):
    ingestor, session = _make_ingestor(tmp_path)
    df = pd.DataFrame(
        [
            {
                "hcpcs": "99213",
                "modifier": None,
                "status": "A",
                "mac": "01112",
                "locality_code": "05",
                "facility_price": 45.50,
                "nonfacility_price": 67.80,
            }
        ]
    )

    inserted = ingestor._load_oppscap_data(df, uuid.uuid4(), batch_id="test")
    assert inserted == 1
    records = _extract_records(session)
    assert records[0]["hcpcs_code"] == "99213"
    assert records[0]["proc_status"] == "A"
    assert records[0]["price_fac"] == 45.50
    assert records[0]["price_nonfac"] == 67.80


def test_locality_loader_aliases(tmp_path):
    ingestor, session = _make_ingestor(tmp_path)
    df = pd.DataFrame(
        [
            {
                "mac": "01112",
                "locality_code": "05",
                "state_name": "California",
                "fee_area": "SAN FRANCISCO-OAKLAND",
                "county_names": "SAN FRANCISCO",
            }
        ]
    )

    inserted = ingestor._load_locality_data(df, uuid.uuid4(), batch_id="test")
    assert inserted == 1
    records = _extract_records(session)
    assert records[0]["state"] == "CALIFORNIA"
    assert records[0]["fee_schedule_area"].startswith("SAN FRANCISCO")
    assert records[0]["county_name"] == "SAN FRANCISCO"


def test_pprrvu_loader_normalises_modifier(tmp_path):
    ingestor, session = _make_ingestor(tmp_path)
    df = pd.DataFrame(
        [
            {
                "hcpcs": "99213",
                "modifier": pd.NA,
                "description": "OFFICE/OUTPATIENT VISIT",
                "status_code": "A",
                "work_rvu": 1.50,
                "pe_rvu_nonfac": 1.35,
                "pe_rvu_fac": 0.90,
                "mp_rvu": 0.09,
            }
        ]
    )

    inserted = ingestor._load_pprrvu_data(df, uuid.uuid4(), batch_id="test")
    assert inserted == 1
    records = _extract_records(session)
    assert records[0]["hcpcs_code"] == "99213"
    assert records[0]["modifier_key"] is None
    assert records[0]["modifiers"] is None
