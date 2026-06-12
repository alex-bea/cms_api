import asyncio
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cms_pricing.database import Base
from cms_pricing.engines.opps import OPPSEngine
from cms_pricing.ingestion.ingestors.opps_ingestor import (
    TABLE_OPPS_APC_PAYMENT,
    TABLE_OPPS_HCPCS_CROSSWALK,
    OPPSBatchInfo,
    OPPSIngestor,
)
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.models.opps import OPPSAPCPayment, OPPSHCPCSCrosswalk, RefSILookup
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


@pytest.fixture()
def opps_sqlite_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        bind=engine,
        tables=[
            OPPSAPCPayment.__table__,
            OPPSHCPCSCrosswalk.__table__,
            RefSILookup.__table__,
        ],
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE dataset_snapshots (
                    dataset_id VARCHAR(50) NOT NULL,
                    release_id VARCHAR(50) NOT NULL,
                    digest VARCHAR(64) NOT NULL,
                    effective_from DATE NOT NULL,
                    effective_to DATE,
                    manifest_url VARCHAR(500),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    PRIMARY KEY (dataset_id, release_id)
                )
                """
            )
        )
        connection.execute(
            text(
                "CREATE INDEX idx_dataset_snapshots_dataset_effective "
                "ON dataset_snapshots (dataset_id, effective_from, effective_to)"
            )
        )
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def opps_batch_info():
    return OPPSBatchInfo(
        batch_id="opps_2026q2_r1",
        year=2026,
        quarter=2,
        release_number=1,
        effective_from=date(2026, 4, 1),
        effective_to=date(2026, 6, 30),
        files=[],
        discovered_at=datetime(2026, 3, 31, 12, 0, 0),
        downloaded_at=datetime(2026, 3, 31, 12, 5, 0),
    )


def _normalized_opps_frames():
    return {
        TABLE_OPPS_APC_PAYMENT: pd.DataFrame(
            [
                {
                    "apc_code": "5115",
                    "apc_description": "Level 5 Musculoskeletal Procedures",
                    "payment_rate_usd": Decimal("123.456"),
                    "relative_weight": Decimal("1.2345"),
                    "packaging_flag": None,
                },
                {
                    "apc_code": "9999",
                    "apc_description": "Context-dependent packaged item",
                    "payment_rate_usd": None,
                    "relative_weight": None,
                    "packaging_flag": None,
                },
            ]
        ),
        TABLE_OPPS_HCPCS_CROSSWALK: pd.DataFrame(
            [
                {
                    "hcpcs_code": "C1600",
                    "modifier": None,
                    "status_indicator": "S",
                    "apc_code": "5115",
                    "payment_context": None,
                },
                {
                    "hcpcs_code": "C1601",
                    "modifier": None,
                    "status_indicator": "Q1",
                    "apc_code": "9999",
                    "payment_context": None,
                },
                {
                    "hcpcs_code": "C1600",
                    "modifier": "TC",
                    "status_indicator": "S1",
                    "apc_code": "5115",
                    "payment_context": None,
                },
            ]
        ),
    }


def test_opps_validation_gates_reject_unknown_si_and_missing_apc(tmp_path):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    normalized = _normalized_opps_frames()
    normalized[TABLE_OPPS_HCPCS_CROSSWALK] = pd.concat(
        [
            normalized[TABLE_OPPS_HCPCS_CROSSWALK],
            pd.DataFrame(
                [
                    {
                        "hcpcs_code": "C9999",
                        "modifier": None,
                        "status_indicator": "ZZ",
                        "apc_code": "1234",
                        "payment_context": None,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    validation = ingestor.validate_normalized_opps_data(normalized)

    assert validation["passed"] is False
    assert any("unknown status indicators" in error for error in validation["errors"])
    assert any(
        "APCs missing from Addendum A" in error for error in validation["errors"]
    )


def test_opps_validation_allows_blank_packaged_apc_rate_but_rejects_payable_rate(
    tmp_path,
):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    normalized = _normalized_opps_frames()

    validation = ingestor.validate_normalized_opps_data(normalized)

    assert validation["passed"] is True
    assert any(
        "blank payment rates only for non-separately payable" in warning
        for warning in validation["warnings"]
    )

    normalized[TABLE_OPPS_APC_PAYMENT].loc[
        normalized[TABLE_OPPS_APC_PAYMENT]["apc_code"] == "5115",
        "payment_rate_usd",
    ] = None
    validation = ingestor.validate_normalized_opps_data(normalized)

    assert validation["passed"] is False
    assert any("separately payable APCs" in error for error in validation["errors"])


def test_opps_persist_registers_snapshots_and_prices_source_tables(
    tmp_path,
    opps_sqlite_session,
    opps_batch_info,
):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    loaded = ingestor.persist_normalized_opps_data(
        opps_sqlite_session,
        _normalized_opps_frames(),
        opps_batch_info,
    )
    snapshots = ingestor.register_opps_snapshots(
        opps_sqlite_session,
        opps_batch_info,
        source_digest="test-digest",
    )
    opps_sqlite_session.commit()

    assert loaded["tables_loaded"][TABLE_OPPS_APC_PAYMENT] == 2
    assert loaded["tables_loaded"][TABLE_OPPS_HCPCS_CROSSWALK] == 3
    assert loaded["tables_loaded"]["ref_si_lookup"] == 3
    assert {snapshot["dataset_id"] for snapshot in snapshots} == {
        "OPPS",
        TABLE_OPPS_APC_PAYMENT,
        TABLE_OPPS_HCPCS_CROSSWALK,
        "ref_si_lookup",
    }

    selected = DatasetSnapshotService(opps_sqlite_session).select_snapshot(
        "OPPS",
        valuation_date=date(2026, 4, 15),
    )
    assert selected.release_id == "opps_2026q2_r1"

    engine = OPPSEngine(db=opps_sqlite_session)
    result = asyncio.run(
        engine.price_code(
            code="C1600",
            zip="94110",
            year=2026,
            quarter="2",
            valuation_date=date(2026, 4, 15),
            facility_component=True,
            professional_component=False,
        )
    )

    assert result.allowed_cents == 12346
    assert result.beneficiary_coinsurance_cents == 2469
    assert result.program_payment_cents == 9876
    assert result.packaged is False
    assert result.release_id == "opps_2026q2_r1"
    assert "OPPS:apc:5115" in result.trace_refs


def test_opps_resolver_returns_zero_for_context_required_packaging(
    tmp_path,
    opps_sqlite_session,
    opps_batch_info,
):
    ingestor = OPPSIngestor(output_dir=tmp_path)
    ingestor.persist_normalized_opps_data(
        opps_sqlite_session,
        _normalized_opps_frames(),
        opps_batch_info,
    )
    ingestor.register_opps_snapshots(opps_sqlite_session, opps_batch_info)
    opps_sqlite_session.commit()

    engine = OPPSEngine(db=opps_sqlite_session)
    result = asyncio.run(
        engine.price_code(
            code="C1601",
            zip="94110",
            year=2026,
            quarter="2",
            valuation_date=date(2026, 4, 15),
            facility_component=True,
            professional_component=False,
        )
    )

    assert result.allowed_cents == 0
    assert result.facility_allowed_cents == 0
    assert result.packaged is True
    assert "OPPS:packaging:context_required" in result.trace_refs
