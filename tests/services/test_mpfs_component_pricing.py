from unittest.mock import MagicMock
from datetime import date
from decimal import Decimal
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from cms_pricing.engines.mpfs import MPSFEngine
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.models.rvu import GPCIIndex, Release, RVUItem
from cms_pricing.schemas.geography import GeographyCandidate, GeographyResolveResponse


def _require_table(db_session, table_name: str) -> None:
    try:
        db_session.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
    except (ProgrammingError, OperationalError) as exc:
        if "does not exist" in str(exc) or "relation" in str(exc).lower():
            pytest.skip(
                f"Table {table_name} is not available. Run alembic migrations before this test."
            )
        raise


def _row(**values):
    row = MagicMock()
    for key, value in values.items():
        setattr(row, key, value)
    return row


def _geography(locality_id: str = "01") -> GeographyResolveResponse:
    candidate = GeographyCandidate(
        zip5="94110",
        locality_id=locality_id,
        locality_name="Test Locality",
        cbsa=None,
        county_fips=None,
        state_code="CA",
    )
    return GeographyResolveResponse(
        zip5="94110",
        selected_candidate=candidate,
        resolution_method="exact_match",
        candidates=[candidate],
        warnings=[],
    )


def _engine_with_rows(*, pe_nf_rvu=2.0, pe_fac_rvu=3.0) -> MPSFEngine:
    db = MagicMock()
    engine = MPSFEngine(db=db)

    mpfs_row = _row(
        work_rvu=1.0,
        pe_nf_rvu=pe_nf_rvu,
        pe_fac_rvu=pe_fac_rvu,
        mp_rvu=0.5,
        release_id="mpfs_test_release",
        batch_id="mpfs_test_batch",
    )
    gpci_row = _row(
        gpci_work=1.0,
        gpci_pe=1.0,
        gpci_mp=1.0,
        release_id="gpci_test_release",
        batch_id="gpci_test_batch",
    )
    cf_row = _row(
        cf=10.0,
        release_id="cf_test_release",
        batch_id="cf_test_batch",
    )

    query_results = [mpfs_row, gpci_row, cf_row]

    def query_side_effect(*columns):
        chain = MagicMock()
        filtered = MagicMock()
        filtered.first.return_value = query_results.pop(0)

        chain.filter.return_value = filtered
        return chain

    db.query.side_effect = query_side_effect
    return engine


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "modifiers,professional_component,facility_component,expected_allowed,expected_professional,expected_technical",
    [
        ([], True, True, 3500, 1500, 2000),
        (["-26"], True, True, 1500, 1500, 0),
        (["-TC"], True, True, 2000, 0, 2000),
        ([], True, False, 1500, 1500, 0),
        ([], False, True, 2000, 0, 2000),
        (["50"], True, True, 5250, 2250, 3000),
    ],
)
async def test_mpfs_prices_professional_technical_and_global_components(
    modifiers,
    professional_component,
    facility_component,
    expected_allowed,
    expected_professional,
    expected_technical,
):
    engine = _engine_with_rows()

    result = await engine.price_code(
        code="99213",
        zip="94110",
        year=2025,
        geography=_geography(),
        pos="11",
        modifiers=modifiers,
        professional_component=professional_component,
        facility_component=facility_component,
    )

    assert result.allowed_cents == expected_allowed
    assert result.professional_allowed_cents == expected_professional
    assert result.facility_allowed_cents == expected_technical
    assert result.beneficiary_coinsurance_cents == round(expected_allowed * 0.20)
    assert (
        result.program_payment_cents
        == expected_allowed - result.beneficiary_total_cents
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mpfs_global_defaults_to_facility_pe_when_pos_missing():
    engine = _engine_with_rows()

    result = await engine.price_code(
        code="99213",
        zip="94110",
        year=2025,
        geography=_geography(),
    )

    assert result.allowed_cents == 4500
    assert result.professional_allowed_cents == 1500
    assert result.facility_allowed_cents == 3000


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mpfs_rejects_conflicting_professional_and_technical_modifiers():
    engine = _engine_with_rows()

    with pytest.raises(ValueError, match="26 and TC"):
        await engine.price_code(
            code="99213",
            zip="94110",
            year=2025,
            geography=_geography(),
            modifiers=["-26", "-TC"],
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mpfs_rejects_empty_component_selection_without_modifier_override():
    engine = _engine_with_rows()

    with pytest.raises(ValueError, match="At least one MPFS component"):
        await engine.price_code(
            code="99213",
            zip="94110",
            year=2025,
            geography=_geography(),
            professional_component=False,
            facility_component=False,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mpfs_component_modifier_overrides_component_flags():
    engine = _engine_with_rows()

    result = await engine.price_code(
        code="99213",
        zip="94110",
        year=2025,
        geography=_geography(),
        pos="11",
        modifiers=["-TC"],
        professional_component=True,
        facility_component=False,
    )

    assert result.allowed_cents == 2000
    assert result.professional_allowed_cents == 0
    assert result.facility_allowed_cents == 2000


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mpfs_uses_rvu_snapshots_by_valuation_date(test_db_session):
    for table_name in ("dataset_snapshots", "releases", "rvu_items", "gpci_indices"):
        _require_table(test_db_session, table_name)

    release_b_id = uuid.uuid4()
    release_c_id = uuid.uuid4()
    snapshot_release_ids = ["rvu_2029_B", "gpci_2029_B", "rvu_2029_C", "gpci_2029_C"]
    release_source_versions = ["rvu_2029_B", "rvu_2029_C"]
    existing_release_ids = [
        row[0]
        for row in test_db_session.query(Release.id)
        .filter(Release.source_version.in_(release_source_versions))
        .all()
    ]
    release_ids_to_delete = existing_release_ids + [release_b_id, release_c_id]

    test_db_session.query(DatasetSnapshot).filter(
        DatasetSnapshot.release_id.in_(snapshot_release_ids)
    ).delete(synchronize_session=False)
    test_db_session.query(RVUItem).filter(
        RVUItem.release_id.in_(release_ids_to_delete)
    ).delete(synchronize_session=False)
    test_db_session.query(GPCIIndex).filter(
        GPCIIndex.release_id.in_(release_ids_to_delete)
    ).delete(synchronize_session=False)
    test_db_session.query(Release).filter(Release.id.in_(release_ids_to_delete)).delete(
        synchronize_session=False
    )
    test_db_session.commit()

    releases = [
        Release(
            id=release_b_id,
            type="RVU_FULL",
            source_version="rvu_2029_B",
            imported_at=date(2029, 4, 1),
            notes="batch-b",
        ),
        Release(
            id=release_c_id,
            type="RVU_FULL",
            source_version="rvu_2029_C",
            imported_at=date(2029, 7, 1),
            notes="batch-c",
        ),
    ]
    snapshots = [
        DatasetSnapshot(
            dataset_id="rvu_items",
            release_id="rvu_2029_B",
            digest="sha256:rvu-b",
            effective_from=date(2029, 4, 1),
        ),
        DatasetSnapshot(
            dataset_id="gpci_indices",
            release_id="gpci_2029_B",
            digest="sha256:gpci-b",
            effective_from=date(2029, 4, 1),
        ),
        DatasetSnapshot(
            dataset_id="rvu_items",
            release_id="rvu_2029_C",
            digest="sha256:rvu-c",
            effective_from=date(2029, 7, 1),
        ),
        DatasetSnapshot(
            dataset_id="gpci_indices",
            release_id="gpci_2029_C",
            digest="sha256:gpci-c",
            effective_from=date(2029, 7, 1),
        ),
    ]
    rvu_rows = [
        RVUItem(
            id=uuid.uuid4(),
            release_id=release_b_id,
            hcpcs_code="99213",
            modifier_key="",
            work_rvu=Decimal("1.0000"),
            pe_rvu_nonfac=Decimal("2.0000"),
            pe_rvu_fac=Decimal("3.0000"),
            mp_rvu=Decimal("0.5000"),
            conversion_factor=Decimal("10.0000"),
            effective_start=date(2029, 4, 1),
        ),
        RVUItem(
            id=uuid.uuid4(),
            release_id=release_c_id,
            hcpcs_code="99213",
            modifier_key="",
            work_rvu=Decimal("2.0000"),
            pe_rvu_nonfac=Decimal("4.0000"),
            pe_rvu_fac=Decimal("6.0000"),
            mp_rvu=Decimal("1.0000"),
            conversion_factor=Decimal("10.0000"),
            effective_start=date(2029, 7, 1),
        ),
    ]
    gpci_rows = [
        GPCIIndex(
            id=uuid.uuid4(),
            release_id=release_b_id,
            mac="TSTMAC",
            state="CA",
            locality_id="05",
            locality_name="Test California",
            work_gpci=Decimal("1.0000"),
            pe_gpci=Decimal("1.0000"),
            mp_gpci=Decimal("1.0000"),
            effective_start=date(2029, 4, 1),
        ),
        GPCIIndex(
            id=uuid.uuid4(),
            release_id=release_c_id,
            mac="TSTMAC",
            state="CA",
            locality_id="05",
            locality_name="Test California",
            work_gpci=Decimal("1.0000"),
            pe_gpci=Decimal("1.0000"),
            mp_gpci=Decimal("1.0000"),
            effective_start=date(2029, 7, 1),
        ),
    ]

    test_db_session.add_all(releases + snapshots + rvu_rows + gpci_rows)
    test_db_session.commit()

    engine = MPSFEngine(db=test_db_session)

    before_effective = await engine.price_code(
        code="99213",
        zip="94110",
        year=2029,
        geography=_geography("05"),
        pos="11",
        valuation_date=date(2029, 6, 30),
    )
    after_effective = await engine.price_code(
        code="99213",
        zip="94110",
        year=2029,
        geography=_geography("05"),
        pos="11",
        valuation_date=date(2029, 7, 1),
    )

    assert before_effective.release_id == "rvu_2029_B"
    assert before_effective.allowed_cents == 3500
    assert "RVU:release:rvu_2029_B" in before_effective.trace_refs
    assert "GPCI:release:gpci_2029_B" in before_effective.trace_refs
    assert "CF:source:rvu_items.conversion_factor" in before_effective.trace_refs

    assert after_effective.release_id == "rvu_2029_C"
    assert after_effective.allowed_cents == 7000
    assert "RVU:release:rvu_2029_C" in after_effective.trace_refs
    assert "GPCI:release:gpci_2029_C" in after_effective.trace_refs
