from unittest.mock import MagicMock

import pytest

from cms_pricing.engines.mpfs import MPSFEngine
from cms_pricing.schemas.geography import GeographyCandidate, GeographyResolveResponse


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
    assert result.program_payment_cents == expected_allowed - result.beneficiary_total_cents


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
