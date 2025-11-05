from datetime import date
from typing import Dict, Any

import pytest

from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.schemas.geography import GeographyResolveResponse, GeographyCandidate
from cms_pricing.schemas.pricing import CodePricingItem


def test_mpfs_pricing_datasets_used_includes_supporting_datasets(
    client,
    test_db_session,
    monkeypatch,
):
    """Contract test: /pricing/price should list MPFS supporting datasets in datasets_used."""

    # Seed dataset snapshots for MPFS supporting datasets
    target_ids = {
        "MPFS",
        "mpfs_payment_curated",
        "mpfs_rvu",
        "mpfs_gpci",
        "mpfs_cf_vintage",
    }
    test_db_session.query(DatasetSnapshot).filter(
        DatasetSnapshot.dataset_id.in_(target_ids)
    ).delete(synchronize_session=False)
    test_db_session.commit()

    snapshots = [
        DatasetSnapshot(
            dataset_id="MPFS",
            release_id="mpfs_2025_annual_20250115",
            digest="mpfs-digest",
            effective_from=date(2025, 1, 1),
            effective_to=None,
            manifest_url="s3://mpfs/manifest.json",
        ),
        DatasetSnapshot(
            dataset_id="mpfs_payment_curated",
            release_id="mpfs_2025_annual_20250115",
            digest="mpfs-payment-digest",
            effective_from=date(2025, 1, 1),
            effective_to=None,
            manifest_url="s3://mpfs/payment/manifest.json",
        ),
        DatasetSnapshot(
            dataset_id="mpfs_rvu",
            release_id="mpfs_rvu_2025D",
            digest="mpfs-rvu-digest",
            effective_from=date(2025, 1, 1),
            effective_to=None,
            manifest_url="s3://mpfs/rvu/manifest.json",
        ),
        DatasetSnapshot(
            dataset_id="mpfs_gpci",
            release_id="gpci_2025_annual",
            digest="mpfs-gpci-digest",
            effective_from=date(2025, 1, 1),
            effective_to=None,
            manifest_url="s3://mpfs/gpci/manifest.json",
        ),
        DatasetSnapshot(
            dataset_id="mpfs_cf_vintage",
            release_id="cf_2025_annual",
            digest="mpfs-cf-digest",
            effective_from=date(2025, 1, 1),
            effective_to=None,
            manifest_url="s3://mpfs/cf/manifest.json",
        ),
    ]
    test_db_session.add_all(snapshots)
    test_db_session.commit()

    # Stub geography resolution to avoid relying on seeded geography data
    mock_candidate = GeographyCandidate(
        zip5="10001",
        locality_id="01",
        locality_name="Test Locality",
        cbsa="35620",
        cbsa_name="New York-Newark-Jersey City",
        county_fips=None,
        state_code="NY",
    )
    mock_geography = GeographyResolveResponse(
        zip5="10001",
        candidates=[mock_candidate],
        requires_resolution=False,
        resolution_method="exact_match",
        selected_candidate=mock_candidate,
        warnings=[],
    )

    async def mock_resolve_zip(self, zip5: str, *args: Any, **kwargs: Any) -> GeographyResolveResponse:
        return mock_geography

    monkeypatch.setattr(
        "cms_pricing.services.geography.GeographyService.resolve_zip",
        mock_resolve_zip,
    )

    async def mock_price_code(self, **kwargs: Any) -> CodePricingItem:
        return CodePricingItem(
            code=kwargs["code"],
            setting="MPFS",
            modifier=None,
            allowed_cents=10000,
            beneficiary_deductible_cents=0,
            beneficiary_coinsurance_cents=0,
            beneficiary_total_cents=0,
            program_payment_cents=10000,
            professional_allowed_cents=10000,
            facility_allowed_cents=0,
            dataset_id="MPFS",
            release_id="mpfs_2025_annual_20250115",
            batch_id="batch_123",
            trace_refs=[
                "mpfs_2025_01_99213",
                "MPFS:release:mpfs_2025_annual_20250115",
                "MPFS:batch:batch_123",
                "GPCI:release:gpci_2025_annual",
                "CF:release:cf_2025_annual",
            ],
            source="benchmark",
            facility_specific=False,
            packaged=False,
            reference_price_cents=None,
            unit_conversion=None,
            units=1.0,
        )

    monkeypatch.setattr(
        "cms_pricing.engines.mpfs.MPSFEngine.price_code",
        mock_price_code,
    )

    pricing_request: Dict[str, Any] = {
        "zip": "10001",
        "year": 2025,
        "quarter": "1",
        "include_home_health": False,
        "include_snf": False,
        "apply_sequestration": False,
        "ad_hoc_plan": {
            "name": "MPFS single component",
            "components": [
                {
                    "sequence": 1,
                    "code": "99213",
                    "setting": "MPFS",
                    "units": 1,
                    "utilization_weight": 1.0,
                    "professional_component": True,
                    "facility_component": False,
                    "modifiers": [],
                }
            ],
        },
    }

    response = client.post("/pricing/price", json=pricing_request)
    assert response.status_code == 200, response.text

    data = response.json()
    datasets_used = data.get("datasets_used", [])
    dataset_ids = {entry["dataset_id"] for entry in datasets_used}

    expected_ids = target_ids
    assert expected_ids.issubset(dataset_ids)

    # Cleanup seeded snapshots
    test_db_session.query(DatasetSnapshot).filter(
        DatasetSnapshot.dataset_id.in_(target_ids)
    ).delete(synchronize_session=False)
    test_db_session.commit()
