"""Focused coverage for local boot and smoke-pricing stabilization."""

import pytest

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.services.pricing import PricingService
from scripts.bootstrap_local_db import assert_local_database_url


class _TestPricingEngine(BasePricingEngine):
    async def price_code(self, *args, **kwargs):
        raise NotImplementedError


def test_geography_dict_is_normalized_for_pricing_engines():
    service = PricingService()

    geography = service._normalize_geography_result(
        "94110",
        {
            "locality_id": "01",
            "state": "CA",
            "rural_flag": None,
            "match_level": "default",
            "dataset_digest": "benchmark",
        },
    )

    assert geography.zip5 == "94110"
    assert geography.selected_candidate is not None
    assert geography.selected_candidate.locality_id == "01"
    assert geography.selected_candidate.state_code == "CA"
    assert geography.selected_candidate.used is True
    assert geography.resolution_method == "default"
    assert len(geography.candidates) == 1

    response = service._build_geography_response(geography)
    assert response.locality_id == "01"
    assert response.resolution_method == "default"


def test_cost_sharing_defaults_to_no_deductible_remaining():
    engine = _TestPricingEngine()

    result = engine._calculate_beneficiary_cost_sharing(100.0)

    assert result["beneficiary_deductible"] == 0.0
    assert result["beneficiary_coinsurance"] == 20.0
    assert result["beneficiary_total"] == 20.0
    assert result["program_payment"] == 80.0


def test_bootstrap_refuses_remote_database_by_default():
    with pytest.raises(SystemExit) as exc_info:
        assert_local_database_url("postgresql://user:pass@example.com:5432/cms_pricing")

    assert "Refusing to bootstrap non-local database" in str(exc_info.value)


def test_bootstrap_allows_compose_database_host():
    assert_local_database_url("postgresql://cms_user:cms_password@db:5432/cms_pricing")


def test_worker_imports_current_mpfs_ingestor():
    from cms_pricing import worker

    assert worker.MPFSIngestor.__name__ == "MPFSIngestor"
