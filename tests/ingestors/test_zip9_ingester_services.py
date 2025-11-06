"""
Smoke tests for ZIP9 Ingester ServiceFactory Integration

Smoke-style tests that verify basic ServiceFactory integration if/when ZIP9 ingester
is migrated to use ServiceFactory. Currently, ZIP9 ingester is NOT migrated per
verification report. ZIP9 may have dataset-specific requirements (e.g., disable
reference data per Phase 3 plan).

Following QA Testing Standard (QTS) v1.0 patterns.
"""

import pytest

from cms_pricing.ingestion.ingestors.cms_zip9_ingester import CMSZip9Ingester
from cms_pricing.ingestion.services import ServiceFactory


class TestZIP9IngesterServiceFactorySmoke:
    """Smoke tests for ZIP9 ingester ServiceFactory integration"""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory"""
        output_dir = tmp_path / "zip9_output"
        output_dir.mkdir(parents=True)
        return str(output_dir)

    @pytest.fixture
    def zip9_ingester(self, temp_output_dir):
        """Create ZIP9 ingester instance"""
        return CMSZip9Ingester(output_dir=temp_output_dir)

    def test_ingester_instantiates(self, zip9_ingester):
        """Smoke test: Verify ingester instantiates successfully"""
        assert zip9_ingester is not None
        assert hasattr(zip9_ingester, 'output_dir')

    @pytest.mark.xfail(
        reason="ZIP9 ingester not yet migrated to ServiceFactory. TODO: Remove xfail once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_servicefactory_should_exist(self, zip9_ingester):
        """TODO: Remove xfail once ZIP9 migration starts - see verification report"""
        assert hasattr(zip9_ingester, 'services'), "ZIP9 should use ServiceFactory"
        assert isinstance(zip9_ingester.services, ServiceFactory)

    @pytest.mark.xfail(
        reason="ZIP9 ingester not yet migrated - should disable reference_data per Phase 3 plan. TODO: Verify once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_zip9_should_disable_reference_data(self, zip9_ingester):
        """TODO: Verify ServiceConfig(enable_reference_data=False) once migration starts"""
        if hasattr(zip9_ingester, 'services'):
            assert zip9_ingester.services.config.enable_reference_data == False, \
                "ZIP9 should disable reference data per Phase 3 plan"

