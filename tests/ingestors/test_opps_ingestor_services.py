"""
Smoke tests for OPPS Ingestor ServiceFactory Integration

Smoke-style tests that verify basic ServiceFactory integration if/when OPPS ingestor
is migrated to use ServiceFactory. Currently, OPPS ingestor is NOT migrated per
verification report.

Following QA Testing Standard (QTS) v1.0 patterns.
"""

import pytest
from pathlib import Path

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor
from cms_pricing.ingestion.services import ServiceFactory


class TestOPPSIngestorServiceFactorySmoke:
    """Smoke tests for OPPS ingestor ServiceFactory integration"""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory"""
        output_dir = tmp_path / "opps_output"
        output_dir.mkdir(parents=True)
        return str(output_dir)

    @pytest.fixture
    def opps_ingestor(self, temp_output_dir):
        """Create OPPS ingestor instance"""
        return OPPSIngestor(output_dir=Path(temp_output_dir))

    def test_ingestor_instantiates(self, opps_ingestor):
        """Smoke test: Verify ingestor instantiates successfully"""
        assert opps_ingestor is not None
        assert hasattr(opps_ingestor, 'output_dir')

    @pytest.mark.xfail(
        reason="OPPS ingestor not yet migrated to ServiceFactory. TODO: Remove xfail once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_servicefactory_should_exist(self, opps_ingestor):
        """TODO: Remove xfail once OPPS migration starts - see verification report"""
        assert hasattr(opps_ingestor, 'services'), "OPPS should use ServiceFactory"
        assert isinstance(opps_ingestor.services, ServiceFactory)

    @pytest.mark.xfail(
        reason="OPPS ingestor not yet migrated - manual instantiation still present. TODO: Remove xfail once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_no_manual_instantiation(self, opps_ingestor):
        """TODO: Remove xfail once OPPS migration starts - see verification report"""
        assert not hasattr(opps_ingestor, 'validation_engine'), "Should use services.validation_service"
        assert not hasattr(opps_ingestor, 'quarantine_manager'), "Should use services.quarantine_manager"
        assert not hasattr(opps_ingestor, 'observability'), "Should use services.observability_collector"

