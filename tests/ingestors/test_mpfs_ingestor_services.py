"""
Smoke tests for MPFS Ingestor ServiceFactory Integration

Smoke-style tests that verify basic ServiceFactory integration if/when MPFS ingestor
is migrated to use ServiceFactory. Currently, MPFS ingestor is NOT migrated per
verification report.

Following QA Testing Standard (QTS) v1.0 patterns.
"""

import pytest

from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor
from cms_pricing.ingestion.services import ServiceFactory


class TestMPFSIngestorServiceFactorySmoke:
    """Smoke tests for MPFS ingestor ServiceFactory integration"""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory"""
        output_dir = tmp_path / "mpfs_output"
        output_dir.mkdir(parents=True)
        return str(output_dir)

    @pytest.fixture
    def mpfs_ingestor(self, temp_output_dir):
        """Create MPFS ingestor instance"""
        return MPFSIngestor(output_dir=temp_output_dir)

    def test_ingestor_instantiates(self, mpfs_ingestor):
        """Smoke test: Verify ingestor instantiates successfully"""
        assert mpfs_ingestor is not None
        assert hasattr(mpfs_ingestor, 'output_dir')

    @pytest.mark.xfail(
        reason="MPFS ingestor not yet migrated to ServiceFactory. TODO: Remove xfail once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_servicefactory_should_exist(self, mpfs_ingestor):
        """TODO: Remove xfail once MPFS migration starts - see verification report"""
        assert hasattr(mpfs_ingestor, 'services'), "MPFS should use ServiceFactory"
        assert isinstance(mpfs_ingestor.services, ServiceFactory)

    @pytest.mark.xfail(
        reason="MPFS ingestor not yet migrated - manual instantiation still present. TODO: Remove xfail once migration starts. See artifacts/servicefactory_verification_report.md",
        strict=False
    )
    def test_no_manual_instantiation(self, mpfs_ingestor):
        """TODO: Remove xfail once MPFS migration starts - see verification report"""
        assert not hasattr(mpfs_ingestor, 'validation_engine'), "Should use services.validation_service"
        assert not hasattr(mpfs_ingestor, 'quarantine_manager'), "Should use services.quarantine_manager"
        assert not hasattr(mpfs_ingestor, 'observability_collector'), "Should use services.observability_collector"

