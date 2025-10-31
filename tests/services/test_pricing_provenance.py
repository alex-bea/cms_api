"""Tests for provenance tracking in pricing service (Phase 2.5/2.6)

Validates that:
1. Engines return provenance fields (release_id, batch_id, dataset_id)
2. Provenance flows through to trace_refs in standardized format
3. _collect_datasets_used() extracts and includes provenance in datasets_used
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from cms_pricing.services.pricing import PricingService
from cms_pricing.schemas.pricing import LineItemResponse
from cms_pricing.models.fee_schedules import FeeMPFS


@pytest.mark.unit
class TestEngineProvenanceConstants:
    """Test that engines have correct DATASET_ID constants"""

    def test_engine_dataset_id_constants_exist(self):
        """Test all engines define DATASET_ID constant"""
        from cms_pricing.engines.mpfs import DATASET_ID as MPFS_ID
        from cms_pricing.engines.opps import DATASET_ID as OPPS_ID
        from cms_pricing.engines.asc import DATASET_ID as ASC_ID
        from cms_pricing.engines.clfs import DATASET_ID as CLFS_ID
        from cms_pricing.engines.dmepos import DATASET_ID as DMEPOS_ID
        from cms_pricing.engines.ipps import DATASET_ID as IPPS_ID
        
        assert MPFS_ID == "MPFS"
        assert OPPS_ID == "OPPS"
        assert ASC_ID == "ASC"
        assert CLFS_ID == "CLFS"
        assert DMEPOS_ID == "DMEPOS"
        assert IPPS_ID == "IPPS"


@pytest.mark.unit
class TestProvenanceExtraction:
    """Test _extract_provenance_from_line_items method"""

    def test_extract_provenance_from_trace_refs(self):
        """Test extraction of release_id/batch_id from standardized trace_refs format"""
        service = PricingService()
        
        # Create line items with provenance in trace_refs
        line_items = [
            LineItemResponse(
                sequence=1,
                code="99213",
                setting="MPFS",
                units=1.0,
                utilization_weight=1.0,
                allowed_cents=4500,
                beneficiary_deductible_cents=900,
                beneficiary_coinsurance_cents=0,
                beneficiary_total_cents=900,
                program_payment_cents=3600,
                source="benchmark",
                trace_refs=[
                    "mpfs_2025_01_99213",
                    "MPFS:release:mpfs_2025_annual_20250115",
                    "MPFS:batch:batch_abc123"
                ]
            ),
            LineItemResponse(
                sequence=2,
                code="27447",
                setting="OPPS",
                units=1.0,
                utilization_weight=1.0,
                allowed_cents=12000,
                beneficiary_deductible_cents=2400,
                beneficiary_coinsurance_cents=0,
                beneficiary_total_cents=2400,
                program_payment_cents=9600,
                source="benchmark",
                trace_refs=[
                    "opps_2025_1_27447",
                    "OPPS:release:opps_2025q1_r1",
                    "OPPS:batch:opps_2025q1_batch_001"
                ]
            )
        ]
        
        datasets_seen = {"MPFS", "OPPS"}
        release_map = service._extract_provenance_from_line_items(line_items, datasets_seen)
        
        # Verify MPFS provenance extracted
        assert "MPFS" in release_map
        assert release_map["MPFS"]["release_id"] == "mpfs_2025_annual_20250115"
        assert release_map["MPFS"]["batch_id"] == "batch_abc123"
        
        # Verify OPPS provenance extracted
        assert "OPPS" in release_map
        assert release_map["OPPS"]["release_id"] == "opps_2025q1_r1"
        assert release_map["OPPS"]["batch_id"] == "opps_2025q1_batch_001"
    
    def test_extract_provenance_handles_missing_provenance(self):
        """Test extraction handles line items without provenance gracefully"""
        service = PricingService()
        
        # Line items without provenance trace_refs
        line_items = [
            LineItemResponse(
                sequence=1,
                code="99213",
                setting="MPFS",
                units=1.0,
                utilization_weight=1.0,
                allowed_cents=4500,
                beneficiary_deductible_cents=900,
                beneficiary_coinsurance_cents=0,
                beneficiary_total_cents=900,
                program_payment_cents=3600,
                source="benchmark",
                trace_refs=["mpfs_2025_01_99213"]  # No provenance refs
            )
        ]
        
        datasets_seen = {"MPFS"}
        release_map = service._extract_provenance_from_line_items(line_items, datasets_seen)
        
        # Should return empty dict if no provenance found
        assert release_map == {}
    
    def test_extract_provenance_filters_by_dataset(self):
        """Test extraction only processes datasets in datasets_seen"""
        service = PricingService()
        
        line_items = [
            LineItemResponse(
                sequence=1,
                code="99213",
                setting="MPFS",
                units=1.0,
                utilization_weight=1.0,
                allowed_cents=4500,
                beneficiary_deductible_cents=900,
                beneficiary_coinsurance_cents=0,
                beneficiary_total_cents=900,
                program_payment_cents=3600,
                source="benchmark",
                trace_refs=["MPFS:release:test_release"]
            ),
            LineItemResponse(
                sequence=2,
                code="27447",
                setting="OPPS",
                units=1.0,
                utilization_weight=1.0,
                allowed_cents=12000,
                beneficiary_deductible_cents=2400,
                beneficiary_coinsurance_cents=0,
                beneficiary_total_cents=2400,
                program_payment_cents=9600,
                source="benchmark",
                trace_refs=["OPPS:release:test_release"]
            )
        ]
        
        # Only look for MPFS
        datasets_seen = {"MPFS"}
        release_map = service._extract_provenance_from_line_items(line_items, datasets_seen)
        
        # Should only include MPFS
        assert "MPFS" in release_map
        assert "OPPS" not in release_map


@pytest.mark.unit
class TestProvenanceTraceRefsDeduplication:
    """Test that trace_refs deduplication works correctly"""

    def test_trace_refs_deduplication_preserves_order(self):
        """Test that deduplication preserves order and removes duplicates"""
        # Simulate what happens in engines
        trace_refs = [
            "mpfs_2025_01_99213",
            "MPFS:release:test_release",
            "MPFS:batch:test_batch",
            "MPFS:release:test_release",  # Duplicate
            "gpci_2025_01"
        ]
        
        # Apply deduplication (as done in engines)
        deduplicated = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
        
        # Should preserve order, remove duplicates
        assert deduplicated == [
            "mpfs_2025_01_99213",
            "MPFS:release:test_release",
            "MPFS:batch:test_batch",
            "gpci_2025_01"
        ]
        assert len(deduplicated) == 4
        assert len(set(deduplicated)) == 4  # All unique
