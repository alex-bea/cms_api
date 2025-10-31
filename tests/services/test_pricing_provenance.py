"""Tests for provenance tracking in pricing service (Phase 2.5/2.6)

Validates that:
1. Engines return provenance fields (release_id, batch_id, dataset_id)
2. Provenance flows through to trace_refs in standardized format
3. _collect_datasets_used() extracts and includes provenance in datasets_used
4. Engines return unified CodePricingItem (Quick Win #2)
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from cms_pricing.services.pricing import PricingService
from cms_pricing.schemas.pricing import LineItemResponse, CodePricingItem
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


@pytest.mark.unit
class TestEngineReturnsCodePricingItem:
    """Test that engines return CodePricingItem (Quick Win #2)"""

    @pytest.mark.asyncio
    async def test_mpfs_engine_returns_code_pricing_item(self):
        """Test MPFS engine actually returns CodePricingItem (not just signature check)"""
        from cms_pricing.engines.mpfs import MPSFEngine
        from cms_pricing.schemas.geography import GeographyResolveResponse, GeographyCandidate
        from cms_pricing.models.fee_schedules import GPCI, ConversionFactor
        
        engine = MPSFEngine()
        
        # Mock geography with locality
        mock_candidate = GeographyCandidate(
            zip5="10001",
            locality_id="01",
            locality_name="New York",
            cbsa=None,
            county_fips=None,
            state_code="NY"
        )
        mock_geography = GeographyResolveResponse(
            zip5="10001",
            selected_candidate=mock_candidate,
            resolution_method="exact_match",
            candidates=[mock_candidate],
            warnings=[]
        )
        
        # Mock database data
        # Engines now use with_entities() which returns Row objects with attribute access
        from unittest.mock import MagicMock
        
        # Create Row-like mock objects that support attribute access
        mock_mpfs_row = MagicMock()
        mock_mpfs_row.work_rvu = 0.93
        mock_mpfs_row.pe_nf_rvu = 0.94  # Note: engine uses pe_nf_rvu, not pe_nonfac_rvu
        mock_mpfs_row.pe_fac_rvu = 0.94
        mock_mpfs_row.mp_rvu = 0.10
        mock_mpfs_row.release_id = "mpfs_2025_annual_20250115"
        mock_mpfs_row.batch_id = "batch_123"
        
        mock_gpci_row = MagicMock()
        mock_gpci_row.gpci_work = 1.0
        mock_gpci_row.gpci_pe = 1.0
        mock_gpci_row.gpci_mp = 1.0
        mock_gpci_row.release_id = "gpci_2025_annual"
        mock_gpci_row.batch_id = None
        
        mock_cf_row = MagicMock()
        mock_cf_row.cf = 32.3465
        mock_cf_row.release_id = "cf_2025_annual"
        mock_cf_row.batch_id = None
        
        # Setup query chain to handle with_entities() calls
        # query() is called with column expressions, then .filter(), then .first()
        def create_query_chain(*args):
            # args contains column expressions like FeeMPFS.work_rvu, FeeMPFS.release_id, etc.
            chain = MagicMock()
            
            def filter_chain(*filter_args, **filter_kwargs):
                filter_mock = MagicMock()
                
                def first_result():
                    # Determine which row to return based on columns in args
                    columns = [str(arg) for arg in args if hasattr(arg, 'key')]
                    
                    # Check if this is MPFS query (has work_rvu)
                    if any('work_rvu' in str(c) for c in args):
                        return mock_mpfs_row
                    # Check if this is GPCI query (has gpci_work)
                    elif any('gpci_work' in str(c) for c in args):
                        return mock_gpci_row
                    # Check if this is ConversionFactor query (has cf)
                    elif any('cf' in str(c) for c in args):
                        return mock_cf_row
                    return None
                
                filter_mock.first = MagicMock(return_value=first_result())
                return filter_mock
            
            chain.filter = MagicMock(return_value=MagicMock(
                first=MagicMock(return_value=first_result())
            ))
            # Also support direct .first() call
            chain.first = MagicMock(return_value=first_result())
            return chain
        
        with patch.object(engine.db, 'query', side_effect=create_query_chain):
            # Actually await the engine call to verify it returns CodePricingItem
            result = await engine.price_code(
                code="99213",
                zip="10001",
                year=2025,
                quarter="1",
                geography=mock_geography
            )
            
            # Verify return type and key fields
            assert isinstance(result, CodePricingItem), "Engine should return CodePricingItem instance"
            assert result.code == "99213"
            assert result.setting == "MPFS"
            assert result.dataset_id == "MPFS"
            assert result.release_id == "mpfs_2025_annual_20250115"
            assert result.batch_id == "batch_123"
            assert result.allowed_cents > 0
            assert "MPFS:release:mpfs_2025_annual_20250115" in result.trace_refs
    
    def test_all_engines_have_code_pricing_item_return_type(self):
        """Test all engines declare CodePricingItem return type"""
        from cms_pricing.engines.mpfs import MPSFEngine
        from cms_pricing.engines.opps import OPPSEngine
        from cms_pricing.engines.asc import ASCEngine
        from cms_pricing.engines.clfs import CLFSEngine
        from cms_pricing.engines.dmepos import DMEPOSEngine
        from cms_pricing.engines.ipps import IPPSEngine
        from cms_pricing.engines.drugs import DrugEngine
        import inspect
        
        engines = [
            (MPSFEngine, "MPFS"),
            (OPPSEngine, "OPPS"),
            (ASCEngine, "ASC"),
            (CLFSEngine, "CLFS"),
            (DMEPOSEngine, "DMEPOS"),
            (IPPSEngine, "IPPS"),
            (DrugEngine, "DRUGS")
        ]
        
        for engine_class, name in engines:
            sig = inspect.signature(engine_class().price_code)
            return_type = sig.return_annotation
            
            # Check if it's CodePricingItem (could be string or actual type)
            assert return_type == CodePricingItem or return_type.__name__ == "CodePricingItem", \
                f"{name} engine should return CodePricingItem, got {return_type}"
    
    @pytest.mark.asyncio
    async def test_opps_engine_returns_code_pricing_item(self):
        """Integration-style test: OPPS engine returns CodePricingItem with proper structure"""
        from cms_pricing.engines.opps import OPPSEngine
        from cms_pricing.schemas.geography import GeographyResolveResponse, GeographyCandidate
        from cms_pricing.models.fee_schedules import FeeOPPS, WageIndex
        
        engine = OPPSEngine()
        
        # Mock geography with CBSA
        mock_candidate = GeographyCandidate(
            zip5="10001",
            locality_id=None,
            locality_name=None,
            cbsa="35620",  # New York-Newark-Jersey City
            cbsa_name="New York-Newark-Jersey City",
            county_fips=None,
            state_code="NY"
        )
        mock_geography = GeographyResolveResponse(
            zip5="10001",
            selected_candidate=mock_candidate,
            resolution_method="exact_match",
            candidates=[mock_candidate],
            warnings=[]
        )
        
        # Mock database data
        # Engines now use with_entities() which returns Row objects with attribute access
        from unittest.mock import MagicMock
        
        # Create Row-like mock objects
        mock_opps_row = MagicMock()
        mock_opps_row.national_unadj_rate = 10000.0  # $100.00
        mock_opps_row.status_indicator = "S"  # Status "S" = separate payment (not packaged)
        mock_opps_row.release_id = "opps_2025_q1_20250115"
        mock_opps_row.batch_id = "opps_batch_123"
        
        mock_wage_index_row = MagicMock()
        mock_wage_index_row.wage_index = 1.5
        mock_wage_index_row.release_id = "wage_index_2025_q1"
        mock_wage_index_row.batch_id = None
        
        # Setup query chain to handle with_entities() calls
        def create_query_chain(*args):
            chain = MagicMock()
            
            def filter_chain(*filter_args, **filter_kwargs):
                filter_mock = MagicMock()
                
                def first_result():
                    # Determine which row to return based on columns in args
                    columns = [str(arg) for arg in args if hasattr(arg, 'key')]
                    
                    # Check if this is OPPS query (has national_unadj_rate)
                    if any('national_unadj_rate' in str(c) for c in args):
                        return mock_opps_row
                    # Check if this is WageIndex query (has wage_index)
                    elif any('wage_index' in str(c) for c in args):
                        return mock_wage_index_row
                    return None
                
                filter_mock.first = MagicMock(return_value=first_result())
                return filter_mock
            
            chain.filter = MagicMock(return_value=MagicMock(
                first=MagicMock(return_value=first_result())
            ))
            chain.first = MagicMock(return_value=first_result())
            return chain
        
        with patch.object(engine.db, 'query', side_effect=create_query_chain):
            # Actually await the engine call
            result = await engine.price_code(
                code="27447",
                zip="10001",
                year=2025,
                quarter="1",
                geography=mock_geography
            )
            
            # Verify return type and key fields
            assert isinstance(result, CodePricingItem), "OPPS engine should return CodePricingItem instance"
            assert result.code == "27447"
            assert result.setting == "OPPS"
            assert result.dataset_id == "OPPS"
            assert result.release_id == "opps_2025_q1_20250115"
            assert result.batch_id == "opps_batch_123"
            assert result.allowed_cents > 0
            assert result.facility_allowed_cents > 0  # OPPS is facility-only
            assert result.professional_allowed_cents == 0  # OPPS has no professional component
            assert "OPPS:release:opps_2025_q1_20250115" in result.trace_refs
            assert result.source == "benchmark"
            assert result.facility_specific == False
    
    @pytest.mark.asyncio
    async def test_drug_engine_returns_code_pricing_item(self):
        """Integration-style test: Drug engine returns CodePricingItem with proper structure"""
        from cms_pricing.engines.drugs import DrugEngine
        from cms_pricing.schemas.geography import GeographyResolveResponse, GeographyCandidate
        from cms_pricing.models.drugs import DrugASP
        
        engine = DrugEngine()
        
        # Mock geography (drugs don't require specific geography, but engine accepts it)
        mock_candidate = GeographyCandidate(
            zip5="10001",
            locality_id=None,
            locality_name=None,
            cbsa=None,
            cbsa_name=None,
            county_fips=None,
            state_code="NY"
        )
        mock_geography = GeographyResolveResponse(
            zip5="10001",
            selected_candidate=mock_candidate,
            resolution_method="exact_match",
            candidates=[mock_candidate],
            warnings=[]
        )
        
        # Mock database data
        # Drug engine doesn't use with_entities() yet, so we can mock the full model
        mock_asp = Mock(spec=DrugASP)
        mock_asp.year = 2025
        mock_asp.quarter = "1"
        mock_asp.hcpcs = "J9300"  # Common drug code
        mock_asp.asp_per_unit = 50.0  # $50.00 per unit
        mock_asp.effective_from = None
        mock_asp.effective_to = None
        
        # Setup query chain (simpler since DrugEngine doesn't use with_entities yet)
        def create_query_chain(*args):
            chain = MagicMock()
            
            def filter_chain(*filter_args, **filter_kwargs):
                filter_mock = MagicMock()
                filter_mock.first = MagicMock(return_value=mock_asp)
                return filter_mock
            
            chain.filter = MagicMock(return_value=MagicMock(
                first=MagicMock(return_value=mock_asp)
            ))
            chain.first = MagicMock(return_value=mock_asp)
            return chain
        
        with patch.object(engine.db, 'query', side_effect=create_query_chain):
            # Actually await the engine call
            result = await engine.price_code(
                code="J9300",
                zip="10001",
                year=2025,
                quarter="1",
                geography=mock_geography
            )
            
            # Verify return type and key fields
            assert isinstance(result, CodePricingItem), "Drug engine should return CodePricingItem instance"
            assert result.code == "J9300"
            assert result.setting == "DRUGS"
            assert result.dataset_id == "DRUGS"
            # Drug engine doesn't have Phase 2 provenance yet
            assert result.release_id is None, "Drug engine doesn't have Phase 2 provenance yet"
            assert result.batch_id is None, "Drug engine doesn't have Phase 2 provenance yet"
            assert result.allowed_cents > 0
            # Drugs are professional-only
            assert result.professional_allowed_cents == result.allowed_cents
            assert result.facility_allowed_cents == 0
            # Formula: ASP × 1.06 × units
            # 50.0 * 1.06 * 1.0 = 53.0 = 5300 cents
            assert result.allowed_cents == 5300
            assert "asp_2025_1_J9300" in result.trace_refs
            assert result.source == "benchmark"
            assert result.facility_specific == False
            # Drug-specific fields should be None when no NDC provided
            assert result.reference_price_cents is None
            assert result.unit_conversion is None
