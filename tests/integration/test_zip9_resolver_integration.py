"""
Integration tests for ZIP9 resolver integration following QA Testing Standard (QTS)

Test ID: QA-ZIP9-INT-0001
Owner: Data Engineering
Tier: integration
Environments: dev, ci, staging
Dependencies: cms_pricing.services.nearest_zip_resolver, cms_pricing.ingestion.ingestors.cms_zip9_ingester
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from cms_pricing.database import SessionLocal
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from cms_pricing.ingestion.ingestors.cms_zip9_ingester import CMSZip9Ingester
from cms_pricing.models.nearest_zip import ZIP9Overrides, CMSZipLocality


class TestZIP9ResolverIntegration:
    """Integration tests for ZIP9 resolver functionality"""
    
    @pytest.fixture
    def db_session(self):
        """Create database session for testing"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @pytest.fixture
    def zip9_ingester(self):
        """Create ZIP9 ingester for testing"""
        return CMSZip9Ingester(output_dir="./test_data/zip9")
    
    @pytest.fixture
    def resolver(self, db_session):
        """Create nearest ZIP resolver for testing"""
        return NearestZipResolver(db_session)
    
    @pytest.fixture
    def sample_zip9_data(self):
        """Sample ZIP9 data for integration testing"""
        return [
            {
                'zip9_low': '902100000',
                'zip9_high': '902109999',
                'state': 'CA',
                'locality': '01',
                'rural_flag': True,
                'effective_from': datetime(2025, 8, 14).date(),
                'effective_to': None,
                'vintage': '2025-08-14'
            },
            {
                'zip9_low': '100010000', 
                'zip9_high': '100019999',
                'state': 'NY',
                'locality': '02',
                'rural_flag': False,
                'effective_from': datetime(2025, 8, 14).date(),
                'effective_to': None,
                'vintage': '2025-08-14'
            }
        ]
    
    def test_zip9_data_ingestion(self, zip9_ingester, sample_zip9_data):
        """Test QA-ZIP9-INT-0001: ZIP9 data ingestion works correctly"""
        # Test ingestion process
        result = asyncio.run(zip9_ingester.ingest("test_zip9_2025"))
        
        assert result['status'] == 'success'
        assert result['record_count'] > 0
        assert 'quality_score' in result
        assert result['quality_score'] >= 0.9
    
    def test_zip9_resolver_lookup(self, resolver, db_session, sample_zip9_data):
        """Test QA-ZIP9-INT-0002: ZIP9 resolver lookup works correctly"""
        # Insert test data
        for data in sample_zip9_data:
            zip9_override = ZIP9Overrides(**data)
            db_session.add(zip9_override)
        db_session.commit()
        
        # Test ZIP9 lookup
        test_zip9 = '902101234'
        result = resolver.find_nearest_zip(test_zip9, include_trace=True)
        
        assert result is not None
        assert result.nearest_zip is not None
        assert result.distance_miles is not None
        assert result.trace is not None
        
        # Verify ZIP9-specific trace information
        assert 'zip9_override_used' in result.trace
        assert result.trace['zip9_override_used'] == True
    
    def test_zip9_zip5_fallback(self, resolver, db_session, sample_zip9_data):
        """Test QA-ZIP9-INT-0003: ZIP9 falls back to ZIP5 when no override exists"""
        # Insert only ZIP5 data (no ZIP9 overrides)
        zip5_data = {
            'zip5': '90210',
            'state': 'CA', 
            'locality': '01',
            'rural_flag': True,
            'effective_from': datetime(2025, 8, 14).date(),
            'effective_to': None,
            'vintage': '2025-08-14'
        }
        cms_zip = CMSZipLocality(**zip5_data)
        db_session.add(cms_zip)
        db_session.commit()
        
        # Test ZIP9 lookup (should fall back to ZIP5)
        test_zip9 = '902101234'
        result = resolver.find_nearest_zip(test_zip9, include_trace=True)
        
        assert result is not None
        assert result.nearest_zip is not None
        assert result.trace['zip9_override_used'] == False
        assert result.trace['zip5_fallback_used'] == True
    
    def test_zip9_state_boundary_enforcement(self, resolver, db_session, sample_zip9_data):
        """Test QA-ZIP9-INT-0004: ZIP9 respects state boundaries"""
        # Insert ZIP9 data for CA only
        for data in sample_zip9_data:
            if data['state'] == 'CA':
                zip9_override = ZIP9Overrides(**data)
                db_session.add(zip9_override)
        db_session.commit()
        
        # Test ZIP9 lookup in CA (should work)
        ca_zip9 = '902101234'
        result = resolver.find_nearest_zip(ca_zip9, include_trace=True)
        assert result is not None
        assert result.trace['zip9_override_used'] == True
        
        # Test ZIP9 lookup in NY (should not find ZIP9 override)
        ny_zip9 = '100011234'
        result = resolver.find_nearest_zip(ny_zip9, include_trace=True)
        # Should either find ZIP5 fallback or return no results
        if result:
            assert result.trace['zip9_override_used'] == False
    
    def test_zip9_range_matching(self, resolver, db_session, sample_zip9_data):
        """Test QA-ZIP9-INT-0005: ZIP9 range matching works correctly"""
        # Insert ZIP9 data
        for data in sample_zip9_data:
            zip9_override = ZIP9Overrides(**data)
            db_session.add(zip9_override)
        db_session.commit()
        
        # Test ZIP9 within range
        test_zip9 = '902101234'  # Should match 902100000-902109999
        result = resolver.find_nearest_zip(test_zip9, include_trace=True)
        assert result is not None
        assert result.trace['zip9_override_used'] == True
        assert result.trace['zip9_range_matched'] == '902100000-902109999'
        
        # Test ZIP9 outside range
        test_zip9 = '902200000'  # Should not match any range
        result = resolver.find_nearest_zip(test_zip9, include_trace=True)
        # Should fall back to ZIP5 or return no results
        if result:
            assert result.trace['zip9_override_used'] == False
    
    def test_zip9_api_endpoint_integration(self, resolver, db_session, sample_zip9_data):
        """Test QA-ZIP9-INT-0006: ZIP9 API endpoint integration"""
        # Insert test data
        for data in sample_zip9_data:
            zip9_override = ZIP9Overrides(**data)
            db_session.add(zip9_override)
        db_session.commit()
        
        # Test API endpoint with ZIP9
        test_zip9 = '902101234'
        result = resolver.find_nearest_zip(test_zip9, include_trace=True)
        
        # Verify API response format
        assert result is not None
        assert hasattr(result, 'nearest_zip')
        assert hasattr(result, 'distance_miles')
        assert hasattr(result, 'trace')
        
        # Verify trace contains ZIP9-specific information
        trace = result.trace
        assert 'zip9_override_used' in trace
        assert 'zip9_range_matched' in trace
        assert 'zip5_fallback_used' in trace


class TestZIP9DataQualityIntegration:
    """Data quality integration tests for ZIP9 functionality"""
    
    @pytest.fixture
    def db_session(self):
        """Create database session for testing"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @pytest.fixture
    def zip9_ingester(self):
        """Create ZIP9 ingester for testing"""
        return CMSZip9Ingester(output_dir="./test_data/zip9")
    
    def test_zip9_data_consistency_with_zip5(self, zip9_ingester, db_session):
        """Test QA-ZIP9-DQ-0001: ZIP9 data consistency with ZIP5 data"""
        # This test would verify that ZIP9 data is consistent with existing ZIP5 data
        # Implementation would check for conflicts and inconsistencies
        pass
    
    def test_zip9_data_completeness(self, zip9_ingester, db_session):
        """Test QA-ZIP9-DQ-0002: ZIP9 data completeness validation"""
        # This test would verify that all required ZIP9 data is present
        # Implementation would check for missing fields and incomplete records
        pass
    
    def test_zip9_data_accuracy(self, zip9_ingester, db_session):
        """Test QA-ZIP9-DQ-0003: ZIP9 data accuracy validation"""
        # This test would verify that ZIP9 data is accurate
        # Implementation would check for data accuracy and validation
        pass


class TestZIP9PerformanceIntegration:
    """Performance integration tests for ZIP9 functionality"""
    
    @pytest.fixture
    def db_session(self):
        """Create database session for testing"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @pytest.fixture
    def resolver(self, db_session):
        """Create nearest ZIP resolver for testing"""
        return NearestZipResolver(db_session)
    
    def test_zip9_lookup_performance(self, resolver):
        """Test QA-ZIP9-PERF-0001: ZIP9 lookup performance meets SLOs"""
        # Test that ZIP9 lookups meet performance SLOs
        # Implementation would measure lookup times and verify they're within bounds
        pass
    
    def test_zip9_bulk_processing_performance(self, resolver):
        """Test QA-ZIP9-PERF-0002: ZIP9 bulk processing performance"""
        # Test that bulk ZIP9 processing meets performance requirements
        # Implementation would measure bulk processing times
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
