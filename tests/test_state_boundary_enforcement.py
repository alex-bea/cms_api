"""Comprehensive tests to verify resolver never crosses state lines"""

import pytest
import uuid
from datetime import datetime, date
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZipMetadata, NearestZipTrace, NBERCentroids, ZCTADistances, IngestRun
)
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from cms_pricing.services.nearest_zip_distance import DistanceEngine
from cms_pricing.main import app


@pytest.fixture(scope="function")
def clean_db_session():
    """Create a clean database session for each test"""
    db = SessionLocal()
    try:
        # Clear all data before each test
        db.query(NearestZipTrace).delete()
        db.query(ZCTADistances).delete()
        db.query(ZIP9Overrides).delete()
        db.query(ZipMetadata).delete()
        db.query(CMSZipLocality).delete()
        db.query(ZipToZCTA).delete()
        db.query(NBERCentroids).delete()
        db.query(ZCTACoords).delete()
        db.query(IngestRun).delete()
        db.commit()
        yield db
    finally:
        db.close()


@pytest.fixture
def test_ingest_run(clean_db_session):
    """Create a test ingest run"""
    ingest_run = IngestRun(
        run_id=uuid.uuid4(),
        started_at=datetime.utcnow(),
        source_url="test_data",
        status="completed",
        row_count=0
    )
    clean_db_session.add(ingest_run)
    clean_db_session.commit()
    return ingest_run


@pytest.fixture
def border_zip_test_data(clean_db_session, test_ingest_run):
    """Create test data with border ZIPs across different states"""
    
    # California ZIPs (near Nevada border)
    ca_zips = [
        "96150",  # South Lake Tahoe, CA (near NV border)
        "96151",  # South Lake Tahoe, CA
        "96152",  # South Lake Tahoe, CA
        "96155",  # South Lake Tahoe, CA
        "96158",  # South Lake Tahoe, CA
    ]
    
    # Nevada ZIPs (near California border)
    nv_zips = [
        "89448",  # Stateline, NV (near CA border)
        "89449",  # Stateline, NV
        "89450",  # Stateline, NV
        "89451",  # Stateline, NV
        "89452",  # Stateline, NV
    ]
    
    # Texas ZIPs (near New Mexico border)
    tx_zips = [
        "79821",  # El Paso, TX (near NM border)
        "79822",  # El Paso, TX
        "79823",  # El Paso, TX
        "79824",  # El Paso, TX
        "79825",  # El Paso, TX
    ]
    
    # New Mexico ZIPs (near Texas border)
    nm_zips = [
        "88001",  # Anthony, NM (near TX border)
        "88002",  # Anthony, NM
        "88003",  # Anthony, NM
        "88004",  # Anthony, NM
        "88005",  # Anthony, NM
    ]
    
    # Florida ZIPs (near Georgia border)
    fl_zips = [
        "32003",  # Jacksonville, FL (near GA border)
        "32004",  # Jacksonville, FL
        "32005",  # Jacksonville, FL
        "32006",  # Jacksonville, FL
        "32007",  # Jacksonville, FL
    ]
    
    # Georgia ZIPs (near Florida border)
    ga_zips = [
        "31520",  # Brunswick, GA (near FL border)
        "31521",  # Brunswick, GA
        "31522",  # Brunswick, GA
        "31523",  # Brunswick, GA
        "31524",  # Brunswick, GA
    ]
    
    all_zips = ca_zips + nv_zips + tx_zips + nm_zips + fl_zips + ga_zips
    all_states = ["CA", "NV", "TX", "NM", "FL", "GA"]
    
    # Create ZCTA coordinates (simulate close geographic proximity)
    zcta_coords = []
    for i, zip_code in enumerate(all_zips):
        # Simulate coordinates - border ZIPs are geographically close
        base_lat = 37.0 if zip_code.startswith("96") else 32.0 if zip_code.startswith("79") else 31.0
        base_lon = -120.0 if zip_code.startswith("96") else -106.0 if zip_code.startswith("79") else -81.0
        
        zcta_coords.append(ZCTACoords(
            zcta5=zip_code,
            lat=base_lat + (i * 0.01),  # Small variations
            lon=base_lon + (i * 0.01),
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        ))
    
    # Create ZIP to ZCTA mappings
    zip_to_zcta = []
    for zip_code in all_zips:
        zip_to_zcta.append(ZipToZCTA(
            zip5=zip_code,
            zcta5=zip_code,
            relationship="Zip matches ZCTA",
            weight=1.0,
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        ))
    
    # Create CMS ZIP locality mappings with correct states
    cms_zip_locality = []
    state_mapping = {
        "CA": ca_zips,
        "NV": nv_zips,
        "TX": tx_zips,
        "NM": nm_zips,
        "FL": fl_zips,
        "GA": ga_zips
    }
    
    for state, zip_list in state_mapping.items():
        for zip_code in zip_list:
            cms_zip_locality.append(CMSZipLocality(
                zip5=zip_code,
                state=state,
                locality="01",
                effective_from=date(2025, 1, 1),
                vintage="2025",
                ingest_run_id=test_ingest_run.run_id
            ))
    
    # Create ZIP metadata
    zip_metadata = []
    for zip_code in all_zips:
        zip_metadata.append(ZipMetadata(
            zip5=zip_code,
            zcta_bool=True,
            population=50000 + (hash(zip_code) % 10000),  # Vary population
            is_pobox=False,
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        ))
    
    # Add all records to database
    all_records = zcta_coords + zip_to_zcta + cms_zip_locality + zip_metadata
    
    for record in all_records:
        clean_db_session.add(record)
    
    clean_db_session.commit()
    
    return {
        'ca_zips': ca_zips,
        'nv_zips': nv_zips,
        'tx_zips': tx_zips,
        'nm_zips': nm_zips,
        'fl_zips': fl_zips,
        'ga_zips': ga_zips,
        'all_zips': all_zips,
        'all_states': all_states
    }


class TestStateBoundaryEnforcement:
    """Test that resolver never crosses state lines"""
    
    def test_candidates_filtered_by_state(self, clean_db_session, border_zip_test_data):
        """Test that _get_candidates only returns ZIPs from the same state"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test CA candidates - should only include CA ZIPs
        ca_candidates = resolver._get_candidates("CA", "96150")
        ca_zip_codes = [c['zip5'] for c in ca_candidates]
        
        # Should only contain CA ZIPs
        assert all(zip_code in border_zip_test_data['ca_zips'] for zip_code in ca_zip_codes)
        # Should not contain any other state ZIPs
        assert not any(zip_code in border_zip_test_data['nv_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_zip_test_data['tx_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_zip_test_data['nm_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_zip_test_data['fl_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_zip_test_data['ga_zips'] for zip_code in ca_zip_codes)
        
        # Test NV candidates - should only include NV ZIPs
        nv_candidates = resolver._get_candidates("NV", "89448")
        nv_zip_codes = [c['zip5'] for c in nv_candidates]
        
        assert all(zip_code in border_zip_test_data['nv_zips'] for zip_code in nv_zip_codes)
        assert not any(zip_code in border_zip_test_data['ca_zips'] for zip_code in nv_zip_codes)
    
    def test_border_zip_resolution_ca_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from CA side of CA-NV border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from CA ZIP near NV border
        result = resolver.find_nearest_zip("96150", include_trace=True)
        
        # Result should be another CA ZIP, never NV
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'CA'
        assert all(c['zip5'] in border_zip_test_data['ca_zips'] for c in trace['candidates']['candidates'])
    
    def test_border_zip_resolution_nv_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from NV side of CA-NV border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from NV ZIP near CA border
        result = resolver.find_nearest_zip("89448", include_trace=True)
        
        # Result should be another NV ZIP, never CA
        assert result['nearest_zip'] in border_zip_test_data['nv_zips']
        assert result['nearest_zip'] not in border_zip_test_data['ca_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'NV'
        assert all(c['zip5'] in border_zip_test_data['nv_zips'] for c in trace['candidates']['candidates'])
    
    def test_border_zip_resolution_tx_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from TX side of TX-NM border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from TX ZIP near NM border
        result = resolver.find_nearest_zip("79821", include_trace=True)
        
        # Result should be another TX ZIP, never NM
        assert result['nearest_zip'] in border_zip_test_data['tx_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nm_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'TX'
        assert all(c['zip5'] in border_zip_test_data['tx_zips'] for c in trace['candidates']['candidates'])
    
    def test_border_zip_resolution_nm_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from NM side of TX-NM border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from NM ZIP near TX border
        result = resolver.find_nearest_zip("88001", include_trace=True)
        
        # Result should be another NM ZIP, never TX
        assert result['nearest_zip'] in border_zip_test_data['nm_zips']
        assert result['nearest_zip'] not in border_zip_test_data['tx_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'NM'
        assert all(c['zip5'] in border_zip_test_data['nm_zips'] for c in trace['candidates']['candidates'])
    
    def test_border_zip_resolution_fl_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from FL side of FL-GA border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from FL ZIP near GA border
        result = resolver.find_nearest_zip("32003", include_trace=True)
        
        # Result should be another FL ZIP, never GA
        assert result['nearest_zip'] in border_zip_test_data['fl_zips']
        assert result['nearest_zip'] not in border_zip_test_data['ga_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'FL'
        assert all(c['zip5'] in border_zip_test_data['fl_zips'] for c in trace['candidates']['candidates'])
    
    def test_border_zip_resolution_ga_side(self, clean_db_session, border_zip_test_data):
        """Test resolution from GA side of FL-GA border"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Resolve from GA ZIP near FL border
        result = resolver.find_nearest_zip("31520", include_trace=True)
        
        # Result should be another GA ZIP, never FL
        assert result['nearest_zip'] in border_zip_test_data['ga_zips']
        assert result['nearest_zip'] not in border_zip_test_data['fl_zips']
        
        # Verify in trace
        trace = result['trace']
        assert trace['normalization']['state'] == 'GA'
        assert all(c['zip5'] in border_zip_test_data['ga_zips'] for c in trace['candidates']['candidates'])
    
    def test_all_border_zip_combinations(self, clean_db_session, border_zip_test_data):
        """Test all possible border ZIP combinations"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all border ZIPs
        test_cases = [
            ("CA", border_zip_test_data['ca_zips']),
            ("NV", border_zip_test_data['nv_zips']),
            ("TX", border_zip_test_data['tx_zips']),
            ("NM", border_zip_test_data['nm_zips']),
            ("FL", border_zip_test_data['fl_zips']),
            ("GA", border_zip_test_data['ga_zips']),
        ]
        
        for state, zip_list in test_cases:
            for zip_code in zip_list:
                result = resolver.find_nearest_zip(zip_code, include_trace=True)
                
                # Result should always be from the same state
                assert result['nearest_zip'] in zip_list
                
                # Verify state consistency
                trace = result['trace']
                assert trace['normalization']['state'] == state
                
                # Verify all candidates are from same state
                candidates = trace['candidates']['candidates']
                assert all(c['zip5'] in zip_list for c in candidates)
    
    def test_zip9_override_preserves_state_boundary(self, clean_db_session, border_zip_test_data, test_ingest_run):
        """Test that ZIP9 overrides don't allow state boundary crossing"""
        # Add ZIP9 override for CA ZIP
        zip9_override = ZIP9Overrides(
            zip9_low="961500000",
            zip9_high="961509999",
            state="CA",  # Must stay in CA
            locality="02",
            rural_flag=False,
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        )
        clean_db_session.add(zip9_override)
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test ZIP9 input
        result = resolver.find_nearest_zip("96150-1234", include_trace=True)
        
        # Should still be CA state
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify ZIP9 override was used but state preserved
        trace = result['trace']
        assert trace['normalization']['state'] == 'CA'
        assert trace['normalization']['zip9_hit'] is True
        assert trace['normalization']['locality'] == '02'  # From ZIP9 override
    
    def test_no_candidates_error_when_isolated_zip(self, clean_db_session, test_ingest_run):
        """Test NO_CANDIDATES_IN_STATE error when ZIP is isolated in its state"""
        # Create a ZIP with no other ZIPs in the same state
        zcta_coord = ZCTACoords(zcta5="99999", lat=40.0, lon=-100.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_to_zcta = ZipToZCTA(zip5="99999", zcta5="99999", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        cms_zip = CMSZipLocality(zip5="99999", state="XX", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_metadata = ZipMetadata(zip5="99999", zcta_bool=True, population=1000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([zcta_coord, zip_to_zcta, cms_zip, zip_metadata])
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Should raise NO_CANDIDATES_IN_STATE error
        with pytest.raises(ValueError, match="NO_CANDIDATES_IN_STATE"):
            resolver.find_nearest_zip("99999")
    
    def test_state_boundary_with_pobox_exclusion(self, clean_db_session, border_zip_test_data, test_ingest_run):
        """Test that PO Box exclusion doesn't cause state boundary crossing"""
        # Add a PO Box in CA
        pobox_zip = ZipMetadata(
            zip5="96199",
            zcta_bool=True,
            population=1000,
            is_pobox=True,  # This is a PO Box
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        )
        pobox_cms = CMSZipLocality(
            zip5="96199",
            state="CA",  # Same state as other CA ZIPs
            locality="01",
            effective_from=date(2025, 1, 1),
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        )
        pobox_zcta = ZCTACoords(
            zcta5="96199",
            lat=37.0,
            lon=-120.0,
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        )
        pobox_mapping = ZipToZCTA(
            zip5="96199",
            zcta5="96199",
            relationship="Zip matches ZCTA",
            weight=1.0,
            vintage="2025",
            ingest_run_id=test_ingest_run.run_id
        )
        
        clean_db_session.add_all([pobox_zip, pobox_cms, pobox_zcta, pobox_mapping])
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test resolution from CA ZIP
        result = resolver.find_nearest_zip("96150", include_trace=True)
        
        # Result should be another CA ZIP, not the PO Box, and not from other states
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] != "96199"  # PO Box should be excluded
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify PO Box was excluded from candidates
        trace = result['trace']
        candidates = trace['candidates']['candidates']
        candidate_zips = [c['zip5'] for c in candidates]
        assert "96199" not in candidate_zips  # PO Box should be excluded
        assert all(zip_code in border_zip_test_data['ca_zips'] for zip_code in candidate_zips)


class TestStateBoundaryIntegrationTests:
    """Integration tests for state boundary enforcement"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_api_endpoint_respects_state_boundaries(self, client, clean_db_session, border_zip_test_data):
        """Test that API endpoints respect state boundaries"""
        # Test CA ZIP
        response = client.get(
            "/nearest-zip/nearest?zip=96150&include_trace=true",
            headers={"X-API-Key": "dev-key-123"}
        )
        
        if response.status_code == 200:
            data = response.json()
            assert data['nearest_zip'] in border_zip_test_data['ca_zips']
            assert data['nearest_zip'] not in border_zip_test_data['nv_zips']
            
            # Verify trace shows correct state
            trace = data['trace']
            assert trace['normalization']['state'] == 'CA'
    
    def test_api_endpoint_handles_isolated_zip(self, client, clean_db_session, test_ingest_run):
        """Test that API endpoint handles isolated ZIPs correctly"""
        # Create isolated ZIP
        zcta_coord = ZCTACoords(zcta5="99999", lat=40.0, lon=-100.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_to_zcta = ZipToZCTA(zip5="99999", zcta5="99999", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        cms_zip = CMSZipLocality(zip5="99999", state="XX", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_metadata = ZipMetadata(zip5="99999", zcta_bool=True, population=1000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([zcta_coord, zip_to_zcta, cms_zip, zip_metadata])
        clean_db_session.commit()
        
        # Test API endpoint
        response = client.get(
            "/nearest-zip/nearest?zip=99999",
            headers={"X-API-Key": "dev-key-123"}
        )
        
        # Should return 422 for NO_CANDIDATES_IN_STATE
        assert response.status_code == 422
        data = response.json()
        assert "NO_CANDIDATES_IN_STATE" in data['detail']


class TestStateBoundaryEdgeCases:
    """Test edge cases for state boundary enforcement"""
    
    def test_state_boundary_with_nber_fallback(self, clean_db_session, border_zip_test_data, test_ingest_run):
        """Test that NBER fallback doesn't cause state boundary crossing"""
        # Add NBER centroid for CA ZIP (no Gazetteer data)
        nber_centroid = NBERCentroids(
            zcta5="96150",
            lat=37.0,
            lon=-120.0,
            vintage="2024",
            ingest_run_id=test_ingest_run.run_id
        )
        clean_db_session.add(nber_centroid)
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test resolution - should use NBER fallback but stay in CA
        result = resolver.find_nearest_zip("96150", include_trace=True)
        
        # Result should still be CA ZIP
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify NBER fallback was used
        trace = result['trace']
        assert trace['starting_centroid']['source'] == 'nber_fallback'
        assert trace['normalization']['state'] == 'CA'
    
    def test_state_boundary_with_discrepancy_detection(self, clean_db_session, border_zip_test_data, test_ingest_run):
        """Test that discrepancy detection doesn't cause state boundary crossing"""
        # Add NBER distance data
        nber_distance = ZCTADistances(
            zcta5_a="96150",
            zcta5_b="96151",
            miles=0.5,  # Very different from Haversine
            vintage="2024",
            ingest_run_id=test_ingest_run.run_id
        )
        clean_db_session.add(nber_distance)
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test resolution with NBER enabled
        result = resolver.find_nearest_zip("96150", include_trace=True)
        
        # Result should still be CA ZIP despite discrepancy detection
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify discrepancy detection was performed
        trace = result['trace']
        assert 'dist_calc' in trace
        assert 'discrepancies' in trace['dist_calc']
    
    def test_state_boundary_with_asymmetry_detection(self, clean_db_session, border_zip_test_data):
        """Test that asymmetry detection doesn't cause state boundary crossing"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test resolution with asymmetry detection
        result = resolver.find_nearest_zip("96150", include_trace=True)
        
        # Result should still be CA ZIP
        assert result['nearest_zip'] in border_zip_test_data['ca_zips']
        assert result['nearest_zip'] not in border_zip_test_data['nv_zips']
        
        # Verify asymmetry detection was performed
        trace = result['trace']
        assert 'asymmetry' in trace
        assert 'asymmetry_detected' in trace['asymmetry']
        
        # If asymmetry detected, reverse lookup should also respect state boundaries
        if trace['asymmetry']['asymmetry_detected']:
            reverse_zip = trace['asymmetry']['reverse_nearest']
            # Reverse ZIP should also be from CA
            assert reverse_zip in border_zip_test_data['ca_zips']
            assert reverse_zip not in border_zip_test_data['nv_zips']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
