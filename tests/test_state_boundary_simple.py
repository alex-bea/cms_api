"""Simple, focused tests to verify resolver never crosses state lines"""

import pytest
import uuid
from datetime import datetime, date
from unittest.mock import Mock, patch

from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZipMetadata, IngestRun
)
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver


@pytest.fixture(scope="function")
def clean_db_session():
    """Create a clean database session for each test"""
    db = SessionLocal()
    try:
        # Clear all data before each test
        db.query(CMSZipLocality).delete()
        db.query(ZipMetadata).delete()
        db.query(ZipToZCTA).delete()
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
def border_test_data(clean_db_session, test_ingest_run):
    """Create test data with border ZIPs across different states"""
    
    # California ZIPs (near Nevada border)
    ca_zips = ["96150", "96151", "96152"]
    
    # Nevada ZIPs (near California border)  
    nv_zips = ["89448", "89449", "89450"]
    
    # Texas ZIPs (near New Mexico border)
    tx_zips = ["79821", "79822", "79823"]
    
    # New Mexico ZIPs (near Texas border)
    nm_zips = ["88001", "88002", "88003"]
    
    all_zips = ca_zips + nv_zips + tx_zips + nm_zips
    
    # Create ZCTA coordinates
    zcta_coords = []
    for i, zip_code in enumerate(all_zips):
        base_lat = 37.0 if zip_code.startswith("96") else 32.0 if zip_code.startswith("79") else 31.0
        base_lon = -120.0 if zip_code.startswith("96") else -106.0 if zip_code.startswith("79") else -81.0
        
        zcta_coords.append(ZCTACoords(
            zcta5=zip_code,
            lat=base_lat + (i * 0.01),
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
            population=50000 + (hash(zip_code) % 10000),
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
        'all_zips': all_zips
    }


class TestStateBoundaryEnforcement:
    """Test that resolver never crosses state lines"""
    
    def test_candidates_filtered_by_state(self, clean_db_session, border_test_data):
        """Test that _get_candidates only returns ZIPs from the same state"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test CA candidates - should only include CA ZIPs
        ca_candidates = resolver._get_candidates("CA", "96150")
        ca_zip_codes = [c['zip5'] for c in ca_candidates]
        
        # Should only contain CA ZIPs
        assert all(zip_code in border_test_data['ca_zips'] for zip_code in ca_zip_codes)
        # Should not contain any other state ZIPs
        assert not any(zip_code in border_test_data['nv_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_test_data['tx_zips'] for zip_code in ca_zip_codes)
        assert not any(zip_code in border_test_data['nm_zips'] for zip_code in ca_zip_codes)
        
        # Test NV candidates - should only include NV ZIPs
        nv_candidates = resolver._get_candidates("NV", "89448")
        nv_zip_codes = [c['zip5'] for c in nv_candidates]
        
        assert all(zip_code in border_test_data['nv_zips'] for zip_code in nv_zip_codes)
        assert not any(zip_code in border_test_data['ca_zips'] for zip_code in nv_zip_codes)
    
    def test_ca_zip_resolution_stays_in_ca(self, clean_db_session, border_test_data):
        """Test that CA ZIP resolution never returns NV ZIPs"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all CA ZIPs
        for ca_zip in border_test_data['ca_zips']:
            result = resolver.find_nearest_zip(ca_zip, include_trace=False)
            
            # Result should always be another CA ZIP, never NV
            assert result['nearest_zip'] in border_test_data['ca_zips']
            assert result['nearest_zip'] not in border_test_data['nv_zips']
            assert result['nearest_zip'] not in border_test_data['tx_zips']
            assert result['nearest_zip'] not in border_test_data['nm_zips']
    
    def test_nv_zip_resolution_stays_in_nv(self, clean_db_session, border_test_data):
        """Test that NV ZIP resolution never returns CA ZIPs"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all NV ZIPs
        for nv_zip in border_test_data['nv_zips']:
            result = resolver.find_nearest_zip(nv_zip, include_trace=False)
            
            # Result should always be another NV ZIP, never CA
            assert result['nearest_zip'] in border_test_data['nv_zips']
            assert result['nearest_zip'] not in border_test_data['ca_zips']
            assert result['nearest_zip'] not in border_test_data['tx_zips']
            assert result['nearest_zip'] not in border_test_data['nm_zips']
    
    def test_tx_zip_resolution_stays_in_tx(self, clean_db_session, border_test_data):
        """Test that TX ZIP resolution never returns NM ZIPs"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all TX ZIPs
        for tx_zip in border_test_data['tx_zips']:
            result = resolver.find_nearest_zip(tx_zip, include_trace=False)
            
            # Result should always be another TX ZIP, never NM
            assert result['nearest_zip'] in border_test_data['tx_zips']
            assert result['nearest_zip'] not in border_test_data['nm_zips']
            assert result['nearest_zip'] not in border_test_data['ca_zips']
            assert result['nearest_zip'] not in border_test_data['nv_zips']
    
    def test_nm_zip_resolution_stays_in_nm(self, clean_db_session, border_test_data):
        """Test that NM ZIP resolution never returns TX ZIPs"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all NM ZIPs
        for nm_zip in border_test_data['nm_zips']:
            result = resolver.find_nearest_zip(nm_zip, include_trace=False)
            
            # Result should always be another NM ZIP, never TX
            assert result['nearest_zip'] in border_test_data['nm_zips']
            assert result['nearest_zip'] not in border_test_data['tx_zips']
            assert result['nearest_zip'] not in border_test_data['ca_zips']
            assert result['nearest_zip'] not in border_test_data['nv_zips']
    
    def test_all_zip_combinations_respect_state_boundaries(self, clean_db_session, border_test_data):
        """Test all possible ZIP combinations respect state boundaries"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test all ZIPs
        for zip_code in border_test_data['all_zips']:
            result = resolver.find_nearest_zip(zip_code, include_trace=False)
            
            # Determine which state this ZIP belongs to
            if zip_code in border_test_data['ca_zips']:
                expected_state_zips = border_test_data['ca_zips']
                forbidden_states = border_test_data['nv_zips'] + border_test_data['tx_zips'] + border_test_data['nm_zips']
            elif zip_code in border_test_data['nv_zips']:
                expected_state_zips = border_test_data['nv_zips']
                forbidden_states = border_test_data['ca_zips'] + border_test_data['tx_zips'] + border_test_data['nm_zips']
            elif zip_code in border_test_data['tx_zips']:
                expected_state_zips = border_test_data['tx_zips']
                forbidden_states = border_test_data['ca_zips'] + border_test_data['nv_zips'] + border_test_data['nm_zips']
            elif zip_code in border_test_data['nm_zips']:
                expected_state_zips = border_test_data['nm_zips']
                forbidden_states = border_test_data['ca_zips'] + border_test_data['nv_zips'] + border_test_data['tx_zips']
            else:
                pytest.fail(f"Unknown ZIP code: {zip_code}")
            
            # Result should be from the same state
            assert result['nearest_zip'] in expected_state_zips
            
            # Result should never be from other states
            assert result['nearest_zip'] not in forbidden_states
    
    def test_state_boundary_with_pobox_exclusion(self, clean_db_session, border_test_data, test_ingest_run):
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
        result = resolver.find_nearest_zip("96150", include_trace=False)
        
        # Result should be another CA ZIP, not the PO Box, and not from other states
        assert result['nearest_zip'] in border_test_data['ca_zips']
        assert result['nearest_zip'] != "96199"  # PO Box should be excluded
        assert result['nearest_zip'] not in border_test_data['nv_zips']
        assert result['nearest_zip'] not in border_test_data['tx_zips']
        assert result['nearest_zip'] not in border_test_data['nm_zips']
    
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


class TestStateBoundaryEdgeCases:
    """Test edge cases for state boundary enforcement"""
    
    def test_state_boundary_with_zip9_override(self, clean_db_session, border_test_data, test_ingest_run):
        """Test that ZIP9 overrides don't allow state boundary crossing"""
        from cms_pricing.models.nearest_zip import ZIP9Overrides
        
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
        result = resolver.find_nearest_zip("96150-1234", include_trace=False)
        
        # Should still be CA state
        assert result['nearest_zip'] in border_test_data['ca_zips']
        assert result['nearest_zip'] not in border_test_data['nv_zips']
        assert result['nearest_zip'] not in border_test_data['tx_zips']
        assert result['nearest_zip'] not in border_test_data['nm_zips']
    
    def test_state_boundary_with_nber_fallback(self, clean_db_session, border_test_data, test_ingest_run):
        """Test that NBER fallback doesn't cause state boundary crossing"""
        from cms_pricing.models.nearest_zip import NBERCentroids
        
        # Add NBER centroid for CA ZIP (no Gazetteer data)
        nber_centroid = NBERCentroids(
            zcta5="96150",
            lat=37.0,
            lon=-120.0,
            vintage="2024",
            ingest_run_id=test_ingest_run.run_id
        )
        clean_db_session.add(nber_centroid)
        
        # Remove Gazetteer data for this ZIP to force NBER fallback
        clean_db_session.query(ZCTACoords).filter(ZCTACoords.zcta5 == "96150").delete()
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test resolution - should use NBER fallback but stay in CA
        result = resolver.find_nearest_zip("96150", include_trace=False)
        
        # Result should still be CA ZIP
        assert result['nearest_zip'] in border_test_data['ca_zips']
        assert result['nearest_zip'] not in border_test_data['nv_zips']
        assert result['nearest_zip'] not in border_test_data['tx_zips']
        assert result['nearest_zip'] not in border_test_data['nm_zips']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
