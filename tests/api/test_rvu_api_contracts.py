"""
RVU API Contract Tests

Tests to verify API behavior and latency SLOs

QTS Compliance Header:
Test ID: QA-RVU-CONTRACT-0001
Owner: Data Engineering
Tier: contract
Environments: dev, ci, staging, production
Dependencies: cms_pricing.routers.rvu, cms_pricing.schemas.rvu
Quality Gates: merge, pre-deploy, release
SLOs: completion ≤ 10 min, pass rate ≥95%, flake rate <1%
"""

import pytest
import time
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from datetime import date, datetime
from cms_pricing.main import app
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
import uuid

client = TestClient(app)


class TestRVUAPIContracts:
    """Contract tests for RVU API endpoints"""
    
    def setup_method(self):
        """Setup test data"""
        self.test_release_id = str(uuid.uuid4())
        self.test_rvu_item_id = str(uuid.uuid4())
        self.test_gpci_id = str(uuid.uuid4())
        self.test_opps_cap_id = str(uuid.uuid4())
        self.test_anes_cf_id = str(uuid.uuid4())
        self.test_locality_county_id = str(uuid.uuid4())
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_releases_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/releases contract"""
        
        # Mock database response
        mock_release = Mock()
        mock_release.id = self.test_release_id
        mock_release.type = "RVU_FULL"
        mock_release.source_version = "2025D"
        mock_release.source_url = "https://example.com"
        mock_release.imported_at = datetime.now()
        mock_release.published_at = datetime.now()
        mock_release.notes = "Test release"
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_release]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/releases?limit=10&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == self.test_release_id
        assert data[0]["type"] == "RVU_FULL"
        assert data[0]["source_version"] == "2025D"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_release_by_id_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/releases/{release_id} contract"""
        
        # Mock database response
        mock_release = Mock()
        mock_release.id = self.test_release_id
        mock_release.type = "RVU_FULL"
        mock_release.source_version = "2025D"
        mock_release.source_url = "https://example.com"
        mock_release.imported_at = datetime.now()
        mock_release.published_at = datetime.now()
        mock_release.notes = "Test release"
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_release
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get(f"/api/v1/rvu/releases/{self.test_release_id}")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == self.test_release_id
        assert data["type"] == "RVU_FULL"
        assert data["source_version"] == "2025D"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_search_rvu_items_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/rvu-items contract"""
        
        # Mock database response
        mock_item = Mock()
        mock_item.id = self.test_rvu_item_id
        mock_item.hcpcs_code = "99213"
        mock_item.modifiers = []
        mock_item.description = "Office visit"
        mock_item.status_code = "A"
        mock_item.work_rvu = 1.0
        mock_item.pe_rvu_nonfac = 0.5
        mock_item.pe_rvu_fac = 0.3
        mock_item.mp_rvu = 0.1
        mock_item.na_indicator = "0"
        mock_item.global_days = "000"
        mock_item.bilateral_ind = "0"
        mock_item.multiple_proc_ind = "0"
        mock_item.assistant_surg_ind = "0"
        mock_item.co_surg_ind = "0"
        mock_item.team_surg_ind = "0"
        mock_item.endoscopic_base = "0"
        mock_item.conversion_factor = 32.3465
        mock_item.physician_supervision = "01"
        mock_item.diag_imaging_family = "00"
        mock_item.total_nonfac = 100.0
        mock_item.total_fac = 80.0
        mock_item.effective_start = date.today()
        mock_item.effective_end = date.today()
        mock_item.source_file = "test.txt"
        mock_item.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.count.return_value = 1
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_item]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/rvu-items?hcpcs_code=99213&limit=100&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total_count" in data
        assert "limit" in data
        assert "offset" in data
        assert data["total_count"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["hcpcs_code"] == "99213"
        assert data["items"][0]["status_code"] == "A"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_rvu_item_by_id_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/rvu-items/{item_id} contract"""
        
        # Mock database response
        mock_item = Mock()
        mock_item.id = self.test_rvu_item_id
        mock_item.hcpcs_code = "99213"
        mock_item.modifiers = []
        mock_item.description = "Office visit"
        mock_item.status_code = "A"
        mock_item.work_rvu = 1.0
        mock_item.pe_rvu_nonfac = 0.5
        mock_item.pe_rvu_fac = 0.3
        mock_item.mp_rvu = 0.1
        mock_item.na_indicator = "0"
        mock_item.global_days = "000"
        mock_item.bilateral_ind = "0"
        mock_item.multiple_proc_ind = "0"
        mock_item.assistant_surg_ind = "0"
        mock_item.co_surg_ind = "0"
        mock_item.team_surg_ind = "0"
        mock_item.endoscopic_base = "0"
        mock_item.conversion_factor = 32.3465
        mock_item.physician_supervision = "01"
        mock_item.diag_imaging_family = "00"
        mock_item.total_nonfac = 100.0
        mock_item.total_fac = 80.0
        mock_item.effective_start = date.today()
        mock_item.effective_end = date.today()
        mock_item.source_file = "test.txt"
        mock_item.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_item
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get(f"/api/v1/rvu/rvu-items/{self.test_rvu_item_id}")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == self.test_rvu_item_id
        assert data["hcpcs_code"] == "99213"
        assert data["status_code"] == "A"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_gpci_indices_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/gpci contract"""
        
        # Mock database response
        mock_gpci = Mock()
        mock_gpci.id = self.test_gpci_id
        mock_gpci.mac = "10112"
        mock_gpci.state = "AL"
        mock_gpci.locality_id = "00"
        mock_gpci.locality_name = "Alabama"
        mock_gpci.work_gpci = 1.0
        mock_gpci.pe_gpci = 0.869
        mock_gpci.mp_gpci = 0.575
        mock_gpci.effective_start = date.today()
        mock_gpci.effective_end = date.today()
        mock_gpci.source_file = "test.txt"
        mock_gpci.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_gpci]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/gpci?mac=10112&limit=100&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["mac"] == "10112"
        assert data[0]["state"] == "AL"
        assert data[0]["locality_id"] == "00"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_opps_caps_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/opps-caps contract"""
        
        # Mock database response
        mock_opps = Mock()
        mock_opps.id = self.test_opps_cap_id
        mock_opps.hcpcs_code = "0633T"
        mock_opps.modifier = "TC"
        mock_opps.proc_status = "C"
        mock_opps.mac = "01112"
        mock_opps.locality_id = "05"
        mock_opps.price_fac = 150.69
        mock_opps.price_nonfac = 150.69
        mock_opps.effective_start = date.today()
        mock_opps.effective_end = date.today()
        mock_opps.source_file = "test.txt"
        mock_opps.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_opps]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/opps-caps?hcpcs_code=0633T&limit=100&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["hcpcs_code"] == "0633T"
        assert data[0]["mac"] == "01112"
        assert data[0]["price_fac"] == 150.69
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_anes_cfs_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/anes-cfs contract"""
        
        # Mock database response
        mock_anes = Mock()
        mock_anes.id = self.test_anes_cf_id
        mock_anes.mac = "10112"
        mock_anes.locality_id = "00"
        mock_anes.locality_name = "Alabama"
        mock_anes.anesthesia_cf = 19.31
        mock_anes.effective_start = date.today()
        mock_anes.effective_end = date.today()
        mock_anes.source_file = "test.txt"
        mock_anes.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_anes]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/anes-cfs?mac=10112&limit=100&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["mac"] == "10112"
        assert data[0]["locality_id"] == "00"
        assert data[0]["anesthesia_cf"] == 19.31
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    @patch('cms_pricing.routers.rvu.get_db')
    def test_get_locality_counties_contract(self, mock_get_db):
        """Test GET /api/v1/rvu/locality-counties contract"""
        
        # Mock database response
        mock_locco = Mock()
        mock_locco.id = self.test_locality_county_id
        mock_locco.mac = "10112"
        mock_locco.locality_id = "0"
        mock_locco.state = "ALABAMA"
        mock_locco.fee_schedule_area = "STATEWIDE"
        mock_locco.county_name = "ALL COUNTIES"
        mock_locco.effective_start = date.today()
        mock_locco.effective_end = date.today()
        mock_locco.source_file = "test.txt"
        mock_locco.row_num = 1
        
        mock_db = Mock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [mock_locco]
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/locality-counties?mac=10112&limit=100&offset=0")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["mac"] == "10112"
        assert data[0]["state"] == "ALABAMA"
        assert data[0]["county_name"] == "ALL COUNTIES"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    def test_health_check_contract(self):
        """Test GET /api/v1/rvu/health contract"""
        
        # Test request
        start_time = time.time()
        response = client.get("/api/v1/rvu/health")
        end_time = time.time()
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["service"] == "RVU Data API"
        assert data["version"] == "1.0.0"
        
        # Verify latency SLO (500ms)
        assert (end_time - start_time) * 1000 < 500, f"Response time {end_time - start_time:.3f}s exceeds 500ms SLO"
    
    def test_error_handling_contract(self):
        """Test error handling contracts"""
        
        # Test 404 for non-existent release
        with patch('cms_pricing.routers.rvu.get_db') as mock_get_db:
            mock_db = Mock()
            mock_db.query.return_value.filter.return_value.first.return_value = None
            mock_get_db.return_value.__enter__.return_value = mock_db
            
            response = client.get("/api/v1/rvu/releases/non-existent-id")
            assert response.status_code == 404
            data = response.json()
            assert "not found" in data["detail"].lower()
        
        # Test 404 for non-existent RVU item
        with patch('cms_pricing.routers.rvu.get_db') as mock_get_db:
            mock_db = Mock()
            mock_db.query.return_value.filter.return_value.first.return_value = None
            mock_get_db.return_value.__enter__.return_value = mock_db
            
            response = client.get("/api/v1/rvu/rvu-items/non-existent-id")
            assert response.status_code == 404
            data = response.json()
            assert "not found" in data["detail"].lower()
    
    def test_pagination_contract(self):
        """Test pagination contracts"""
        
        with patch('cms_pricing.routers.rvu.get_db') as mock_get_db:
            mock_db = Mock()
            mock_db.query.return_value.filter.return_value.count.return_value = 1000
            mock_db.query.return_value.filter.return_value.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []
            mock_get_db.return_value.__enter__.return_value = mock_db
            
            # Test pagination parameters
            response = client.get("/api/v1/rvu/rvu-items?limit=50&offset=100")
            assert response.status_code == 200
            data = response.json()
            assert data["limit"] == 50
            assert data["offset"] == 100
            assert data["total_count"] == 1000
    
    def test_query_parameter_validation(self):
        """Test query parameter validation"""
        
        # Test invalid limit
        response = client.get("/api/v1/rvu/rvu-items?limit=2000")
        assert response.status_code == 422  # Validation error
        
        # Test invalid offset
        response = client.get("/api/v1/rvu/rvu-items?offset=-1")
        assert response.status_code == 422  # Validation error
        
        # Test valid parameters
        response = client.get("/api/v1/rvu/rvu-items?limit=100&offset=0")
        assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

