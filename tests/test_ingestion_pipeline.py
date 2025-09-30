"""Comprehensive tests for data ingestion pipeline"""

import asyncio
import io
import json
import tempfile
import zipfile
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
import uuid

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.nearest_zip_ingestion import (
    GazetteerIngester, UDSCrosswalkIngester, CMSZip5Ingester, NBERIngester,
    NearestZipIngestionPipeline
)
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, NBERCentroids, ZCTADistances, IngestRun
)
from cms_pricing.main import app


@pytest.fixture(scope="function")
def clean_db_session():
    """Create a clean database session for each test"""
    db = SessionLocal()
    try:
        # Clear all data before each test
        db.query(IngestRun).delete()
        db.query(ZCTADistances).delete()
        db.query(NBERCentroids).delete()
        db.query(CMSZipLocality).delete()
        db.query(ZipToZCTA).delete()
        db.query(ZCTACoords).delete()
        db.commit()
        yield db
    finally:
        db.close()


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_gazetteer_data():
    """Sample Gazetteer data for testing"""
    return """GEOID|INTPTLAT|INTPTLONG|ALAND|AWATER
01001|32.5431|-86.6449|1234567|89012
01002|32.5432|-86.6448|1234568|89013
01003|32.5433|-86.6447|1234569|89014"""


@pytest.fixture
def sample_uds_data():
    """Sample UDS crosswalk data for testing"""
    return pd.DataFrame({
        'zip5': ['01001', '01002', '01003'],
        'zcta5': ['01001', '01002', '01003'],
        'relationship': ['Zip matches ZCTA', 'Zip matches ZCTA', 'Zip matches ZCTA'],
        'weight': [1.0, 1.0, 1.0],
        'city': ['City1', 'City2', 'City3'],
        'state': ['AL', 'AL', 'AL']
    })


@pytest.fixture
def sample_nber_data():
    """Sample NBER data for testing"""
    return pd.DataFrame({
        'zip1': ['01001', '01002', '01003'],
        'lat1': [32.5431, 32.5432, 32.5433],
        'lon1': [-86.6449, -86.6448, -86.6447],
        'zip2': ['01002', '01003', '01001'],
        'lat2': [32.5432, 32.5433, 32.5431],
        'lon2': [-86.6448, -86.6447, -86.6449],
        'mi_to_zcta5': [0.5, 0.6, 0.7]
    })


class TestGazetteerIngester:
    """Test Gazetteer data ingestion"""
    
    @pytest.mark.asyncio
    async def test_gazetteer_ingestion(self, clean_db_session, temp_output_dir, sample_gazetteer_data):
        """Test complete Gazetteer ingestion process"""
        
        # Create sample ZIP file
        zip_content = self._create_gazetteer_zip(sample_gazetteer_data)
        
        # Mock the download
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.content = zip_content
            mock_response.raise_for_status = Mock()
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            ingester = GazetteerIngester(clean_db_session, str(temp_output_dir))
            result = await ingester.ingest()
        
        # Verify result
        assert result['status'] == 'success'
        assert 'zcta_centroids' in result['record_counts']
        assert result['record_counts']['zcta_centroids'] == 3
        
        # Verify data in database
        zcta_records = clean_db_session.query(ZCTACoords).all()
        assert len(zcta_records) == 3
        
        # Verify specific records
        zcta_01001 = clean_db_session.query(ZCTACoords).filter(ZCTACoords.zcta5 == '01001').first()
        assert zcta_01001 is not None
        assert zcta_01001.lat == 32.5431
        assert zcta_01001.lon == -86.6449
        assert zcta_01001.vintage == '2025'
    
    def _create_gazetteer_zip(self, data: str) -> bytes:
        """Create a ZIP file with Gazetteer data"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            zip_file.writestr('2025_Gaz_zcta_national.txt', data)
        
        return zip_buffer.getvalue()


class TestUDSCrosswalkIngester:
    """Test UDS crosswalk data ingestion"""
    
    @pytest.mark.asyncio
    async def test_uds_ingestion(self, clean_db_session, temp_output_dir, sample_uds_data):
        """Test complete UDS crosswalk ingestion process"""
        
        # Create sample Excel file
        excel_content = self._create_excel_file(sample_uds_data)
        
        # Mock the download
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.content = excel_content
            mock_response.raise_for_status = Mock()
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            ingester = UDSCrosswalkIngester(clean_db_session, str(temp_output_dir))
            result = await ingester.ingest()
        
        # Verify result
        assert result['status'] == 'success'
        assert 'zip_zcta_crosswalk' in result['record_counts']
        assert result['record_counts']['zip_zcta_crosswalk'] == 3
        
        # Verify data in database
        crosswalk_records = clean_db_session.query(ZipToZCTA).all()
        assert len(crosswalk_records) == 3
        
        # Verify specific records
        zip_01001 = clean_db_session.query(ZipToZCTA).filter(ZipToZCTA.zip5 == '01001').first()
        assert zip_01001 is not None
        assert zip_01001.zcta5 == '01001'
        assert zip_01001.relationship == 'Zip matches ZCTA'
        assert zip_01001.weight == 1.0
    
    def _create_excel_file(self, df: pd.DataFrame) -> bytes:
        """Create an Excel file from DataFrame"""
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False)
        return excel_buffer.getvalue()


class TestNBERIngester:
    """Test NBER data ingestion"""
    
    @pytest.mark.asyncio
    async def test_nber_ingestion(self, clean_db_session, temp_output_dir, sample_nber_data):
        """Test complete NBER ingestion process"""
        
        # Create sample CSV file
        csv_content = sample_nber_data.to_csv(index=False)
        
        # Mock the download
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.content = csv_content.encode()
            mock_response.raise_for_status = Mock()
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            ingester = NBERIngester(clean_db_session, str(temp_output_dir))
            result = await ingester.ingest()
        
        # Verify result
        assert result['status'] == 'success'
        assert 'zcta_centroids' in result['record_counts']
        assert 'zcta_distances' in result['record_counts']
        
        # Verify centroids in database
        centroid_records = clean_db_session.query(NBERCentroids).all()
        assert len(centroid_records) == 3
        
        # Verify distances in database
        distance_records = clean_db_session.query(ZCTADistances).all()
        assert len(distance_records) == 3
        
        # Verify specific records
        centroid_01001 = clean_db_session.query(NBERCentroids).filter(NBERCentroids.zcta5 == '01001').first()
        assert centroid_01001 is not None
        assert centroid_01001.lat == 32.5431
        assert centroid_01001.lon == -86.6449


class TestCMSZip5Ingester:
    """Test CMS ZIP5 data ingestion"""
    
    @pytest.mark.asyncio
    async def test_cms_zip5_ingestion(self, clean_db_session, temp_output_dir):
        """Test CMS ZIP5 ingestion with mocked scraping"""
        
        # Mock the CMS page scraping
        mock_html = """
        <html>
        <body>
        <a href="/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip">
        ZIP Code to Carrier Locality File
        </a>
        </body>
        </html>
        """
        
        # Create sample ZIP file with CSV data
        csv_data = "ZIP_CODE,CARRIER,LOCALITY,STATE\n01001,12345,01,AL\n01002,12345,01,AL"
        zip_content = self._create_zip_file({'zip_locality.csv': csv_data})
        
        # Mock the downloads
        with patch('httpx.AsyncClient') as mock_client:
            # Mock CMS page response
            mock_page_response = Mock()
            mock_page_response.content = mock_html.encode()
            mock_page_response.raise_for_status = Mock()
            
            # Mock ZIP file response
            mock_zip_response = Mock()
            mock_zip_response.content = zip_content
            mock_zip_response.raise_for_status = Mock()
            
            mock_client.return_value.__aenter__.return_value.get.side_effect = [
                mock_page_response,  # First call for CMS page
                mock_zip_response    # Second call for ZIP file
            ]
            
            ingester = CMSZip5Ingester(clean_db_session, str(temp_output_dir))
            result = await ingester.ingest()
        
        # Verify result
        assert result['status'] == 'success'
        
        # Verify data in database
        cms_records = clean_db_session.query(CMSZipLocality).all()
        assert len(cms_records) == 2
        
        # Verify specific records
        zip_01001 = clean_db_session.query(CMSZipLocality).filter(CMSZipLocality.zip5 == '01001').first()
        assert zip_01001 is not None
        assert zip_01001.state == 'AL'
        assert zip_01001.locality == '01'
    
    def _create_zip_file(self, files: dict) -> bytes:
        """Create a ZIP file with multiple files"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename, content in files.items():
                zip_file.writestr(filename, content)
        
        return zip_buffer.getvalue()


class TestIngestionPipeline:
    """Test the complete ingestion pipeline"""
    
    @pytest.mark.asyncio
    async def test_full_pipeline(self, clean_db_session, temp_output_dir):
        """Test the complete ingestion pipeline"""
        
        # Mock all downloads
        with patch('httpx.AsyncClient') as mock_client:
            # Create mock responses for all sources
            mock_responses = []
            
            # Gazetteer response
            gazetteer_data = "GEOID|INTPTLAT|INTPTLONG|ALAND|AWATER\n01001|32.5431|-86.6449|1234567|89012"
            gazetteer_zip = self._create_gazetteer_zip(gazetteer_data)
            mock_gazetteer = Mock()
            mock_gazetteer.content = gazetteer_zip
            mock_gazetteer.raise_for_status = Mock()
            mock_responses.append(mock_gazetteer)
            
            # UDS response
            uds_data = pd.DataFrame({
                'zip5': ['01001'], 'zcta5': ['01001'], 'relationship': ['Zip matches ZCTA'],
                'weight': [1.0], 'city': ['City1'], 'state': ['AL']
            })
            uds_excel = self._create_excel_file(uds_data)
            mock_uds = Mock()
            mock_uds.content = uds_excel
            mock_uds.raise_for_status = Mock()
            mock_responses.append(mock_uds)
            
            # CMS response
            cms_html = '<html><body><a href="/test.zip">ZIP Code to Carrier Locality File</a></body></html>'
            cms_zip = self._create_zip_file({'zip_locality.csv': 'ZIP_CODE,CARRIER,LOCALITY,STATE\n01001,12345,01,AL'})
            mock_cms_page = Mock()
            mock_cms_page.content = cms_html.encode()
            mock_cms_page.raise_for_status = Mock()
            mock_cms_zip = Mock()
            mock_cms_zip.content = cms_zip
            mock_cms_zip.raise_for_status = Mock()
            mock_responses.extend([mock_cms_page, mock_cms_zip])
            
            # NBER response
            nber_data = pd.DataFrame({
                'zip1': ['01001'], 'lat1': [32.5431], 'lon1': [-86.6449],
                'zip2': ['01002'], 'lat2': [32.5432], 'lon2': [-86.6448],
                'mi_to_zcta5': [0.5]
            })
            nber_csv = nber_data.to_csv(index=False)
            mock_nber = Mock()
            mock_nber.content = nber_csv.encode()
            mock_nber.raise_for_status = Mock()
            mock_responses.append(mock_nber)
            
            # Set up mock client
            mock_client.return_value.__aenter__.return_value.get.side_effect = mock_responses
            
            # Run pipeline
            pipeline = NearestZipIngestionPipeline(str(temp_output_dir))
            results = await pipeline.run_full_pipeline()
        
        # Verify results
        assert results['overall_status'] == 'success'
        assert len(results['results']) == 4
        
        # Verify all sources were processed
        for source in ['gazetteer', 'uds_crosswalk', 'cms_zip5', 'nber']:
            assert source in results['results']
            assert results['results'][source]['status'] == 'success'
        
        # Verify data was loaded to database
        assert clean_db_session.query(ZCTACoords).count() > 0
        assert clean_db_session.query(ZipToZCTA).count() > 0
        assert clean_db_session.query(CMSZipLocality).count() > 0
        assert clean_db_session.query(NBERCentroids).count() > 0
        assert clean_db_session.query(ZCTADistances).count() > 0
    
    def _create_gazetteer_zip(self, data: str) -> bytes:
        """Create a ZIP file with Gazetteer data"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            zip_file.writestr('2025_Gaz_zcta_national.txt', data)
        
        return zip_buffer.getvalue()
    
    def _create_excel_file(self, df: pd.DataFrame) -> bytes:
        """Create an Excel file from DataFrame"""
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False)
        return excel_buffer.getvalue()
    
    def _create_zip_file(self, files: dict) -> bytes:
        """Create a ZIP file with multiple files"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename, content in files.items():
                zip_file.writestr(filename, content)
        
        return zip_buffer.getvalue()


class TestIngestionValidation:
    """Test data validation in ingestion pipeline"""
    
    def test_gazetteer_validation(self, clean_db_session, temp_output_dir):
        """Test Gazetteer data validation"""
        ingester = GazetteerIngester(clean_db_session, str(temp_output_dir))
        
        # Test with valid data
        valid_data = {
            'zcta_centroids.zip': b'valid_zip_content'
        }
        warnings = ingester.validate_raw_data(valid_data)
        assert len(warnings) == 0
        
        # Test with invalid data
        invalid_data = {
            'empty_file.zip': b'',
            'wrong_extension.txt': b'some_content'
        }
        warnings = ingester.validate_raw_data(invalid_data)
        assert len(warnings) == 2
        assert any('Empty file' in w for w in warnings)
        assert any('Unexpected file type' in w for w in warnings)
    
    def test_uds_validation(self, clean_db_session, temp_output_dir):
        """Test UDS crosswalk data validation"""
        ingester = UDSCrosswalkIngester(clean_db_session, str(temp_output_dir))
        
        # Test with valid data
        valid_data = {
            'zip_zcta_crosswalk.xlsx': b'valid_excel_content'
        }
        warnings = ingester.validate_raw_data(valid_data)
        assert len(warnings) == 0
        
        # Test with invalid data
        invalid_data = {
            'empty_file.xlsx': b'',
            'wrong_extension.csv': b'some_content'
        }
        warnings = ingester.validate_raw_data(invalid_data)
        assert len(warnings) == 2


class TestIngestionCLI:
    """Test CLI interface for ingestion"""
    
    def test_cli_help(self):
        """Test CLI help output"""
        from cms_pricing.cli.ingestion import main
        
        # This would test the CLI help, but we'll skip for now
        # as it requires more complex testing setup
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
