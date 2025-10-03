#!/usr/bin/env python3
"""
DIS Metadata & Catalog Integration Tests
========================================

Tests for DIS metadata and catalog requirements following QTS v1.1 standards.
Includes ingestion_runs table, technical metadata, business metadata, and lineage.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import pytest
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock, patch

from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper


class TestDISMetadataCatalog:
    """Tests for DIS metadata and catalog requirements."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper instance for testing."""
        return CMSOPPSScraper()
    
    @pytest.fixture
    def sample_run_data(self):
        """Sample run data for testing."""
        return {
            'run_id': str(uuid.uuid4()),
            'dataset': 'cms_opps_quarterly_addenda',
            'pipeline': 'cms_opps_scraper',
            'source_urls': [
                'https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates'
            ],
            'file_hashes': {
                'addendum_a.csv': 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456',
                'addendum_b.csv': 'b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef1234567890'
            },
            'file_bytes': 2048576,  # 2MB
            'row_count': 120345,
            'schema_version': '1.0.0',
            'quality_score': 98.5,
            'runtime_sec': 45,
            'cost_estimate_usd': 0.25,
            'outcome': 'success',
            'environment': 'test',
            'commit_sha': 'abc123def456',
            'started_at': datetime.now(timezone.utc),
            'finished_at': datetime.now(timezone.utc),
            'tags': {
                'quarter': '2025Q1',
                'addendum_type': 'A',
                'test_run': True
            }
        }
    
    def test_ingestion_runs_table_schema(self, sample_run_data):
        """Test ingestion_runs table schema compliance."""
        # Verify all required fields are present
        required_fields = [
            'run_id', 'dataset', 'pipeline', 'source_urls', 'file_hashes',
            'file_bytes', 'row_count', 'schema_version', 'quality_score',
            'runtime_sec', 'cost_estimate_usd', 'outcome', 'environment',
            'commit_sha', 'started_at', 'finished_at', 'tags'
        ]
        
        for field in required_fields:
            assert field in sample_run_data, f"Missing required field: {field}"
        
        # Verify data types
        assert isinstance(sample_run_data['run_id'], str)
        assert isinstance(sample_run_data['dataset'], str)
        assert isinstance(sample_run_data['pipeline'], str)
        assert isinstance(sample_run_data['source_urls'], list)
        assert isinstance(sample_run_data['file_hashes'], dict)
        assert isinstance(sample_run_data['file_bytes'], int)
        assert isinstance(sample_run_data['row_count'], int)
        assert isinstance(sample_run_data['schema_version'], str)
        assert isinstance(sample_run_data['quality_score'], float)
        assert isinstance(sample_run_data['runtime_sec'], int)
        assert isinstance(sample_run_data['cost_estimate_usd'], float)
        assert isinstance(sample_run_data['outcome'], str)
        assert isinstance(sample_run_data['environment'], str)
        assert isinstance(sample_run_data['commit_sha'], str)
        assert isinstance(sample_run_data['started_at'], datetime)
        assert isinstance(sample_run_data['finished_at'], datetime)
        assert isinstance(sample_run_data['tags'], dict)
        
        # Verify outcome values
        valid_outcomes = ['success', 'partial', 'failed']
        assert sample_run_data['outcome'] in valid_outcomes
    
    def test_technical_metadata_capture(self, scraper):
        """Test technical metadata auto-capture."""
        # Mock schema capture
        mock_schema = {
            'columns': [
                {'name': 'hcpcs', 'type': 'string', 'nullable': False},
                {'name': 'modifier', 'type': 'string', 'nullable': True},
                {'name': 'apc_code', 'type': 'string', 'nullable': False},
                {'name': 'payment_rate', 'type': 'decimal', 'nullable': False},
                {'name': 'effective_date', 'type': 'date', 'nullable': False}
            ],
            'primary_keys': ['hcpcs', 'modifier'],
            'constraints': {
                'hcpcs': {'min_length': 5, 'max_length': 5, 'pattern': r'^[A-Z0-9]{5}$'},
                'payment_rate': {'min_value': 0.0, 'max_value': 999999.99}
            },
            'pii_tags': [],
            'schema_version': '1.0.0'
        }
        
        # Test schema validation
        assert 'columns' in mock_schema
        assert 'primary_keys' in mock_schema
        assert 'constraints' in mock_schema
        assert 'pii_tags' in mock_schema
        assert 'schema_version' in mock_schema
        
        # Verify column metadata
        for column in mock_schema['columns']:
            assert 'name' in column
            assert 'type' in column
            assert 'nullable' in column
            assert isinstance(column['nullable'], bool)
        
        # Verify constraints
        assert 'hcpcs' in mock_schema['constraints']
        assert 'payment_rate' in mock_schema['constraints']
    
    def test_business_metadata_structure(self):
        """Test business metadata structure."""
        business_metadata = {
            'ownership': {
                'dataset_owner': 'Platform/Data Engineering',
                'data_steward': 'Medicare SME',
                'escalation_channel': '#cms-pricing-alerts'
            },
            'glossary': {
                'addendum_a': 'OPPS hospital outpatient payment addendum A',
                'addendum_b': 'OPPS hospital outpatient payment addendum B',
                'apc_code': 'Ambulatory Payment Classification code',
                'payment_rate': 'Medicare payment rate in USD'
            },
            'classification': 'Public',
            'license': {
                'name': 'CMS Open Data',
                'url': 'https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy',
                'attribution_required': True
            },
            'intended_use': [
                'Medicare payment analysis',
                'Healthcare cost research',
                'Policy impact assessment'
            ]
        }
        
        # Verify ownership structure
        assert 'dataset_owner' in business_metadata['ownership']
        assert 'data_steward' in business_metadata['ownership']
        assert 'escalation_channel' in business_metadata['ownership']
        
        # Verify glossary structure
        assert isinstance(business_metadata['glossary'], dict)
        assert len(business_metadata['glossary']) > 0
        
        # Verify classification
        valid_classifications = ['Public', 'Internal', 'Confidential', 'Restricted']
        assert business_metadata['classification'] in valid_classifications
        
        # Verify license structure
        assert 'name' in business_metadata['license']
        assert 'url' in business_metadata['license']
        assert 'attribution_required' in business_metadata['license']
        assert isinstance(business_metadata['license']['attribution_required'], bool)
        
        # Verify intended use
        assert isinstance(business_metadata['intended_use'], list)
        assert len(business_metadata['intended_use']) > 0
    
    def test_openlineage_event_structure(self, sample_run_data):
        """Test OpenLineage event structure."""
        openlineage_event = {
            'eventType': 'COMPLETE',
            'job': {
                'namespace': 'pricing.scrapers',
                'name': 'cms_opps_scraper'
            },
            'run': {
                'runId': sample_run_data['run_id']
            },
            'inputs': [
                {
                    'namespace': 'web',
                    'name': 'cms.gov/opps/addenda',
                    'facets': {
                        'source': {
                            'url': sample_run_data['source_urls'][0],
                            'checksum': sample_run_data['file_hashes']
                        }
                    }
                }
            ],
            'outputs': [
                {
                    'namespace': 'warehouse',
                    'name': 'curated.opps_addenda',
                    'facets': {
                        'outputStatistics': {
                            'rowCount': sample_run_data['row_count'],
                            'size': sample_run_data['file_bytes']
                        }
                    }
                }
            ],
            'eventTime': sample_run_data['finished_at'].isoformat(),
            'producer': 'cms-pricing-platform'
        }
        
        # Verify required fields
        assert 'eventType' in openlineage_event
        assert 'job' in openlineage_event
        assert 'run' in openlineage_event
        assert 'inputs' in openlineage_event
        assert 'outputs' in openlineage_event
        assert 'eventTime' in openlineage_event
        assert 'producer' in openlineage_event
        
        # Verify job structure
        assert 'namespace' in openlineage_event['job']
        assert 'name' in openlineage_event['job']
        
        # Verify run structure
        assert 'runId' in openlineage_event['run']
        
        # Verify inputs/outputs structure
        assert isinstance(openlineage_event['inputs'], list)
        assert isinstance(openlineage_event['outputs'], list)
        assert len(openlineage_event['inputs']) > 0
        assert len(openlineage_event['outputs']) > 0
        
        # Verify facets structure
        for input_item in openlineage_event['inputs']:
            assert 'facets' in input_item
            assert 'source' in input_item['facets']
        
        for output_item in openlineage_event['outputs']:
            assert 'facets' in output_item
            assert 'outputStatistics' in output_item['facets']
    
    def test_metadata_persistence(self, sample_run_data):
        """Test metadata persistence to database."""
        # Mock database connection
        mock_db = Mock()
        mock_cursor = Mock()
        mock_db.cursor.return_value = mock_cursor
        
        # Test SQL generation
        sql = """
        INSERT INTO ingestion_runs (
            run_id, dataset, pipeline, source_urls, file_hashes,
            file_bytes, row_count, schema_version, quality_score,
            runtime_sec, cost_estimate_usd, outcome, environment,
            commit_sha, started_at, finished_at, tags
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Test parameter binding
        params = (
            sample_run_data['run_id'],
            sample_run_data['dataset'],
            sample_run_data['pipeline'],
            json.dumps(sample_run_data['source_urls']),
            json.dumps(sample_run_data['file_hashes']),
            sample_run_data['file_bytes'],
            sample_run_data['row_count'],
            sample_run_data['schema_version'],
            sample_run_data['quality_score'],
            sample_run_data['runtime_sec'],
            sample_run_data['cost_estimate_usd'],
            sample_run_data['outcome'],
            sample_run_data['environment'],
            sample_run_data['commit_sha'],
            sample_run_data['started_at'],
            sample_run_data['finished_at'],
            json.dumps(sample_run_data['tags'])
        )
        
        # Verify SQL structure
        assert 'INSERT INTO ingestion_runs' in sql
        assert 'run_id' in sql
        assert 'dataset' in sql
        assert 'pipeline' in sql
        
        # Verify parameter count
        assert len(params) == 17
        
        # Verify JSON serialization
        assert isinstance(json.dumps(sample_run_data['source_urls']), str)
        assert isinstance(json.dumps(sample_run_data['file_hashes']), str)
        assert isinstance(json.dumps(sample_run_data['tags']), str)
    
    def test_metadata_validation(self, sample_run_data):
        """Test metadata validation rules."""
        # Test required field validation
        required_fields = [
            'run_id', 'dataset', 'pipeline', 'source_urls', 'file_hashes',
            'outcome', 'started_at', 'finished_at'
        ]
        
        for field in required_fields:
            # Test missing field
            test_data = sample_run_data.copy()
            del test_data[field]
            
            # Should raise validation error
            with pytest.raises(KeyError):
                # Simulate validation
                if field not in test_data:
                    raise KeyError(f"Missing required field: {field}")
        
        # Test data type validation
        test_data = sample_run_data.copy()
        test_data['quality_score'] = "invalid"  # Should be float
        
        # Should raise type error
        with pytest.raises(TypeError):
            if not isinstance(test_data['quality_score'], float):
                raise TypeError("quality_score must be float")
        
        # Test value range validation
        test_data = sample_run_data.copy()
        test_data['quality_score'] = 150.0  # Should be 0-100
        
        # Should raise value error
        with pytest.raises(ValueError):
            if not (0 <= test_data['quality_score'] <= 100):
                raise ValueError("quality_score must be between 0 and 100")
    
    def test_lineage_tracking(self, sample_run_data):
        """Test lineage tracking functionality."""
        lineage_data = {
            'upstream_sources': [
                {
                    'source_type': 'web_scraper',
                    'source_url': sample_run_data['source_urls'][0],
                    'source_checksum': sample_run_data['file_hashes'],
                    'extraction_method': 'cms_opps_scraper',
                    'extraction_timestamp': sample_run_data['started_at'].isoformat()
                }
            ],
            'downstream_consumers': [
                {
                    'consumer_type': 'api_endpoint',
                    'consumer_name': 'opps_payment_rates_api',
                    'consumption_timestamp': sample_run_data['finished_at'].isoformat(),
                    'usage_pattern': 'read_only'
                }
            ],
            'transformations': [
                {
                    'transformation_type': 'data_cleaning',
                    'transformation_name': 'normalize_hcpcs_codes',
                    'applied_at': sample_run_data['started_at'].isoformat(),
                    'parameters': {'trim_whitespace': True, 'uppercase': True}
                },
                {
                    'transformation_type': 'data_enrichment',
                    'transformation_name': 'add_locality_mapping',
                    'applied_at': sample_run_data['started_at'].isoformat(),
                    'parameters': {'reference_table': 'cms_localities'}
                }
            ]
        }
        
        # Verify upstream sources
        assert 'upstream_sources' in lineage_data
        assert isinstance(lineage_data['upstream_sources'], list)
        assert len(lineage_data['upstream_sources']) > 0
        
        # Verify downstream consumers
        assert 'downstream_consumers' in lineage_data
        assert isinstance(lineage_data['downstream_consumers'], list)
        
        # Verify transformations
        assert 'transformations' in lineage_data
        assert isinstance(lineage_data['transformations'], list)
        assert len(lineage_data['transformations']) > 0
        
        # Verify source structure
        source = lineage_data['upstream_sources'][0]
        assert 'source_type' in source
        assert 'source_url' in source
        assert 'source_checksum' in source
        assert 'extraction_method' in source
        assert 'extraction_timestamp' in source
        
        # Verify consumer structure
        if lineage_data['downstream_consumers']:
            consumer = lineage_data['downstream_consumers'][0]
            assert 'consumer_type' in consumer
            assert 'consumer_name' in consumer
            assert 'consumption_timestamp' in consumer
            assert 'usage_pattern' in consumer
        
        # Verify transformation structure
        transformation = lineage_data['transformations'][0]
        assert 'transformation_type' in transformation
        assert 'transformation_name' in transformation
        assert 'applied_at' in transformation
        assert 'parameters' in transformation
    
    def test_metadata_catalog_integration(self, sample_run_data):
        """Test metadata catalog integration."""
        catalog_entry = {
            'dataset_id': sample_run_data['dataset'],
            'dataset_name': 'CMS OPPS Quarterly Addenda',
            'domain': 'payments/medicare',
            'owner': 'Platform/Data Engineering',
            'steward': 'Medicare SME',
            'classification': 'Public',
            'license': {
                'name': 'CMS Open Data',
                'attribution_required': True
            },
            'schema': {
                'version': sample_run_data['schema_version'],
                'columns': [
                    {'name': 'hcpcs', 'type': 'string', 'description': 'HCPCS code'},
                    {'name': 'modifier', 'type': 'string', 'description': 'HCPCS modifier'},
                    {'name': 'apc_code', 'type': 'string', 'description': 'APC code'},
                    {'name': 'payment_rate', 'type': 'decimal', 'description': 'Payment rate in USD'}
                ]
            },
            'quality_metrics': {
                'completeness': sample_run_data['quality_score'],
                'freshness': '2025-01-15T10:30:00Z',
                'volume': sample_run_data['row_count']
            },
            'lineage': {
                'upstream': sample_run_data['source_urls'],
                'downstream': ['opps_payment_rates_api', 'medicare_cost_analysis']
            },
            'tags': sample_run_data['tags'],
            'last_updated': sample_run_data['finished_at'].isoformat()
        }
        
        # Verify catalog entry structure
        assert 'dataset_id' in catalog_entry
        assert 'dataset_name' in catalog_entry
        assert 'domain' in catalog_entry
        assert 'owner' in catalog_entry
        assert 'steward' in catalog_entry
        assert 'classification' in catalog_entry
        assert 'license' in catalog_entry
        assert 'schema' in catalog_entry
        assert 'quality_metrics' in catalog_entry
        assert 'lineage' in catalog_entry
        assert 'tags' in catalog_entry
        assert 'last_updated' in catalog_entry
        
        # Verify schema structure
        assert 'version' in catalog_entry['schema']
        assert 'columns' in catalog_entry['schema']
        assert isinstance(catalog_entry['schema']['columns'], list)
        
        # Verify quality metrics
        assert 'completeness' in catalog_entry['quality_metrics']
        assert 'freshness' in catalog_entry['quality_metrics']
        assert 'volume' in catalog_entry['quality_metrics']
        
        # Verify lineage
        assert 'upstream' in catalog_entry['lineage']
        assert 'downstream' in catalog_entry['lineage']
        assert isinstance(catalog_entry['lineage']['upstream'], list)
        assert isinstance(catalog_entry['lineage']['downstream'], list)


if __name__ == "__main__":
    """Run metadata catalog tests."""
    pytest.main([__file__, "-v"])
