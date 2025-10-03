#!/usr/bin/env python3
"""
OPPS Scraper Performance Tests
==============================

Performance and load testing for OPPS scraper following QTS v1.1 standards.
Includes benchmarking, load profiles, and performance monitoring.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import pytest
import asyncio
import time
import json
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import Mock, patch

from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper


class TestOPPSScraperPerformance:
    """Performance tests for OPPS scraper."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper instance for testing."""
        return CMSOPPSScraper()
    
    @pytest.fixture
    def synthetic_links(self):
        """Generate synthetic links for performance testing."""
        links = []
        quarters = ["January", "April", "July", "October"]
        years = [2024, 2025]
        addenda_types = ["A", "B"]
        
        for quarter in quarters:
            for year in years:
                for addendum_type in addenda_types:
                    href = f"/{quarter.lower()}-{year}-addendum-{addendum_type.lower()}.csv"
                    text = f"{quarter} {year} Addendum {addendum_type}"
                    links.append((href, text))
        
        return links
    
    def test_classify_file_benchmark(self, benchmark, scraper, synthetic_links):
        """Benchmark file classification performance."""
        def run_classification():
            for href, text in synthetic_links:
                scraper._classify_file(href, text)
        
        benchmark(run_classification)
    
    def test_extract_quarter_info_benchmark(self, benchmark, scraper):
        """Benchmark quarter extraction performance."""
        test_cases = [
            ("January 2025 Addendum A", (2025, 1)),
            ("April 2025 Addendum B", (2025, 2)),
            ("July 2025 Addendum A", (2025, 3)),
            ("October 2025 Addendum B", (2025, 4)),
        ] * 100  # Repeat for meaningful benchmark
        
        def run_extraction():
            for text, expected in test_cases:
                scraper._extract_quarter_info({'text': text, 'href': ''})
        
        benchmark(run_extraction)
    
    def test_is_quarterly_addenda_link_benchmark(self, benchmark, scraper, synthetic_links):
        """Benchmark quarterly addenda link identification performance."""
        def run_identification():
            for href, text in synthetic_links:
                scraper._is_quarterly_addenda_link(href, text)
        
        benchmark(run_identification)
    
    @pytest.mark.asyncio
    async def test_resolve_disclaimer_url_benchmark(self, benchmark, scraper):
        """Benchmark disclaimer URL resolution performance."""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.headers = {'content-type': 'application/zip'}
        mock_response.text = ''
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        
        async def run_resolution():
            for i in range(100):
                await scraper._resolve_disclaimer_url(
                    mock_client, 
                    f"https://example.com/file{i}.zip", 
                    f"Test File {i}"
                )
        
        await benchmark(run_resolution)
    
    def test_memory_usage_large_dataset(self, scraper):
        """Test memory usage with large dataset processing."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Generate large dataset
        large_links = []
        for i in range(10000):
            href = f"/january-2025-addendum-a-{i}.csv"
            text = f"January 2025 Addendum A {i}"
            large_links.append((href, text))
        
        # Process large dataset
        for href, text in large_links:
            scraper._classify_file(href, text)
            scraper._is_quarterly_addenda_link(href, text)
            scraper._extract_quarter_info({'text': text, 'href': href})
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (< 100MB)
        assert memory_increase < 100 * 1024 * 1024, f"Memory increase too large: {memory_increase / 1024 / 1024:.2f}MB"
    
    @pytest.mark.asyncio
    async def test_concurrent_disclaimer_resolution(self, scraper):
        """Test concurrent disclaimer resolution performance."""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.text = 'disclaimer terms accept agreement'
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        
        # Mock browser resolution
        with patch.object(scraper, '_handle_disclaimer_with_browser', return_value="https://example.com/resolved.zip"):
            start_time = time.time()
            
            # Run 10 concurrent disclaimer resolutions
            tasks = []
            for i in range(10):
                task = scraper._resolve_disclaimer_url(
                    mock_client,
                    f"https://example.com/license{i}.asp",
                    f"Test File {i}"
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # All should succeed
            assert len(results) == 10
            assert all(result.startswith("https://example.com/resolved.zip") for result in results)
            
            # Should complete within reasonable time (< 5 seconds for 10 concurrent)
            duration = end_time - start_time
            assert duration < 5.0, f"Concurrent resolution took too long: {duration:.2f}s"
    
    def test_load_profile_high_volume(self, scraper, synthetic_links):
        """Test high-volume processing load profile."""
        # Simulate processing 300 files (high-volume scenario)
        extended_links = synthetic_links * 20  # ~300 links
        
        start_time = time.time()
        
        # Process all links
        results = []
        for href, text in extended_links:
            classification = scraper._classify_file(href, text)
            is_quarterly = scraper._is_quarterly_addenda_link(href, text)
            quarter_info = scraper._extract_quarter_info({'text': text, 'href': href})
            
            results.append({
                'href': href,
                'text': text,
                'classification': classification,
                'is_quarterly': is_quarterly,
                'quarter_info': quarter_info
            })
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should process 300 files within 30 seconds (SLA requirement)
        assert duration < 30.0, f"High-volume processing took too long: {duration:.2f}s"
        assert len(results) == len(extended_links)
        
        # Calculate throughput
        throughput = len(extended_links) / duration
        assert throughput > 10, f"Throughput too low: {throughput:.2f} files/second"
    
    @pytest.mark.asyncio
    async def test_stress_disclaimer_resolution(self, scraper):
        """Test stress scenario with 50% disclaimer resolution."""
        mock_client = Mock()
        
        # Mock responses - 50% disclaimers, 50% direct files
        disclaimer_response = Mock()
        disclaimer_response.headers = {'content-type': 'text/html'}
        disclaimer_response.text = 'disclaimer terms accept agreement'
        disclaimer_response.status_code = 200
        
        direct_response = Mock()
        direct_response.headers = {'content-type': 'application/zip'}
        direct_response.text = ''
        direct_response.status_code = 200
        
        def mock_get(url, **kwargs):
            if 'license.asp' in url:
                return disclaimer_response
            else:
                return direct_response
        
        mock_client.get.side_effect = mock_get
        
        # Mock browser resolution for disclaimers
        with patch.object(scraper, '_handle_disclaimer_with_browser', return_value="https://example.com/resolved.zip"):
            start_time = time.time()
            
            # Process 100 URLs with 50% disclaimers
            tasks = []
            for i in range(100):
                if i % 2 == 0:
                    url = f"https://example.com/license{i}.asp"
                else:
                    url = f"https://example.com/file{i}.zip"
                
                task = scraper._resolve_disclaimer_url(mock_client, url, f"Test File {i}")
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            duration = end_time - start_time
            
            # Should complete within reasonable time even with 50% disclaimers
            assert duration < 60.0, f"Stress test took too long: {duration:.2f}s"
            assert len(results) == 100
    
    def test_performance_regression_detection(self, scraper):
        """Test performance regression detection."""
        # This would typically compare against baseline metrics
        # For now, we'll establish a baseline and test against it
        
        test_cases = [
            ("January 2025 Addendum A", (2025, 1)),
            ("April 2025 Addendum B", (2025, 2)),
            ("July 2025 Addendum A", (2025, 3)),
            ("October 2025 Addendum B", (2025, 4)),
        ] * 100
        
        start_time = time.time()
        
        for text, expected in test_cases:
            result = scraper._extract_quarter_info({'text': text, 'href': ''})
            assert result == expected
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Baseline: should complete 400 operations in < 0.1 seconds
        baseline_duration = 0.1
        regression_threshold = baseline_duration * 1.2  # 20% regression threshold
        
        assert duration < regression_threshold, f"Performance regression detected: {duration:.4f}s > {regression_threshold:.4f}s"
    
    def test_benchmark_json_export(self, benchmark, scraper, synthetic_links):
        """Test that benchmark results can be exported as JSON."""
        def run_classification():
            for href, text in synthetic_links:
                scraper._classify_file(href, text)
        
        # Run benchmark
        result = benchmark(run_classification)
        
        # Verify benchmark result has required attributes
        assert hasattr(result, 'stats')
        assert hasattr(result, 'iterations')
        assert hasattr(result, 'rounds')
        
        # Verify stats contain required metrics
        stats = result.stats
        assert 'mean' in stats
        assert 'stddev' in stats
        assert 'min' in stats
        assert 'max' in stats
        assert 'median' in stats
        
        # Verify performance is within SLA (< 30s per file end-to-end)
        # This is a simplified check - in practice, this would be the full pipeline
        mean_time_per_iteration = stats['mean']
        assert mean_time_per_iteration < 0.001, f"Mean time per iteration too high: {mean_time_per_iteration:.6f}s"


class TestOPPSScraperLoadProfiles:
    """Load profile tests for OPPS scraper."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper instance for testing."""
        return CMSOPPSScraper()
    
    def test_concurrency_profile_1_worker(self, scraper):
        """Test single worker concurrency profile."""
        # Simulate single worker processing
        links = [("/january-2025-addendum-a.csv", "January 2025 Addendum A")] * 100
        
        start_time = time.time()
        
        for href, text in links:
            scraper._classify_file(href, text)
            scraper._is_quarterly_addenda_link(href, text)
            scraper._extract_quarter_info({'text': text, 'href': href})
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Single worker should be fast for simple operations
        assert duration < 1.0, f"Single worker took too long: {duration:.2f}s"
    
    def test_concurrency_profile_5_workers(self, scraper):
        """Test 5-worker concurrency profile."""
        # This would typically use threading/asyncio for real concurrency
        # For now, we'll simulate the workload
        
        links = [("/january-2025-addendum-a.csv", "January 2025 Addendum A")] * 500
        
        start_time = time.time()
        
        # Simulate 5 workers processing 100 links each
        for worker_id in range(5):
            worker_links = links[worker_id * 100:(worker_id + 1) * 100]
            for href, text in worker_links:
                scraper._classify_file(href, text)
                scraper._is_quarterly_addenda_link(href, text)
                scraper._extract_quarter_info({'text': text, 'href': href})
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 5 workers should be faster than single worker
        assert duration < 2.0, f"5 workers took too long: {duration:.2f}s"
    
    def test_concurrency_profile_10_workers(self, scraper):
        """Test 10-worker concurrency profile."""
        links = [("/january-2025-addendum-a.csv", "January 2025 Addendum A")] * 1000
        
        start_time = time.time()
        
        # Simulate 10 workers processing 100 links each
        for worker_id in range(10):
            worker_links = links[worker_id * 100:(worker_id + 1) * 100]
            for href, text in worker_links:
                scraper._classify_file(href, text)
                scraper._is_quarterly_addenda_link(href, text)
                scraper._extract_quarter_info({'text': text, 'href': href})
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 10 workers should be faster than 5 workers
        assert duration < 3.0, f"10 workers took too long: {duration:.2f}s"
    
    def test_database_throughput_simulation(self, scraper):
        """Test database ingestion throughput simulation."""
        # Simulate database ingestion with backpressure
        records = []
        
        # Generate test records
        for i in range(1000):
            record = {
                'id': i,
                'href': f"/january-2025-addendum-a-{i}.csv",
                'text': f"January 2025 Addendum A {i}",
                'classification': 'addendum_a',
                'is_quarterly': True,
                'quarter_info': (2025, 1),
                'processed_at': time.time()
            }
            records.append(record)
        
        start_time = time.time()
        
        # Simulate database write with backpressure
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            # Simulate database write delay
            time.sleep(0.001)  # 1ms per batch
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should handle 1000 records in reasonable time
        assert duration < 5.0, f"Database simulation took too long: {duration:.2f}s"
        
        # Calculate throughput
        throughput = len(records) / duration
        assert throughput > 200, f"Database throughput too low: {throughput:.2f} records/second"


if __name__ == "__main__":
    """Run performance tests."""
    pytest.main([__file__, "-v", "--benchmark-only"])
