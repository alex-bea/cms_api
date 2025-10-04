#!/usr/bin/env python3
"""
OPPS Scraper Method Unit Tests
=============================

Comprehensive unit tests for OPPS scraper methods following QTS v1.1 standards.
Tests core scraper helpers with ≥90% coverage and comprehensive edge cases.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pathlib import Path

from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper


class TestOPPSScraperMethods:
    """Unit tests for OPPS scraper core methods."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper instance for testing."""
        return CMSOPPSScraper()
    
    # Test _extract_quarter_info method
    @pytest.mark.parametrize(
        "text,expected",
        [
            # Valid cases
            ("January 2025 Addendum A", {'year': 2025, 'quarter': 1}),
            ("Jan 2025 OPPS", {'year': 2025, 'quarter': 1}),
            ("  january  2025  ", {'year': 2025, 'quarter': 1}),
            ("JANUARY 2025", {'year': 2025, 'quarter': 1}),
            ("Apr 2025 OPPS", {'year': 2025, 'quarter': 2}),
            ("April 2025 Addendum B", {'year': 2025, 'quarter': 2}),
            ("july 2025", {'year': 2025, 'quarter': 3}),
            ("July 2025 Errata", {'year': 2025, 'quarter': 3}),
            ("Oct 2025", {'year': 2025, 'quarter': 4}),
            ("October 2025", {'year': 2025, 'quarter': 4}),
            ("Q1 2025", {'year': 2025, 'quarter': 1}),
            ("Q2 2025", {'year': 2025, 'quarter': 2}),
            ("Q3 2025", {'year': 2025, 'quarter': 3}),
            ("Q4 2025", {'year': 2025, 'quarter': 4}),
            ("First Quarter 2025", {'year': 2025, 'quarter': 1}),
            ("Second Quarter 2025", {'year': 2025, 'quarter': 2}),
            ("Third Quarter 2025", {'year': 2025, 'quarter': 3}),
            ("Fourth Quarter 2025", {'year': 2025, 'quarter': 4}),
            
            # Edge cases
            ("N/A", None),
            ("", None),
            (None, None),
            ("Invalid text", None),
            ("2025", None),  # No quarter indicator
            ("January", None),  # No year
            ("January 2025 Extra Text", {'year': 2025, 'quarter': 1}),  # Extra text should still work
            ("2025 January", {'year': 2025, 'quarter': 1}),  # Year first
            ("jan2025", {'year': 2025, 'quarter': 1}),  # No space
            ("JAN-2025", {'year': 2025, 'quarter': 1}),  # Dash separator
            
            # Non-English months (should fail gracefully)
            ("Enero 2025", None),
            ("Janvier 2025", {'year': 2025, 'quarter': 1}),  # Contains "jan" so matches Q1
            
            # Mixed case variations
            ("JaNuArY 2025", {'year': 2025, 'quarter': 1}),
            ("APRIL 2025", {'year': 2025, 'quarter': 2}),
            ("jUlY 2025", {'year': 2025, 'quarter': 3}),
            ("OcToBeR 2025", {'year': 2025, 'quarter': 4}),
        ],
    )
    def test_extract_quarter_info(self, scraper, text, expected):
        """Test quarter extraction from various text patterns."""
        result = scraper._extract_quarter_info({'text': text, 'href': ''})
        assert result == expected
    
    def test_extract_quarter_info_with_href(self, scraper):
        """Test quarter extraction when info is in href instead of text."""
        link_info = {
            'text': 'Some generic text',
            'href': '/january-2025-addendum'
        }
        result = scraper._extract_quarter_info(link_info)
        assert result == {'year': 2025, 'quarter': 1}
    
    def test_extract_quarter_info_combined_text_href(self, scraper):
        """Test quarter extraction when info is split between text and href."""
        link_info = {
            'text': 'April',
            'href': '/2025-addendum'
        }
        result = scraper._extract_quarter_info(link_info)
        assert result == {'year': 2025, 'quarter': 2}
    
    # Test _is_quarterly_addenda_link method
    @pytest.mark.parametrize(
        "href,text,expected",
        [
            # Positive cases (must have year pattern)
            ("/january-2025-addendum", "January 2025 Addendum A", True),
            ("/april-2025-addendum-b", "April 2025 Addendum B", True),
            ("/july-2025-addendum", "July 2025", True),
            ("/october-2025-addendum", "October 2025", True),
            ("/q1-2025-addendum", "Q1 2025", True),
            ("/addendum-a-2025", "Addendum A 2025", True),
            ("/addendum-b-2025", "Addendum B 2025", True),
            ("/opps-addendum-2025", "OPPS Addendum 2025", True),
            ("/2025-addendum", "2025 Addendum", True),
            
            # Negative cases
            ("/annual-report", "Annual Report", False),
            ("/errata", "Errata", False),
            ("/corrections", "Corrections", False),
            ("/general-info", "General Information", False),
            ("/policy-update", "Policy Update", False),
            ("/news", "News", False),
            ("/archive", "Archive", False),
            ("/quarterly-addendum", "Quarterly Addendum", False),  # No year pattern
            ("/addendum-a", "Addendum A", False),  # No year pattern
            ("/addendum-b", "Addendum B", False),  # No year pattern
            
            # Edge cases
            ("", "", False),
            ("/addendum", "", False),  # No year pattern
            ("", "Addendum", False),   # No year pattern
            ("/addendum-annual", "Annual Addendum", False),  # annual overrides
            ("/quarterly-annual", "Quarterly Annual", False),  # annual overrides
        ],
    )
    def test_is_quarterly_addenda_link(self, scraper, href, text, expected):
        """Test quarterly addenda link identification."""
        result = scraper._is_quarterly_addenda_link(href, text)
        assert result == expected
    
    def test_is_quarterly_addenda_link_case_insensitive(self, scraper):
        """Test that link identification is case insensitive."""
        assert scraper._is_quarterly_addenda_link("/ADDENDUM-2025", "ADDENDUM A 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-2025", "addendum a 2025") == True
        assert scraper._is_quarterly_addenda_link("/Addendum-2025", "Addendum A 2025") == True
    
    # Test _classify_file method
    @pytest.mark.parametrize(
        "href,text,expected",
        [
            # Addendum A files
            ("/january-2025-addendum-a.csv", "January 2025 Addendum A", "addendum_a"),
            ("/april-2025-addendum-a.xlsx", "April 2025 Addendum A", "addendum_a"),
            ("/july-2025-addendum-a.txt", "July 2025 Addendum A", "addendum_a"),
            ("/addendum-a.csv", "Addendum A", "addendum_a"),
            ("/opps-addendum-a.xlsx", "OPPS Addendum A", "addendum_a"),
            
            # Addendum B files
            ("/january-2025-addendum-b.csv", "January 2025 Addendum B", "addendum_b"),
            ("/april-2025-addendum-b.xlsx", "April 2025 Addendum B", "addendum_b"),
            ("/july-2025-addendum-b.txt", "July 2025 Addendum B", "addendum_b"),
            ("/addendum-b.csv", "Addendum B", "addendum_b"),
            ("/opps-addendum-b.xlsx", "OPPS Addendum B", "addendum_b"),
            
            # ZIP files
            ("/january-2025-addendum.zip", "January 2025 Addendum", "addendum_zip"),
            ("/april-2025-addendum.zip", "April 2025 Addendum", "addendum_zip"),
            ("/opps-addendum.zip", "OPPS Addendum", "addendum_zip"),
            ("/quarterly-addendum.zip", "Quarterly Addendum", "addendum_zip"),
            
            # Unknown/ambiguous files
            ("/general-info.pdf", "General Information", None),
            ("/readme.txt", "Readme", None),
            ("/data.csv", "Data", None),
            ("/unknown.xlsx", "Unknown", None),
            ("", "", None),
        ],
    )
    def test_classify_file(self, scraper, href, text, expected):
        """Test file type classification."""
        result = scraper._classify_file(href, text)
        assert result == expected
    
    def test_classify_file_case_insensitive(self, scraper):
        """Test that file classification is case insensitive."""
        assert scraper._classify_file("/ADDENDUM-A.CSV", "ADDENDUM A") == "addendum_a"
        assert scraper._classify_file("/addendum-b.xlsx", "addendum b") == "addendum_b"
        assert scraper._classify_file("/ADDENDUM.ZIP", "ADDENDUM") == "addendum_zip"
    
    # Test _resolve_disclaimer_url method
    @pytest.mark.asyncio
    async def test_resolve_disclaimer_url_direct_success(self, scraper):
        """Test direct URL resolution without disclaimer."""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.headers = {'content-type': 'application/zip'}
        mock_response.text = ''
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        
        result = await scraper._resolve_disclaimer_url(mock_client, "https://example.com/file.zip", "Test File")
        assert result == "https://example.com/file.zip"
    
    @pytest.mark.asyncio
    async def test_resolve_disclaimer_url_disclaimer_detected(self, scraper):
        """Test disclaimer detection and browser resolution."""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.text = 'disclaimer terms accept agreement'
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        
        # Mock the browser resolution to return a different URL
        async def mock_browser_resolution(*args, **kwargs):
            return "https://example.com/resolved.zip"
        
        with patch.object(scraper, '_handle_disclaimer_with_browser', side_effect=mock_browser_resolution):
            result = await scraper._resolve_disclaimer_url(mock_client, "https://example.com/license.asp", "Test File")
            assert result == "https://example.com/resolved.zip"
    
    @pytest.mark.asyncio
    async def test_resolve_disclaimer_url_disclaimer_fallback(self, scraper):
        """Test disclaimer resolution fallback when browser fails."""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.text = 'disclaimer terms accept agreement'
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        
        # Mock the browser resolution to fail (return original URL)
        async def mock_browser_fallback(*args, **kwargs):
            return "https://example.com/license.asp"
        
        with patch.object(scraper, '_handle_disclaimer_with_browser', side_effect=mock_browser_fallback):
            result = await scraper._resolve_disclaimer_url(mock_client, "https://example.com/license.asp", "Test File")
            assert result == "https://example.com/license.asp"
    
    @pytest.mark.asyncio
    async def test_resolve_disclaimer_url_http_error(self, scraper):
        """Test HTTP error handling."""
        mock_client = Mock()
        mock_client.get.side_effect = Exception("HTTP Error")
        
        result = await scraper._resolve_disclaimer_url(mock_client, "https://example.com/file.zip", "Test File")
        assert result == "https://example.com/file.zip"
    
    # Test _handle_disclaimer_with_browser method
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_success(self, scraper):
        """Test successful browser disclaimer acceptance."""
        mock_driver = Mock()
        mock_driver.current_url = "https://example.com/license.asp"
        mock_driver.page_source = "disclaimer page content"
        
        mock_button = Mock()
        mock_button.is_displayed.return_value = True
        mock_button.is_enabled.return_value = True
        mock_button.text = "Accept"
        
        mock_driver.find_element.return_value = mock_button
        mock_driver.get.return_value = None
        
        with patch('selenium.webdriver') as mock_webdriver:
            mock_webdriver.Chrome.return_value = mock_driver
            
            # Mock the wait condition to simulate successful redirect
            with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.WebDriverWait') as mock_wait:
                mock_wait.return_value.until.side_effect = [
                    None,  # First wait for page load
                    None,  # Second wait for redirect
                ]
                
                result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
                assert result == "https://example.com/license.asp"  # No redirect in this test
    
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_no_button(self, scraper):
        """Test browser disclaimer handling when no accept button is found."""
        mock_driver = Mock()
        mock_driver.current_url = "https://example.com/license.asp"
        mock_driver.page_source = "disclaimer page content"
        mock_driver.find_element.side_effect = Exception("No such element")
        mock_driver.get.return_value = None
        
        with patch('selenium.webdriver') as mock_webdriver:
            mock_webdriver.Chrome.return_value = mock_driver
            
            with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.WebDriverWait') as mock_wait:
                mock_wait.return_value.until.return_value = None
                
                result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
                assert result == "https://example.com/license.asp"
    
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_selenium_import_error(self, scraper):
        """Test browser disclaimer handling when Selenium is not available."""
        with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.webdriver', side_effect=ImportError("Selenium not available")):
            result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
            assert result == "https://example.com/license.asp"
    
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_timeout(self, scraper):
        """Test browser disclaimer handling with timeout."""
        mock_driver = Mock()
        mock_driver.current_url = "https://example.com/license.asp"
        mock_driver.page_source = "disclaimer page content"
        mock_driver.get.return_value = None
        
        with patch('selenium.webdriver') as mock_webdriver:
            mock_webdriver.Chrome.return_value = mock_driver
            
            with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.WebDriverWait') as mock_wait:
                from selenium.common.exceptions import TimeoutException
                mock_wait.return_value.until.side_effect = TimeoutException("Timeout")
                
                result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
                assert result == "https://example.com/license.asp"
    
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_chrome_fallback_firefox(self, scraper):
        """Test browser disclaimer handling with Chrome fallback to Firefox."""
        mock_driver = Mock()
        mock_driver.current_url = "https://example.com/license.asp"
        mock_driver.page_source = "disclaimer page content"
        mock_driver.get.return_value = None
        
        with patch('selenium.webdriver') as mock_webdriver:
            # Chrome fails, Firefox succeeds
            mock_webdriver.Chrome.side_effect = Exception("Chrome not available")
            mock_webdriver.Firefox.return_value = mock_driver
            
            with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.WebDriverWait') as mock_wait:
                mock_wait.return_value.until.return_value = None
                
                result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
                assert result == "https://example.com/license.asp"
    
    @pytest.mark.asyncio
    async def test_handle_disclaimer_with_browser_both_browsers_fail(self, scraper):
        """Test browser disclaimer handling when both Chrome and Firefox fail."""
        with patch('selenium.webdriver') as mock_webdriver:
            mock_webdriver.Chrome.side_effect = Exception("Chrome not available")
            mock_webdriver.Firefox.side_effect = Exception("Firefox not available")
            
            result = await scraper._handle_disclaimer_with_browser("https://example.com/license.asp", "Test File")
            assert result == "https://example.com/license.asp"
    
    # Test edge cases and error handling
    def test_extract_quarter_info_malformed_input(self, scraper):
        """Test quarter extraction with malformed input."""
        # Test with non-string input
        result = scraper._extract_quarter_info({'text': 12345, 'href': ''})
        assert result is None
        
        # Test with very long string
        long_text = "January " + "x" * 1000 + " 2025"
        result = scraper._extract_quarter_info({'text': long_text, 'href': ''})
        assert result == {'year': 2025, 'quarter': 1}
    
    def test_is_quarterly_addenda_link_special_characters(self, scraper):
        """Test quarterly addenda link identification with special characters."""
        assert scraper._is_quarterly_addenda_link("/addendum-a%20file-2025", "Addendum A File 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-a+file-2025", "Addendum A File 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-a_file-2025", "Addendum A File 2025") == True
    
    def test_classify_file_special_characters(self, scraper):
        """Test file classification with special characters."""
        assert scraper._classify_file("/addendum-a%20file.csv", "Addendum A File") == "addendum_a"
        assert scraper._classify_file("/addendum-b+file.xlsx", "Addendum B File") == "addendum_b"
        assert scraper._classify_file("/addendum_file.zip", "Addendum File") == "addendum_zip"
    
    # Test regex patterns
    def test_quarterly_release_patterns(self, scraper):
        """Test that quarterly release patterns are correctly defined."""
        patterns = scraper.quarterly_release_patterns
        
        # Test Q1 patterns
        assert patterns['q1'].search("January 2025")
        assert patterns['q1'].search("jan 2025")
        assert patterns['q1'].search("JANUARY 2025")
        assert patterns['q1'].search("first quarter")
        assert not patterns['q1'].search("April 2025")
        
        # Test Q2 patterns
        assert patterns['q2'].search("April 2025")
        assert patterns['q2'].search("apr 2025")
        assert patterns['q2'].search("APRIL 2025")
        assert patterns['q2'].search("second quarter")
        assert not patterns['q2'].search("January 2025")
        
        # Test Q3 patterns
        assert patterns['q3'].search("July 2025")
        assert patterns['q3'].search("jul 2025")
        assert patterns['q3'].search("JULY 2025")
        assert patterns['q3'].search("third quarter")
        assert not patterns['q3'].search("April 2025")
        
        # Test Q4 patterns
        assert patterns['q4'].search("October 2025")
        assert patterns['q4'].search("oct 2025")
        assert patterns['q4'].search("OCTOBER 2025")
        assert patterns['q4'].search("fourth quarter")
        assert not patterns['q4'].search("July 2025")
    
    def test_file_patterns(self, scraper):
        """Test that file patterns are correctly defined."""
        patterns = scraper.file_patterns
        
        # Test addendum A patterns
        assert patterns['addendum_a'].search("addendum a.csv")
        assert patterns['addendum_a'].search("ADDENDUM A.XLSX")
        assert patterns['addendum_a'].search("addendum-a.txt")
        assert not patterns['addendum_a'].search("addendum b.csv")
        
        # Test addendum B patterns
        assert patterns['addendum_b'].search("addendum b.csv")
        assert patterns['addendum_b'].search("ADDENDUM B.XLSX")
        assert patterns['addendum_b'].search("addendum-b.txt")
        assert not patterns['addendum_b'].search("addendum a.csv")
        
        # Test ZIP patterns
        assert patterns['zip_files'].search("file.zip")
        assert patterns['zip_files'].search("FILE.ZIP")
        assert patterns['zip_files'].search("file.gz")
        assert not patterns['zip_files'].search("file.csv")
    
    # Test initialization
    def test_scraper_initialization(self):
        """Test scraper initialization with default and custom parameters."""
        # Test default initialization
        scraper1 = CMSOPPSScraper()
        assert scraper1.base_url == "https://www.cms.gov"
        assert scraper1.output_dir == Path("data/scraped/opps")
        assert scraper1.opps_base_url == "https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient"
        assert scraper1.quarterly_addenda_url == "https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates"
        
        # Test custom initialization
        custom_output = Path("/custom/output")
        scraper2 = CMSOPPSScraper(output_dir=custom_output)
        assert scraper2.output_dir == custom_output
        
        custom_base = "https://custom.cms.gov"
        scraper3 = CMSOPPSScraper(base_url=custom_base)
        assert scraper3.base_url == custom_base


class TestOPPSScraperEdgeCases:
    """Additional edge case tests for OPPS scraper."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper instance for testing."""
        return CMSOPPSScraper()
    
    def test_extract_quarter_info_boundary_years(self, scraper):
        """Test quarter extraction with boundary years."""
        # Test very old year
        result = scraper._extract_quarter_info({'text': 'January 2000', 'href': ''})
        assert result == {'year': 2000, 'quarter': 1}
        
        # Test very future year
        result = scraper._extract_quarter_info({'text': 'January 2030', 'href': ''})
        assert result == {'year': 2030, 'quarter': 1}
        
        # Test invalid year format
        result = scraper._extract_quarter_info({'text': 'January 25', 'href': ''})
        assert result is None
    
    def test_is_quarterly_addenda_link_unicode(self, scraper):
        """Test quarterly addenda link identification with Unicode characters."""
        assert scraper._is_quarterly_addenda_link("/addendum-a-é-2025.csv", "Addendum A é 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-b-ñ-2025.xlsx", "Addendum B ñ 2025") == True
    
    def test_classify_file_unicode(self, scraper):
        """Test file classification with Unicode characters."""
        assert scraper._classify_file("/addendum-a-é.csv", "Addendum A é") == "addendum_a"
        assert scraper._classify_file("/addendum-b-ñ.xlsx", "Addendum B ñ") == "addendum_b"
    
    def test_extract_quarter_info_whitespace_variations(self, scraper):
        """Test quarter extraction with various whitespace patterns."""
        test_cases = [
            ("\tJanuary\t2025\t", {'year': 2025, 'quarter': 1}),
            ("\nJanuary\n2025\n", {'year': 2025, 'quarter': 1}),
            ("\r\nJanuary\r\n2025\r\n", {'year': 2025, 'quarter': 1}),
            ("January\u00A02025", {'year': 2025, 'quarter': 1}),  # Non-breaking space
            ("January\u20032025", {'year': 2025, 'quarter': 1}),  # Em space
        ]
        
        for text, expected in test_cases:
            result = scraper._extract_quarter_info({'text': text, 'href': ''})
            assert result == expected, f"Failed for text: {repr(text)}"
    
    def test_is_quarterly_addenda_link_html_entities(self, scraper):
        """Test quarterly addenda link identification with HTML entities."""
        assert scraper._is_quarterly_addenda_link("/addendum-a&amp;b-2025.csv", "Addendum A&amp;B 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-a&lt;b-2025.csv", "Addendum A&lt;B 2025") == True
        assert scraper._is_quarterly_addenda_link("/addendum-a&gt;b-2025.csv", "Addendum A&gt;B 2025") == True
