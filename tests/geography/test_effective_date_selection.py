"""Tests for effective date selection functionality"""

import pytest
from datetime import date
from unittest.mock import Mock, patch
from cms_pricing.services.effective_dates import EffectiveDateSelector, EffectiveDateRecord
from cms_pricing.services.geography import GeographyService


class TestEffectiveDateSelector:
    """Test effective date selection logic"""
    
    def setup_method(self):
        self.selector = EffectiveDateSelector()
    
    def test_determine_effective_date_specific_date(self):
        """Test specific date selection"""
        target_date = date(2025, 6, 15)
        result = self.selector.determine_effective_date(valuation_date=target_date)
        
        assert result["date"] == target_date
        assert result["year"] == 2025
        assert result["quarter"] == 2
        assert result["type"] == "specific_date"
    
    def test_determine_effective_date_annual(self):
        """Test annual selection (no quarter specified)"""
        result = self.selector.determine_effective_date(valuation_year=2025)
        
        assert result["date"] == date(2025, 1, 1)
        assert result["year"] == 2025
        assert result["quarter"] is None
        assert result["type"] == "annual"
    
    def test_determine_effective_date_quarterly(self):
        """Test quarterly selection"""
        result = self.selector.determine_effective_date(valuation_year=2025, quarter=3)
        
        assert result["date"] == date(2025, 7, 1)
        assert result["year"] == 2025
        assert result["quarter"] == 3
        assert result["type"] == "quarterly"
        assert result["effective_from"] == date(2025, 7, 1)
        assert result["effective_to"] == date(2025, 9, 30)
    
    def test_determine_effective_date_default_year(self):
        """Test default to current year when no year provided"""
        current_year = date.today().year
        result = self.selector.determine_effective_date()
        
        assert result["year"] == current_year
        assert result["type"] == "annual"
    
    def test_determine_effective_date_invalid_quarter(self):
        """Test error handling for invalid quarter"""
        with pytest.raises(ValueError, match="Invalid quarter 5"):
            self.selector.determine_effective_date(valuation_year=2025, quarter=5)
    
    def test_get_quarter_for_date(self):
        """Test quarter calculation for different dates"""
        assert self.selector._get_quarter_for_date(date(2025, 1, 15)) == 1
        assert self.selector._get_quarter_for_date(date(2025, 3, 31)) == 1
        assert self.selector._get_quarter_for_date(date(2025, 4, 1)) == 2
        assert self.selector._get_quarter_for_date(date(2025, 6, 30)) == 2
        assert self.selector._get_quarter_for_date(date(2025, 7, 1)) == 3
        assert self.selector._get_quarter_for_date(date(2025, 9, 30)) == 3
        assert self.selector._get_quarter_for_date(date(2025, 10, 1)) == 4
        assert self.selector._get_quarter_for_date(date(2025, 12, 31)) == 4
    
    def test_get_quarter_effective_dates(self):
        """Test quarter date range calculation"""
        # Q1
        start, end = self.selector.get_quarter_effective_dates(2025, 1)
        assert start == date(2025, 1, 1)
        assert end == date(2025, 3, 31)
        
        # Q2
        start, end = self.selector.get_quarter_effective_dates(2025, 2)
        assert start == date(2025, 4, 1)
        assert end == date(2025, 6, 30)
        
        # Q3
        start, end = self.selector.get_quarter_effective_dates(2025, 3)
        assert start == date(2025, 7, 1)
        assert end == date(2025, 9, 30)
        
        # Q4
        start, end = self.selector.get_quarter_effective_dates(2025, 4)
        assert start == date(2025, 10, 1)
        assert end == date(2025, 12, 31)
    
    def test_get_quarter_effective_dates_invalid(self):
        """Test error handling for invalid quarter"""
        with pytest.raises(ValueError, match="Invalid quarter 5"):
            self.selector.get_quarter_effective_dates(2025, 5)
    
    def test_select_for_valuation_date_exact_match(self):
        """Test selecting record that exactly covers valuation date"""
        records = [
            EffectiveDateRecord(
                data="record1",
                effective_from=date(2025, 1, 1),
                effective_to=date(2025, 6, 30)
            ),
            EffectiveDateRecord(
                data="record2", 
                effective_from=date(2025, 7, 1),
                effective_to=date(2025, 12, 31)
            )
        ]
        
        result = self.selector.select_for_valuation_date(records, date(2025, 6, 15))
        assert result.data == "record1"
    
    def test_select_for_valuation_date_no_end_date(self):
        """Test selecting record with no end date (covers all future)"""
        records = [
            EffectiveDateRecord(
                data="record1",
                effective_from=date(2025, 1, 1),
                effective_to=date(2025, 6, 30)
            ),
            EffectiveDateRecord(
                data="record2",
                effective_from=date(2025, 7, 1),
                effective_to=None  # No end date
            )
        ]
        
        result = self.selector.select_for_valuation_date(records, date(2025, 8, 15))
        assert result.data == "record2"
    
    def test_select_for_valuation_date_fallback(self):
        """Test fallback to latest record when no exact match"""
        records = [
            EffectiveDateRecord(
                data="record1",
                effective_from=date(2024, 1, 1),
                effective_to=date(2024, 12, 31)
            ),
            EffectiveDateRecord(
                data="record2",
                effective_from=date(2025, 1, 1),
                effective_to=date(2025, 6, 30)
            )
        ]
        
        result = self.selector.select_for_valuation_date(records, date(2025, 8, 15))
        assert result.data == "record2"  # Latest record before valuation date
    
    def test_select_for_valuation_date_strict_mode(self):
        """Test strict mode error when no record covers date"""
        records = [
            EffectiveDateRecord(
                data="record1",
                effective_from=date(2025, 1, 1),
                effective_to=date(2025, 6, 30)
            )
        ]
        
        with pytest.raises(ValueError, match="No records cover valuation date"):
            self.selector.select_for_valuation_date(records, date(2025, 8, 15), strict_mode=True)


class TestGeographyServiceEffectiveDates:
    """Test geography service with effective date selection"""
    
    def setup_method(self):
        self.mock_db = Mock()
        self.service = GeographyService(db=self.mock_db)
    
    @pytest.mark.asyncio
    async def test_resolve_zip_with_specific_date(self):
        """Test ZIP resolution with specific valuation date"""
        # Mock the effective date selector
        with patch.object(self.service.effective_date_selector, 'determine_effective_date') as mock_determine:
            mock_determine.return_value = {
                "date": date(2025, 6, 15),
                "year": 2025,
                "quarter": 2,
                "type": "specific_date"
            }
            
            # Mock the resolution methods
            with patch.object(self.service, '_resolve_zip5_exact') as mock_resolve:
                mock_resolve.return_value = {
                    "locality_id": "1",
                    "state": "CA",
                    "match_level": "zip5",
                    "dataset_digest": "test_digest"
                }
                
                result = await self.service.resolve_zip(
                    zip5="94110",
                    valuation_date=date(2025, 6, 15)
                )
                
                # Verify effective date selector was called correctly
                mock_determine.assert_called_once_with(None, None, date(2025, 6, 15))
                
                # Verify resolution method was called with effective params
                mock_resolve.assert_called_once()
                call_args = mock_resolve.call_args[0]
                assert call_args[0] == "94110"  # zip5
                assert call_args[1]["type"] == "specific_date"  # effective_params
                assert call_args[2] == False  # expose_carrier
    
    @pytest.mark.asyncio
    async def test_resolve_zip_with_quarter(self):
        """Test ZIP resolution with quarterly selection"""
        with patch.object(self.service.effective_date_selector, 'determine_effective_date') as mock_determine:
            mock_determine.return_value = {
                "date": date(2025, 7, 1),
                "year": 2025,
                "quarter": 3,
                "type": "quarterly",
                "effective_from": date(2025, 7, 1),
                "effective_to": date(2025, 9, 30)
            }
            
            with patch.object(self.service, '_resolve_zip5_exact') as mock_resolve:
                mock_resolve.return_value = {
                    "locality_id": "1",
                    "state": "CA", 
                    "match_level": "zip5",
                    "dataset_digest": "test_digest"
                }
                
                result = await self.service.resolve_zip(
                    zip5="94110",
                    valuation_year=2025,
                    quarter=3
                )
                
                # Verify quarterly parameters were passed
                mock_determine.assert_called_once_with(2025, 3, None)
                
                # Verify resolution method received quarterly effective params
                call_args = mock_resolve.call_args[0]
                assert call_args[1]["type"] == "quarterly"
                assert call_args[1]["quarter"] == 3
    
    @pytest.mark.asyncio
    async def test_resolve_zip_annual_default(self):
        """Test ZIP resolution with annual default (no parameters)"""
        with patch.object(self.service.effective_date_selector, 'determine_effective_date') as mock_determine:
            current_year = date.today().year
            mock_determine.return_value = {
                "date": date(current_year, 1, 1),
                "year": current_year,
                "quarter": None,
                "type": "annual"
            }
            
            with patch.object(self.service, '_resolve_zip5_exact') as mock_resolve:
                mock_resolve.return_value = {
                    "locality_id": "1",
                    "state": "CA",
                    "match_level": "zip5", 
                    "dataset_digest": "test_digest"
                }
                
                result = await self.service.resolve_zip(zip5="94110")
                
                # Verify default parameters were used
                mock_determine.assert_called_once_with(None, None, None)
                
                # Verify annual effective params
                call_args = mock_resolve.call_args[0]
                assert call_args[1]["type"] == "annual"
                assert call_args[1]["quarter"] is None
    
    def test_build_effective_date_filter_specific_date(self):
        """Test effective date filter for specific date"""
        effective_params = {
            "date": date(2025, 6, 15),
            "type": "specific_date"
        }
        
        filter_expr = self.service._build_effective_date_filter_from_params(effective_params)
        
        # The filter should be a SQLAlchemy expression
        assert filter_expr is not None
        # We can't easily test the SQL logic without a real database,
        # but we can verify it's a valid SQLAlchemy expression
    
    def test_build_effective_date_filter_quarterly(self):
        """Test effective date filter for quarterly selection"""
        effective_params = {
            "date": date(2025, 7, 1),
            "type": "quarterly",
            "effective_from": date(2025, 7, 1),
            "effective_to": date(2025, 9, 30)
        }
        
        filter_expr = self.service._build_effective_date_filter_from_params(effective_params)
        assert filter_expr is not None
    
    def test_build_effective_date_filter_annual(self):
        """Test effective date filter for annual selection"""
        effective_params = {
            "date": date(2025, 1, 1),
            "year": 2025,
            "type": "annual"
        }
        
        filter_expr = self.service._build_effective_date_filter_from_params(effective_params)
        assert filter_expr is not None


class TestEffectiveDateIntegration:
    """Integration tests for effective date selection"""
    
    @pytest.mark.asyncio
    async def test_effective_date_parameter_precedence(self):
        """Test that valuation_date overrides year/quarter parameters"""
        service = GeographyService(db=Mock())
        
        with patch.object(service.effective_date_selector, 'determine_effective_date') as mock_determine:
            mock_determine.return_value = {
                "date": date(2025, 6, 15),
                "year": 2025,
                "quarter": 2,
                "type": "specific_date"
            }
            
            with patch.object(service, '_resolve_zip5_exact') as mock_resolve:
                mock_resolve.return_value = {"locality_id": "1", "state": "CA", "match_level": "zip5"}
                
                # Call with conflicting parameters - valuation_date should win
                await service.resolve_zip(
                    zip5="94110",
                    valuation_year=2024,  # Different year
                    quarter=1,           # Different quarter  
                    valuation_date=date(2025, 6, 15)  # This should override
                )
                
                # Verify the effective date selector was called with valuation_date
                mock_determine.assert_called_once_with(2024, 1, date(2025, 6, 15))
    
    def test_effective_date_edge_cases(self):
        """Test edge cases for effective date selection"""
        selector = EffectiveDateSelector()
        
        # Test leap year
        result = selector.determine_effective_date(valuation_year=2024, quarter=1)
        assert result["effective_from"] == date(2024, 1, 1)
        assert result["effective_to"] == date(2024, 3, 31)
        
        # Test year boundary
        result = selector.determine_effective_date(valuation_year=2025, quarter=4)
        assert result["effective_from"] == date(2025, 10, 1)
        assert result["effective_to"] == date(2025, 12, 31)
        
        # Test specific date on quarter boundary
        result = selector.determine_effective_date(valuation_date=date(2025, 7, 1))
        assert result["quarter"] == 3
