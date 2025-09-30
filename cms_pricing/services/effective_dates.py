"""Effective date handling for CMS data selection"""

from typing import List, Optional, TypeVar, Generic, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass
import structlog

logger = structlog.get_logger(__name__)

T = TypeVar('T')


@dataclass
class EffectiveDateRecord(Generic[T]):
    """Record with effective date range"""
    data: T
    effective_from: date
    effective_to: Optional[date]
    dataset_digest: Optional[str] = None


class EffectiveDateSelector:
    """Selects records based on effective date ranges and valuation date"""
    
    def __init__(self):
        self.logger = logger.bind(service="effective_dates")
    
    def select_for_valuation_date(
        self, 
        records: List[EffectiveDateRecord[T]], 
        valuation_date: date,
        strict_mode: bool = False
    ) -> Optional[EffectiveDateRecord[T]]:
        """
        Select the most recent record whose effective window covers the valuation date.
        
        Args:
            records: List of records with effective date ranges
            valuation_date: Date for which to select data
            strict_mode: If True, error when no record covers the date
            
        Returns:
            Selected record or None if no suitable record found
        """
        if not records:
            self.logger.warning("No records provided for selection")
            return None
        
        # Filter records that cover the valuation date
        covering_records = []
        for record in records:
            if self._covers_date(record, valuation_date):
                covering_records.append(record)
        
        if not covering_records:
            if strict_mode:
                raise ValueError(f"No records cover valuation date {valuation_date}")
            
            # Fallback: use latest record with effective_from <= valuation_date
            fallback_records = [
                r for r in records 
                if r.effective_from <= valuation_date
            ]
            if fallback_records:
                selected = max(fallback_records, key=lambda r: r.effective_from)
                self.logger.warning(
                    "Using fallback record for valuation date",
                    valuation_date=valuation_date,
                    selected_effective_from=selected.effective_from,
                    selected_effective_to=selected.effective_to
                )
                return selected
            else:
                self.logger.error(
                    "No suitable records found for valuation date",
                    valuation_date=valuation_date
                )
                return None
        
        # Select the most recent record (latest effective_from)
        selected = max(covering_records, key=lambda r: r.effective_from)
        
        self.logger.info(
            "Selected record for valuation date",
            valuation_date=valuation_date,
            selected_effective_from=selected.effective_from,
            selected_effective_to=selected.effective_to,
            total_candidates=len(covering_records)
        )
        
        return selected
    
    def _covers_date(self, record: EffectiveDateRecord[T], valuation_date: date) -> bool:
        """Check if a record's effective window covers the valuation date"""
        if record.effective_from > valuation_date:
            return False
        
        if record.effective_to is None:
            return True  # No end date, covers all future dates
        
        return record.effective_to >= valuation_date
    
    def get_effective_windows_for_year(self, year: int) -> dict:
        """
        Get standard effective date windows for different CMS dataset types.
        
        Args:
            year: Calendar year
            
        Returns:
            Dictionary mapping dataset types to effective date windows
        """
        return {
            "ZIP_LOCALITY_Q1": {
                "effective_from": date(year, 1, 1),
                "effective_to": date(year, 3, 31)
            },
            "ZIP_LOCALITY_Q2": {
                "effective_from": date(year, 4, 1),
                "effective_to": date(year, 6, 30)
            },
            "ZIP_LOCALITY_Q3": {
                "effective_from": date(year, 7, 1),
                "effective_to": date(year, 9, 30)
            },
            "ZIP_LOCALITY_Q4": {
                "effective_from": date(year, 10, 1),
                "effective_to": date(year, 12, 31)
            },
            "GPCI_ANNUAL": {
                "effective_from": date(year, 1, 1),
                "effective_to": date(year, 12, 31)
            },
            "CONVERSION_FACTOR_ANNUAL": {
                "effective_from": date(year, 1, 1),
                "effective_to": date(year, 12, 31)
            },
            "MPFS_ANNUAL": {
                "effective_from": date(year, 1, 1),
                "effective_to": date(year, 12, 31)
            }
        }
    
    def parse_year_quarter(self, year_quarter: int) -> tuple[int, int]:
        """
        Parse year_quarter format (e.g., 20254 = 2025 Q4).
        
        Args:
            year_quarter: Integer in format YYYYQ
            
        Returns:
            Tuple of (year, quarter)
        """
        year = year_quarter // 10
        quarter = year_quarter % 10
        
        if quarter < 1 or quarter > 4:
            raise ValueError(f"Invalid quarter {quarter} in year_quarter {year_quarter}")
        
        return year, quarter
    
    def get_quarter_effective_dates(self, year: int, quarter: int) -> tuple[date, date]:
        """
        Get effective date range for a specific quarter.
        
        Args:
            year: Calendar year
            quarter: Quarter (1-4)
            
        Returns:
            Tuple of (effective_from, effective_to)
        """
        quarter_starts = {
            1: (1, 1),    # Jan 1
            2: (4, 1),    # Apr 1
            3: (7, 1),    # Jul 1
            4: (10, 1)    # Oct 1
        }
        
        quarter_ends = {
            1: (3, 31),   # Mar 31
            2: (6, 30),   # Jun 30
            3: (9, 30),   # Sep 30
            4: (12, 31)   # Dec 31
        }
        
        if quarter not in quarter_starts:
            raise ValueError(f"Invalid quarter {quarter}")
        
        start_month, start_day = quarter_starts[quarter]
        end_month, end_day = quarter_ends[quarter]
        
        effective_from = date(year, start_month, start_day)
        effective_to = date(year, end_month, end_day)
        
        return effective_from, effective_to
    
    def determine_effective_date(
        self, 
        valuation_year: Optional[int] = None, 
        quarter: Optional[int] = None, 
        valuation_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Determine effective date parameters based on input options.
        
        Args:
            valuation_year: Year for effective date selection
            quarter: Optional quarter (1-4) for effective date selection
            valuation_date: Specific date for effective date selection (overrides year/quarter)
            
        Returns:
            Dictionary with effective date parameters
            
        Raises:
            ValueError: If parameters are invalid or conflicting
        """
        if valuation_date:
            # Specific date provided - use it directly
            return {
                "date": valuation_date,
                "year": valuation_date.year,
                "quarter": self._get_quarter_for_date(valuation_date),
                "type": "specific_date"
            }
        
        if valuation_year is None:
            # Default to current year
            valuation_year = date.today().year
        
        if quarter is None:
            # Annual selection - covers all quarters
            return {
                "date": date(valuation_year, 1, 1),  # Start of year
                "year": valuation_year,
                "quarter": None,
                "type": "annual"
            }
        
        # Quarterly selection
        if quarter < 1 or quarter > 4:
            raise ValueError(f"Invalid quarter {quarter}. Must be 1-4.")
        
        effective_from, effective_to = self.get_quarter_effective_dates(valuation_year, quarter)
        
        return {
            "date": effective_from,  # Start of quarter
            "year": valuation_year,
            "quarter": quarter,
            "type": "quarterly",
            "effective_from": effective_from,
            "effective_to": effective_to
        }
    
    def _get_quarter_for_date(self, target_date: date) -> int:
        """Get quarter number for a given date"""
        month = target_date.month
        if month <= 3:
            return 1
        elif month <= 6:
            return 2
        elif month <= 9:
            return 3
        else:
            return 4