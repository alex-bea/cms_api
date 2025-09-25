"""MPFS data ingestion"""

import pandas as pd
import httpx
from typing import Dict, Any, Optional
from datetime import date

from cms_pricing.ingestion.base import BaseIngester
import structlog

logger = structlog.get_logger()


class MPFSIngester(BaseIngester):
    """Ingester for Medicare Physician Fee Schedule data"""
    
    def get_dataset_id(self) -> str:
        return "MPFS"
    
    async def fetch_data(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch MPFS data from CMS"""
        
        # MPFS is annual data, so quarter is ignored
        logger.info("Fetching MPFS data", year=valuation_year)
        
        # URLs for MPFS data (these would be actual CMS URLs)
        base_url = "https://www.cms.gov/files/zip"
        
        urls = {
            "rvu_data": f"{base_url}/mpfs-rvu-{valuation_year}.zip",
            "gpci_data": f"{base_url}/gpci-{valuation_year}.zip",
            "cf_data": f"{base_url}/conversion-factor-{valuation_year}.zip"
        }
        
        fetched_data = {}
        
        async with httpx.AsyncClient() as client:
            for data_type, url in urls.items():
                try:
                    logger.info("Fetching MPFS data", type=data_type, url=url)
                    
                    # In a real implementation, this would download and extract ZIP files
                    # For now, we'll create placeholder data
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        # Save raw data
                        filename = f"{data_type}_{valuation_year}.csv"
                        fetched_data[filename] = response.text
                    else:
                        logger.warning(
                            "Failed to fetch MPFS data",
                            type=data_type,
                            status_code=response.status_code
                        )
                        
                except Exception as e:
                    logger.error(
                        "Error fetching MPFS data",
                        type=data_type,
                        error=str(e)
                    )
                    # Create placeholder data for development
                    fetched_data[f"{data_type}_{valuation_year}.csv"] = self._create_placeholder_data(data_type)
        
        return fetched_data
    
    def normalize_data(
        self,
        raw_data: Dict[str, Any],
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """Normalize MPFS data into DataFrames"""
        
        normalized = {}
        
        # Process RVU data
        rvu_file = f"rvu_data_{valuation_year}.csv"
        if rvu_file in raw_data:
            df = pd.read_csv(rvu_file)
            # Normalize column names and types
            df = df.rename(columns={
                'HCPCS': 'hcpcs',
                'Work RVU': 'work_rvu',
                'PE NF RVU': 'pe_nf_rvu',
                'PE FAC RVU': 'pe_fac_rvu',
                'MP RVU': 'mp_rvu',
                'Global Days': 'global_days',
                'Status Indicator': 'status_indicator'
            })
            
            # Add metadata columns
            df['year'] = valuation_year
            df['revision'] = 'A'  # Default revision
            df['effective_from'] = f"{valuation_year}-01-01"
            df['effective_to'] = None
            
            normalized['fee_mpfs'] = df
        
        # Process GPCI data
        gpci_file = f"gpci_data_{valuation_year}.csv"
        if gpci_file in raw_data:
            df = pd.read_csv(gpci_file)
            df = df.rename(columns={
                'Locality': 'locality_id',
                'Locality Name': 'locality_name',
                'Work GPCI': 'gpci_work',
                'PE GPCI': 'gpci_pe',
                'MP GPCI': 'gpci_mp'
            })
            
            df['year'] = valuation_year
            df['effective_from'] = f"{valuation_year}-01-01"
            df['effective_to'] = None
            
            normalized['gpci'] = df
        
        # Process conversion factor data
        cf_file = f"cf_data_{valuation_year}.csv"
        if cf_file in raw_data:
            df = pd.read_csv(cf_file)
            df = df.rename(columns={
                'Conversion Factor': 'cf',
                'Source': 'source'
            })
            
            df['year'] = valuation_year
            df['effective_from'] = f"{valuation_year}-01-01"
            df['effective_to'] = None
            
            normalized['conversion_factors'] = df
        
        return normalized
    
    def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> list[str]:
        """Validate MPFS data"""
        
        warnings = []
        
        for table_name, df in normalized_data.items():
            if df.empty:
                warnings.append(f"Table {table_name} is empty")
                continue
            
            # Check for required columns
            if table_name == 'fee_mpfs':
                required_cols = ['hcpcs', 'work_rvu', 'pe_nf_rvu', 'pe_fac_rvu', 'mp_rvu']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    warnings.append(f"Missing required columns in {table_name}: {missing_cols}")
            
            # Check for null values in critical columns
            if 'hcpcs' in df.columns:
                null_hcpcs = df['hcpcs'].isnull().sum()
                if null_hcpcs > 0:
                    warnings.append(f"Found {null_hcpcs} null HCPCS codes in {table_name}")
        
        return warnings
    
    def _create_placeholder_data(self, data_type: str) -> str:
        """Create placeholder data for development"""
        
        if data_type == "rvu_data":
            return """HCPCS,Work RVU,PE NF RVU,PE FAC RVU,MP RVU,Global Days,Status Indicator
99213,1.30,1.17,0.00,0.09,0,A
99214,2.60,1.17,0.00,0.09,0,A
99215,3.85,1.17,0.00,0.09,0,A"""
        
        elif data_type == "gpci_data":
            return """Locality,Locality Name,Work GPCI,PE GPCI,MP GPCI
01,Rest of California,1.000,1.000,1.000
02,Los Angeles County,1.000,1.000,1.000
03,Orange County,1.000,1.000,1.000"""
        
        elif data_type == "cf_data":
            return """Conversion Factor,Source
34.6062,MPFS"""
        
        return ""
