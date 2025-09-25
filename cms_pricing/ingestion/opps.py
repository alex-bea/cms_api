"""OPPS (Outpatient Prospective Payment System) data ingestion"""

import pandas as pd
import httpx
from typing import Dict, Any, Optional
from datetime import date
import zipfile
import io

from cms_pricing.ingestion.base import BaseIngester
import structlog

logger = structlog.get_logger()


class OPPSIngester(BaseIngester):
    """Ingester for Outpatient Prospective Payment System data"""
    
    def get_dataset_id(self) -> str:
        return "OPPS"
    
    async def fetch_data(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch OPPS data from CMS"""
        
        # OPPS is quarterly data
        if not quarter:
            quarter = "1"  # Default to Q1
            
        logger.info("Fetching OPPS data", year=valuation_year, quarter=quarter)
        
        # Real CMS URLs for OPPS data
        urls = {
            "addendum_b": f"https://www.cms.gov/files/zip/opps-addendum-b-{valuation_year}q{quarter}.zip",
            "wage_index": f"https://www.cms.gov/files/zip/opps-wage-index-{valuation_year}q{quarter}.zip",
            "addendum_d1": f"https://www.cms.gov/files/zip/opps-addendum-d1-{valuation_year}q{quarter}.zip"
        }
        
        fetched_data = {}
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for data_type, url in urls.items():
                try:
                    logger.info("Fetching OPPS data", type=data_type, url=url)
                    
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        # Extract ZIP file contents
                        zip_content = zipfile.ZipFile(io.BytesIO(response.content))
                        
                        # Find CSV files in the ZIP
                        csv_files = [f for f in zip_content.namelist() if f.endswith('.csv')]
                        
                        if csv_files:
                            # Use the first CSV file found
                            csv_file = csv_files[0]
                            csv_content = zip_content.read(csv_file).decode('utf-8')
                            
                            filename = f"{data_type}_{valuation_year}q{quarter}.csv"
                            fetched_data[filename] = csv_content
                            
                            logger.info("Successfully fetched OPPS data", type=data_type, rows=len(csv_content.split('\n')))
                        else:
                            logger.warning("No CSV files found in ZIP", type=data_type, url=url)
                            fetched_data[f"{data_type}_{valuation_year}q{quarter}.csv"] = self._create_placeholder_data(data_type)
                    else:
                        logger.warning(
                            "Failed to fetch OPPS data",
                            type=data_type,
                            status_code=response.status_code,
                            url=url
                        )
                        fetched_data[f"{data_type}_{valuation_year}q{quarter}.csv"] = self._create_placeholder_data(data_type)
                        
                except Exception as e:
                    logger.error(
                        "Error fetching OPPS data",
                        type=data_type,
                        error=str(e),
                        url=url
                    )
                    # Create placeholder data for development
                    fetched_data[f"{data_type}_{valuation_year}q{quarter}.csv"] = self._create_placeholder_data(data_type)
        
        return fetched_data
    
    def normalize_data(
        self,
        raw_data: Dict[str, Any],
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """Normalize OPPS data into DataFrames"""
        
        normalized = {}
        
        # Process Addendum B data (main OPPS rates)
        addendum_file = f"addendum_b_{valuation_year}q{quarter}.csv"
        if addendum_file in raw_data:
            try:
                df = pd.read_csv(io.StringIO(raw_data[addendum_file]))
                
                # Normalize column names (CMS uses different formats)
                column_mapping = {
                    'HCPCS': 'hcpcs',
                    'HCPCS Code': 'hcpcs',
                    'Status Indicator': 'status_indicator',
                    'Status Ind': 'status_indicator',
                    'APC': 'apc',
                    'APC Code': 'apc',
                    'National Unadj Rate': 'national_unadj_rate',
                    'National Unadj': 'national_unadj_rate',
                    'National Rate': 'national_unadj_rate',
                    'Packaging Flag': 'packaging_flag',
                    'Packaging': 'packaging_flag'
                }
                
                df = df.rename(columns=column_mapping)
                
                # Ensure required columns exist
                required_columns = ['hcpcs', 'status_indicator', 'national_unadj_rate']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    logger.warning("Missing required columns in Addendum B", missing=missing_columns)
                    # Create placeholder columns
                    for col in missing_columns:
                        if col == 'national_unadj_rate':
                            df[col] = 0.0
                        else:
                            df[col] = ''
                
                # Clean and validate data
                df = df.dropna(subset=['hcpcs'])  # Remove rows without HCPCS codes
                df['hcpcs'] = df['hcpcs'].astype(str).str.strip()
                df['national_unadj_rate'] = pd.to_numeric(df['national_unadj_rate'], errors='coerce').fillna(0.0)
                
                # Add metadata columns
                df['year'] = valuation_year
                df['quarter'] = quarter
                df['effective_from'] = f"{valuation_year}-01-01"
                df['effective_to'] = None
                
                normalized['fee_opps'] = df
                
                logger.info("Processed Addendum B data", rows=len(df), columns=list(df.columns))
                
            except Exception as e:
                logger.error("Error processing Addendum B data", error=str(e))
                # Create empty DataFrame as fallback
                normalized['fee_opps'] = pd.DataFrame(columns=[
                    'hcpcs', 'status_indicator', 'apc', 'national_unadj_rate', 
                    'packaging_flag', 'year', 'quarter', 'effective_from', 'effective_to'
                ])
        
        # Process wage index data
        wage_file = f"wage_index_{valuation_year}q{quarter}.csv"
        if wage_file in raw_data:
            try:
                df = pd.read_csv(io.StringIO(raw_data[wage_file]))
                
                # Normalize column names
                column_mapping = {
                    'CBSA': 'cbsa',
                    'CBSA Code': 'cbsa',
                    'Wage Index': 'wage_index',
                    'Wage Ind': 'wage_index',
                    'Wage Index Value': 'wage_index'
                }
                
                df = df.rename(columns=column_mapping)
                
                # Ensure required columns exist
                if 'wage_index' not in df.columns:
                    logger.warning("Missing wage_index column in wage index data")
                    df['wage_index'] = 1.0
                
                # Clean and validate data
                df = df.dropna(subset=['cbsa'])
                df['cbsa'] = df['cbsa'].astype(str).str.strip()
                df['wage_index'] = pd.to_numeric(df['wage_index'], errors='coerce').fillna(1.0)
                
                # Add metadata columns
                df['year'] = valuation_year
                df['quarter'] = quarter
                df['effective_from'] = f"{valuation_year}-01-01"
                df['effective_to'] = None
                
                normalized['wage_index'] = df
                
                logger.info("Processed wage index data", rows=len(df), columns=list(df.columns))
                
            except Exception as e:
                logger.error("Error processing wage index data", error=str(e))
                # Create empty DataFrame as fallback
                normalized['wage_index'] = pd.DataFrame(columns=[
                    'cbsa', 'wage_index', 'year', 'quarter', 'effective_from', 'effective_to'
                ])
        
        # Process Addendum D1 data (APC information)
        addendum_d1_file = f"addendum_d1_{valuation_year}q{quarter}.csv"
        if addendum_d1_file in raw_data:
            try:
                df = pd.read_csv(io.StringIO(raw_data[addendum_d1_file]))
                
                # This contains APC-specific information
                # For now, we'll just log that we found it
                logger.info("Found Addendum D1 data", rows=len(df))
                
                # You could process this further if needed
                
            except Exception as e:
                logger.error("Error processing Addendum D1 data", error=str(e))
        
        return normalized
    
    def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> list[str]:
        """Validate OPPS data"""
        
        warnings = []
        
        for table_name, df in normalized_data.items():
            if df.empty:
                warnings.append(f"Table {table_name} is empty")
                continue
            
            logger.info(f"Validating {table_name}", rows=len(df), columns=list(df.columns))
            
            # Check for required columns
            if table_name == 'fee_opps':
                required_cols = ['hcpcs', 'status_indicator', 'national_unadj_rate']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    warnings.append(f"Missing required columns in {table_name}: {missing_cols}")
                
                # Check for valid HCPCS codes
                invalid_hcpcs = df[~df['hcpcs'].str.match(r'^[A-Z0-9]{5}$', na=False)]
                if len(invalid_hcpcs) > 0:
                    warnings.append(f"Found {len(invalid_hcpcs)} invalid HCPCS codes in {table_name}")
                
                # Check for negative rates
                negative_rates = df[df['national_unadj_rate'] < 0]
                if len(negative_rates) > 0:
                    warnings.append(f"Found {len(negative_rates)} negative rates in {table_name}")
                
                # Check status indicators
                valid_status_indicators = ['A', 'B', 'C', 'E', 'G', 'H', 'J1', 'J2', 'K', 'L', 'M', 'N', 'P', 'Q1', 'Q2', 'Q3', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y']
                invalid_status = df[~df['status_indicator'].isin(valid_status_indicators)]
                if len(invalid_status) > 0:
                    warnings.append(f"Found {len(invalid_status)} invalid status indicators in {table_name}")
            
            elif table_name == 'wage_index':
                required_cols = ['cbsa', 'wage_index']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    warnings.append(f"Missing required columns in {table_name}: {missing_cols}")
                
                # Check for valid CBSA codes
                invalid_cbsa = df[~df['cbsa'].str.match(r'^\d{5}$', na=False)]
                if len(invalid_cbsa) > 0:
                    warnings.append(f"Found {len(invalid_cbsa)} invalid CBSA codes in {table_name}")
                
                # Check wage index values
                invalid_wage_index = df[(df['wage_index'] <= 0) | (df['wage_index'] > 2.0)]
                if len(invalid_wage_index) > 0:
                    warnings.append(f"Found {len(invalid_wage_index)} invalid wage index values in {table_name}")
        
        return warnings
    
    def _create_placeholder_data(self, data_type: str) -> str:
        """Create placeholder data for development"""
        
        if data_type == "addendum_b":
            return """HCPCS,Status Indicator,APC,National Unadj Rate,Packaging Flag
99213,A,0601,125.50,
99214,A,0602,185.75,
99215,A,0603,275.25,
27447,S,5134,8500.00,
27448,S,5134,9200.00,
80053,N,0341,15.75,J1
80061,N,0341,18.25,J1
80069,N,0341,22.50,J1
93000,A,0371,45.00,
93010,A,0372,35.50,
93015,A,0373,28.75"""
        
        elif data_type == "wage_index":
            return """CBSA,CBSA Name,Wage Index
41860,San Francisco-Oakland-Berkeley CA,1.2345
19100,Dallas-Fort Worth-Arlington TX,0.9876
31080,Los Angeles-Long Beach-Anaheim CA,1.1234
35620,New York-Newark-Jersey City NY-NJ-PA,1.3456
16980,Chicago-Naperville-Elgin IL-IN-WI,1.0987
12060,Atlanta-Sandy Springs-Alpharetta GA,0.8765
14460,Boston-Cambridge-Newton MA-NH,1.1567
19820,Detroit-Warren-Dearborn MI,0.9543
26420,Houston-The Woodlands-Sugar Land TX,0.9123
47900,Washington-Arlington-Alexandria DC-VA-MD-WV,1.1876"""
        
        elif data_type == "addendum_d1":
            return """APC,APC Title,Relative Weight,Payment Rate
0601,Level 1 Clinic Visit,1.0000,125.50
0602,Level 2 Clinic Visit,1.4800,185.75
0603,Level 3 Clinic Visit,2.1900,275.25
5134,Knee Procedures,67.7300,8500.00
0341,Comprehensive Metabolic Panel,0.1250,15.75
0371,Electrocardiogram,0.3580,45.00
0372,Electrocardiogram with Interpretation,0.2830,35.50
0373,Electrocardiogram Monitoring,0.2290,28.75"""
        
        return ""
