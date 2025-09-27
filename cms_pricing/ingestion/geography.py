"""Geography data ingestion for ZIP to locality/CBSA mapping"""

import pandas as pd
import httpx
import io
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import date

from cms_pricing.ingestion.base import BaseIngester
from cms_pricing.ingestion.cms_downloader import CMSDownloader
from cms_pricing.ingestion.zip_handler import ZIPHandler
import structlog

logger = structlog.get_logger()


class GeographyIngester(BaseIngester):
    """Ingester for ZIP to locality/CBSA mapping data"""
    
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.downloader = CMSDownloader()
        self.zip_handler = ZIPHandler()
    
    def get_dataset_id(self) -> str:
        return "GEOGRAPHY"
    
    async def fetch_data(
        self,
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch geography data from CMS"""
        
        # Geography data is typically annual, quarter is ignored
        logger.info("Fetching real geography data", year=valuation_year)
        
        try:
            # Download real CMS data
            download_result = await self.downloader.download_dataset(
                "geography", valuation_year
            )
            
            if not download_result["success"]:
                logger.warning(
                    "Failed to download real geography data, using sample data",
                    year=valuation_year,
                    errors=download_result
                )
                return self._create_sample_data_dict(valuation_year)
            
            # Extract and process ZIP files
            fetched_data = {}
            
            for file_type, download_info in download_result["files"].items():
                if download_info["success"]:
                    zip_path = Path(download_info["local_path"])
                    
                    # Extract ZIP file
                    extract_result = self.zip_handler.process_zip_file(zip_path)
                    
                    if extract_result["success"]:
                        # Find and process CSV files
                        extract_dir = Path(extract_result["extract_to"])
                        csv_files = self.zip_handler.identify_csv_files(extract_dir)
                        
                        for csv_info in csv_files:
                            csv_path = Path(csv_info["path"])
                            logger.info("Processing CSV file", csv_file=str(csv_path), file_type=csv_info.get("file_type"))
                            
                            # Read file based on type
                            try:
                                if csv_info.get("file_type") == "zip_list":
                                    # Handle ZIP list text file
                                    df = self._process_zip_list_file(csv_path)
                                    csv_content = df.to_csv(index=False)
                                    
                                    logger.info(
                                        "Processed real geography ZIP list",
                                        file_type=file_type,
                                        filename=csv_info["filename"],
                                        rows=len(df),
                                        columns=len(df.columns)
                                    )
                                elif csv_info.get("file_type") == "zip5_fixed_width":
                                    # Handle ZIP5 fixed-width format
                                    df = self._process_zip5_fixed_width(csv_path)
                                    csv_content = df.to_csv(index=False)
                                    
                                    logger.info(
                                        "Processed real geography ZIP5 fixed-width",
                                        file_type=file_type,
                                        filename=csv_info["filename"],
                                        rows=len(df),
                                        columns=len(df.columns)
                                    )
                                elif csv_info.get("file_type") == "zip9_fixed_width":
                                    # Handle ZIP9 fixed-width format
                                    df = self._process_zip9_fixed_width(csv_path)
                                    csv_content = df.to_csv(index=False)
                                    
                                    logger.info(
                                        "Processed real geography ZIP9 fixed-width",
                                        file_type=file_type,
                                        filename=csv_info["filename"],
                                        rows=len(df),
                                        columns=len(df.columns)
                                    )
                                else:
                                    # Handle regular CSV file
                                    df = self.zip_handler.read_csv_file(csv_path)
                                    csv_content = df.to_csv(index=False)
                                    
                                    logger.info(
                                        "Processed real geography CSV",
                                        file_type=file_type,
                                        filename=csv_info["filename"],
                                        rows=len(df),
                                        columns=len(df.columns)
                                    )
                                
                                # Use the specific file type from CSV info, not the download file type
                                specific_file_type = csv_info.get("file_type", file_type)
                                filename = f"geography_{specific_file_type}_{valuation_year}.csv"
                                fetched_data[filename] = csv_content
                                
                                logger.info(
                                    "Stored geography file",
                                    specific_file_type=specific_file_type,
                                    filename=filename,
                                    rows=len(df)
                                )
                                
                            except Exception as e:
                                logger.error(
                                    "Error reading geography file",
                                    file_type=file_type,
                                    csv_file=str(csv_path),
                                    error=str(e),
                                    exc_info=True
                                )
            
            # If no real data was processed, fall back to sample data
            if not fetched_data:
                logger.warning("No real geography data processed, using sample data")
                return self._create_sample_data_dict(valuation_year)
            
            logger.info("Final fetched data files", files=list(fetched_data.keys()), total_files=len(fetched_data))
            return fetched_data
            
        except Exception as e:
            logger.error(
                "Error fetching real geography data",
                error=str(e),
                year=valuation_year
            )
            # Fall back to sample data
            return self._create_sample_data_dict(valuation_year)
    
    def _process_zip_list_file(self, file_path: Path) -> pd.DataFrame:
        """Process a ZIP list text file (one ZIP code per line)"""
        
        # Read ZIP codes from text file
        zip_codes = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                zip_code = line.strip()
                if zip_code and zip_code.isdigit() and len(zip_code) == 5:
                    zip_codes.append(zip_code)
        
        # Create DataFrame with default locality information
        # These ZIP codes require ZIP+4 extensions, so we'll set them up for that
        df = pd.DataFrame({
            'zip5': zip_codes,
            'plus4': None,  # Will be filled when ZIP+4 is provided
            'locality_id': '01',  # Default locality - will need to be resolved
            'locality_name': 'Requires ZIP+4 Extension',
            'state': 'XX',  # Will need to be resolved
            'carrier': None,
            'rural_flag': None,
            'has_plus4': 0,  # These require ZIP+4 but don't have it yet
            'requires_zip_plus4': 1  # Flag to indicate this ZIP needs ZIP+4
        })
        
        logger.info(
            "Processed ZIP list file",
            file=str(file_path),
            zip_count=len(zip_codes),
            sample_zips=zip_codes[:5]
        )
        
        return df

    def _process_zip5_fixed_width(self, file_path: Path) -> pd.DataFrame:
        """Process ZIP5 fixed-width format file"""
        
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.rstrip('\n\r')
                if len(line) >= 80:  # Ensure we have enough characters
                    record = {
                        'state': line[0:2].strip(),
                        'zip5': line[2:7].strip(),
                        'carrier': line[7:12].strip(),
                        'locality_id': line[12:14].strip(),
                        'rural_flag': line[14:15].strip() if line[14:15].strip() else None,
                        'bene_lab_cb_locality': line[15:17].strip(),
                        'rural_flag2': line[17:18].strip() if line[17:18].strip() else None,
                        'plus_four_flag': line[20:21].strip() if line[20:21].strip() else '0',
                        'part_b_payment_indicator': line[22:23].strip() if line[22:23].strip() else None,
                        'year_quarter': line[75:80].strip()
                    }
                    records.append(record)
        
        df = pd.DataFrame(records)
        
        # Clean and validate data
        df['zip5'] = df['zip5'].str.zfill(5)  # Ensure 5 digits
        df['has_plus4'] = (df['plus_four_flag'] == '1').astype(int)
        df['plus4'] = None  # ZIP5 file doesn't have +4 data
        df['locality_name'] = 'Unknown Locality'  # Will need to be resolved
        df['effective_from'] = f"{df['year_quarter'].iloc[0][:4]}-01-01" if not df.empty else "2025-01-01"
        df['effective_to'] = None
        df['dataset_id'] = 'ZIP5_LOCALITY'
        df['dataset_digest'] = 'unknown'
        df['created_at'] = date.today()
        
        logger.info(
            "Processed ZIP5 fixed-width file",
            file=str(file_path),
            records=len(df),
            sample_records=df.head(3).to_dict('records')
        )
        
        return df

    def _process_zip9_fixed_width(self, file_path: Path) -> pd.DataFrame:
        """Process ZIP9 fixed-width format file"""
        
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.rstrip('\n\r')
                if len(line) >= 80:  # Ensure we have enough characters
                    record = {
                        'state': line[0:2].strip(),
                        'zip5': line[2:7].strip(),
                        'carrier': line[7:12].strip(),
                        'locality_id': line[12:14].strip(),
                        'rural_flag': line[14:15].strip() if line[14:15].strip() else None,
                        'plus_four_flag': line[20:21].strip() if line[20:21].strip() else '0',
                        'plus4': line[21:25].strip() if line[21:25].strip() else None,
                        'part_b_payment_indicator': line[31:32].strip() if line[31:32].strip() else None,
                        'year_quarter': line[75:80].strip()
                    }
                    records.append(record)
        
        df = pd.DataFrame(records)
        
        # Clean and validate data
        df['zip5'] = df['zip5'].str.zfill(5)  # Ensure 5 digits
        df['plus4'] = df['plus4'].str.zfill(4)  # Ensure 4 digits for +4
        df['has_plus4'] = (df['plus_four_flag'] == '1').astype(int)
        df['locality_name'] = 'Unknown Locality'  # Will need to be resolved
        df['effective_from'] = f"{df['year_quarter'].iloc[0][:4]}-01-01" if not df.empty else "2025-01-01"
        df['effective_to'] = None
        df['dataset_id'] = 'ZIP9_LOCALITY'
        df['dataset_digest'] = 'unknown'
        df['created_at'] = date.today()
        
        logger.info(
            "Processed ZIP9 fixed-width file",
            file=str(file_path),
            records=len(df),
            sample_records=df.head(3).to_dict('records')
        )
        
        return df

    def _create_sample_data_dict(self, year: int) -> Dict[str, Any]:
        """Create sample data in the same format as real data"""
        return {
            f"geography_zip_locality_{year}.csv": self._create_sample_data()
        }
    
    def normalize_data(
        self,
        raw_data: Dict[str, Any],
        valuation_year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """Normalize geography data into DataFrames"""
        
        normalized = {}
        
        # Process geography data files
        logger.info("Processing raw data files", file_count=len(raw_data), files=list(raw_data.keys()))
        for filename, csv_content in raw_data.items():
            if filename.startswith("geography_") and filename.endswith(".csv"):
                logger.info("Processing geography file", filename=filename)
                try:
                    df = pd.read_csv(io.StringIO(csv_content))
                    
                    # Normalize column names per PRD section 8.3
                    column_mapping = {
                        # ZIP code columns (ZIP+4-first per PRD)
                        'ZIP5': 'zip5', 'zip5': 'zip5', 'zip_code': 'zip5', 'zip': 'zip5',
                        'ZIP+4': 'zip_plus4', 'zip_plus4': 'zip_plus4', 'plus4': 'plus4',
                        'ZIP9': 'zip_plus4', 'zip9': 'zip_plus4', 'full_zip': 'zip_plus4',
                        # Locality columns
                        'locality_id': 'locality_id', 'locality': 'locality_id', 'locality_code': 'locality_id',
                        'locality_name': 'locality_name', 'locality_desc': 'locality_name',
                        # State columns
                        'state_code': 'state', 'state': 'state', 'st': 'state',
                        # Carrier/MAC columns per PRD
                        'carrier': 'carrier', 'mac': 'carrier', 'carrier_id': 'carrier',
                        # Rural flag columns per PRD
                        'rural_flag': 'rural_flag', 'rural': 'rural_flag', 'rural_indicator': 'rural_flag'
                    }
                    
                    # Apply column mapping
                    df = df.rename(columns=column_mapping)
                    
                    # Ensure required columns exist with defaults per PRD
                    required_columns = {
                        'zip5': None,  # Required
                        'plus4': None,  # Optional ZIP+4 add-on
                        'locality_id': '01',  # Default locality
                        'locality_name': 'Unknown Locality',
                        'state': 'XX',  # Default state
                        'carrier': None,  # Optional MAC/Carrier
                        'rural_flag': None  # Optional rural indicator
                    }
                    
                    for col, default_value in required_columns.items():
                        if col not in df.columns:
                            df[col] = default_value
                    
                    # Clean and validate data per PRD section 8.4
                    df = self._clean_geography_data(df)
                    
                    # Add ZIP+4 handling per PRD
                    df = self._process_zip_plus4(df)
                    
                    # Add metadata columns per PRD section 8.4
                    df['effective_from'] = f"{valuation_year}-01-01"
                    df['effective_to'] = None
                    df['dataset_id'] = 'ZIP_LOCALITY'
                    df['dataset_digest'] = self.manifest.get('digest', 'unknown')
                    df['created_at'] = date.today()
                    
                    # Store in normalized data
                    if 'geography' in normalized:
                        # Combine with existing data
                        normalized['geography'] = pd.concat([normalized['geography'], df], ignore_index=True)
                    else:
                        normalized['geography'] = df
                    
                    logger.info(
                        "Normalized geography data",
                        filename=filename,
                        rows=len(df),
                        columns=len(df.columns)
                    )
                    
                except Exception as e:
                    logger.error(
                        "Error normalizing geography data",
                        filename=filename,
                        error=str(e)
                    )
        
        # Remove duplicates if we combined multiple files
        if 'geography' in normalized:
            initial_count = len(normalized['geography'])
            
            # Debug: Check plus4 values before deduplication
            plus4_stats = normalized['geography']['plus4'].value_counts()
            logger.info("Plus4 value statistics", plus4_unique=len(plus4_stats), plus4_null=plus4_stats.get(None, 0), plus4_sample=plus4_stats.head().to_dict())
            
            # Debug: Check zip5 + plus4 combinations
            combo_stats = normalized['geography'].groupby(['zip5', 'plus4']).size()
            logger.info("ZIP5+Plus4 combinations", unique_combos=len(combo_stats), max_duplicates=combo_stats.max(), sample_combos=combo_stats.head().to_dict())
            
            # Deduplicate on zip5 + plus4 to preserve ZIP+4 granularity per PRD
            normalized['geography'] = normalized['geography'].drop_duplicates(subset=['zip5', 'plus4'], keep='first')
            final_count = len(normalized['geography'])
            
            if initial_count != final_count:
                logger.info(
                    "Removed duplicate geography records",
                    initial_count=initial_count,
                    final_count=final_count,
                    duplicates_removed=initial_count - final_count
                )
            else:
                logger.info(
                    "No duplicates found in geography data",
                    record_count=final_count
                )
        
        return normalized
    
    def _clean_geography_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate geography data per PRD section 8.4"""
        
        # Clean ZIP codes (remove non-numeric characters, ensure 5 digits)
        if 'zip5' in df.columns:
            df['zip5'] = df['zip5'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df['zip5'] = df['zip5'].str.zfill(5)
            # Remove invalid ZIP codes
            df = df[df['zip5'].str.len() == 5]
        
        # Clean locality_id (ensure string format)
        if 'locality_id' in df.columns:
            df['locality_id'] = df['locality_id'].astype(str).str.strip()
        
        # Clean state codes (ensure 2 characters, uppercase)
        if 'state' in df.columns:
            df['state'] = df['state'].astype(str).str.strip().str.upper()
            df['state'] = df['state'].str[:2]  # Take first 2 characters
        
        # Clean carrier codes
        if 'carrier' in df.columns:
            df['carrier'] = df['carrier'].astype(str).str.strip().str.upper()
        
        # Clean rural flag (R, B, or blank per PRD)
        if 'rural_flag' in df.columns:
            df['rural_flag'] = df['rural_flag'].astype(str).str.strip().str.upper()
            # Only keep valid rural flags: R, B, or empty
            df['rural_flag'] = df['rural_flag'].where(df['rural_flag'].isin(['R', 'B', '', 'NAN']), '')
            df['rural_flag'] = df['rural_flag'].replace(['', 'NAN'], None)
        
        return df
    
    def _process_zip_plus4(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process ZIP+4 data per PRD section 18 (ZIP+4 normalization)"""
        
        # Handle ZIP+4 parsing per PRD section 18.2
        if 'zip_plus4' in df.columns:
            # Parse ZIP+4 from various formats (94110-1234 or 941101234)
            df['zip_plus4'] = df['zip_plus4'].astype(str)
            
            # Split into ZIP5 and plus4 components
            zip_plus4_split = df['zip_plus4'].str.extract(r'(\d{5})-?(\d{0,4})')
            df['zip5'] = zip_plus4_split[0].str.zfill(5)
            df['plus4'] = zip_plus4_split[1].str.zfill(4)
            
            # Set has_plus4 flag per PRD section 7.1
            df['has_plus4'] = (df['plus4'].notna() & (df['plus4'] != '0000')).astype(int)
            
            # Clean plus4 - only keep valid 4-digit values
            df['plus4'] = df['plus4'].where(df['plus4'].str.len() == 4, None)
        
        else:
            # No ZIP+4 data - set defaults
            df['plus4'] = None
            df['has_plus4'] = 0
        
        # Ensure ZIP5 is always present and valid
        if 'zip5' not in df.columns or df['zip5'].isna().all():
            raise ValueError("ZIP5 data is required per PRD")
        
        # Clean ZIP5 (ensure 5 digits, preserve leading zeros per PRD section 18.1)
        df['zip5'] = df['zip5'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['zip5'] = df['zip5'].str.zfill(5)
        
        # Remove invalid records
        df = df[df['zip5'].str.len() == 5]
        
        return df
    
    def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Validate geography data and return warnings"""
        warnings = []
        
        if 'geography' in normalized_data:
            df = normalized_data['geography']
            
            # Check required columns
            required_cols = ['zip5', 'locality_id', 'cbsa', 'state_code']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                warnings.append(f"Missing required columns: {missing_cols}")
            
            # Check ZIP code format (5 digits)
            if 'zip5' in df.columns:
                invalid_zips = df[~df['zip5'].astype(str).str.match(r'^\d{5}$')]
                if len(invalid_zips) > 0:
                    warnings.append(f"Invalid ZIP codes found: {len(invalid_zips)}")
            
            # Check CBSA format (5 digits)
            if 'cbsa' in df.columns:
                invalid_cbsas = df[~df['cbsa'].astype(str).str.match(r'^\d{5}$')]
                if len(invalid_cbsas) > 0:
                    warnings.append(f"Invalid CBSA codes found: {len(invalid_cbsas)}")
            
            # Check population share range (0.0 to 1.0)
            if 'population_share' in df.columns:
                invalid_shares = df[(df['population_share'] < 0.0) | (df['population_share'] > 1.0)]
                if len(invalid_shares) > 0:
                    warnings.append(f"Invalid population shares found: {len(invalid_shares)}")
            
            # Check for duplicates
            if 'zip5' in df.columns:
                duplicates = df[df.duplicated(subset=['zip5'], keep=False)]
                if len(duplicates) > 0:
                    warnings.append(f"Duplicate ZIP codes found: {len(duplicates)}")
        
        return warnings
    
    def _create_sample_data(self) -> str:
        """Create sample geography data for development"""
        return """zip5,locality_id,locality_name,cbsa,cbsa_name,county_fips,state_code,population_share,is_rural_dmepos
94110,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94102,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94103,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94104,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94105,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94107,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94108,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94109,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94111,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
94112,01,Rest of California,41860,San Francisco-Oakland-Berkeley CA,06075,CA,1.0,false
10001,01,Manhattan,35620,New York-Newark-Jersey City NY-NJ-PA,36061,NY,1.0,false
10002,01,Manhattan,35620,New York-Newark-Jersey City NY-NJ-PA,36061,NY,1.0,false
10003,01,Manhattan,35620,New York-Newark-Jersey City NY-NJ-PA,36061,NY,1.0,false
10004,01,Manhattan,35620,New York-Newark-Jersey City NY-NJ-PA,36061,NY,1.0,false
10005,01,Manhattan,35620,New York-Newark-Jersey City NY-NJ-PA,36061,NY,1.0,false
60601,01,Chicago,16980,Chicago-Naperville-Elgin IL-IN-WI,17031,IL,1.0,false
60602,01,Chicago,16980,Chicago-Naperville-Elgin IL-IN-WI,17031,IL,1.0,false
60603,01,Chicago,16980,Chicago-Naperville-Elgin IL-IN-WI,17031,IL,1.0,false
60604,01,Chicago,16980,Chicago-Naperville-Elgin IL-IN-WI,17031,IL,1.0,false
60605,01,Chicago,16980,Chicago-Naperville-Elgin IL-IN-WI,17031,IL,1.0,false
75201,01,Dallas,19100,Dallas-Fort Worth-Arlington TX,48113,TX,1.0,false
75202,01,Dallas,19100,Dallas-Fort Worth-Arlington TX,48113,TX,1.0,false
75203,01,Dallas,19100,Dallas-Fort Worth-Arlington TX,48113,TX,1.0,false
75204,01,Dallas,19100,Dallas-Fort Worth-Arlington TX,48113,TX,1.0,false
75205,01,Dallas,19100,Dallas-Fort Worth-Arlington TX,48113,TX,1.0,false
30301,01,Atlanta,12060,Atlanta-Sandy Springs-Alpharetta GA,13089,GA,1.0,false
30302,01,Atlanta,12060,Atlanta-Sandy Springs-Alpharetta GA,13089,GA,1.0,false
30303,01,Atlanta,12060,Atlanta-Sandy Springs-Alpharetta GA,13089,GA,1.0,false
30304,01,Atlanta,12060,Atlanta-Sandy Springs-Alpharetta GA,13089,GA,1.0,false
30305,01,Atlanta,12060,Atlanta-Sandy Springs-Alpharetta GA,13089,GA,1.0,false
"""
