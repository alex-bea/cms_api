"""ZIP file extraction and handling for CMS data"""

import zipfile
import csv
import io
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import structlog

logger = structlog.get_logger()


class ZIPHandler:
    """Handles extraction and parsing of CMS ZIP files"""
    
    def __init__(self, zip_dir: str = "./data/cms_raw"):
        self.zip_dir = Path(zip_dir)
    
    def extract_zip(self, zip_path: Path, extract_to: Path = None) -> Dict[str, Any]:
        """Extract a ZIP file and return information about contents"""
        
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        if extract_to is None:
            extract_to = zip_path.parent / f"{zip_path.stem}_extracted"
        
        extract_to.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                
                for file_name in file_list:
                    # Skip directories
                    if file_name.endswith('/'):
                        continue
                    
                    # Extract file
                    zip_ref.extract(file_name, extract_to)
                    
                    extracted_path = extract_to / file_name
                    extracted_files.append({
                        "filename": file_name,
                        "path": str(extracted_path),
                        "size_bytes": extracted_path.stat().st_size
                    })
                
                logger.info(
                    "ZIP file extracted successfully",
                    zip_file=str(zip_path),
                    extract_to=str(extract_to),
                    files_extracted=len(extracted_files)
                )
                
                return {
                    "success": True,
                    "zip_file": str(zip_path),
                    "extract_to": str(extract_to),
                    "files": extracted_files
                }
                
        except Exception as e:
            logger.error(
                "Error extracting ZIP file",
                zip_file=str(zip_path),
                error=str(e)
            )
            return {
                "success": False,
                "zip_file": str(zip_path),
                "error": str(e)
            }
    
    def identify_csv_files(self, extracted_dir: Path) -> List[Dict[str, Any]]:
        """Identify and analyze CSV and text files in extracted directory"""
        
        print("METHOD CALLED!")
        csv_files = []
        print(f"Starting CSV file identification in {extracted_dir}")
        
        # Look for both CSV and text files
        for pattern in ["*.csv", "*.txt"]:
            print(f"Processing pattern: {pattern}")
            files = list(extracted_dir.rglob(pattern))
            print(f"Found {len(files)} files for pattern {pattern}")
            
            for file_path in files:
                print(f"Processing file: {file_path.name}")
                try:
                    # Try to read the first few rows to understand structure
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        # Read first few lines
                        lines = [f.readline().strip() for _ in range(5)]
                        f.seek(0)
                        
                        # For text files, check if it's ZIP data or layout files
                        if file_path.suffix.lower() == '.txt':
                            # Check if first line looks like a ZIP code (5 digits)
                            if lines[0] and lines[0].isdigit() and len(lines[0]) == 5:
                                csv_info = {
                                    "filename": file_path.name,
                                    "path": str(file_path),
                                    "size_bytes": file_path.stat().st_size,
                                    "delimiter": None,
                                    "headers": ["zip5"],
                                    "header_count": 1,
                                    "sample_lines": lines[:3],
                                    "file_type": "zip_list"
                                }
                                csv_files.append(csv_info)
                            # Check if it's a fixed-width ZIP data file
                            elif "ZIP5_" in file_path.name or "ZIP9_" in file_path.name:
                                # This is a fixed-width ZIP data file
                                file_type = "zip5_fixed_width" if "ZIP5_" in file_path.name else "zip9_fixed_width"
                                csv_info = {
                                    "filename": file_path.name,
                                    "path": str(file_path),
                                    "size_bytes": file_path.stat().st_size,
                                    "delimiter": None,
                                    "headers": ["state", "zip5", "carrier", "locality_id", "rural_flag"],
                                    "header_count": 5,
                                    "sample_lines": lines[:3],
                                    "file_type": file_type
                                }
                                csv_files.append(csv_info)
                            # Check if it's a layout file
                            elif "lyout" in file_path.name.lower():
                                # Skip layout files - they're documentation
                                pass
                            else:
                                # Other text files - try to process as CSV
                                pass
                        else:
                            # Non-text files - try to process as CSV
                            pass
                        
                        # Only process as CSV if we haven't already processed it as a special text file
                        is_special_text = (file_path.suffix.lower() == '.txt' and 
                                          ((lines[0] and lines[0].isdigit() and len(lines[0]) == 5) or
                                           "ZIP5_" in file_path.name or "ZIP9_" in file_path.name or
                                           "lyout" in file_path.name.lower()))
                        
                        if not is_special_text:
                            
                            # Try to detect delimiter for CSV files
                            sample = f.read(1024)
                            f.seek(0)
                            
                            delimiter = ','
                            if '\t' in sample:
                                delimiter = '\t'
                            elif '|' in sample:
                                delimiter = '|'
                            
                            # Read as CSV to get column names
                            f.seek(0)
                            reader = csv.reader(f, delimiter=delimiter)
                            headers = next(reader, [])
                            
                            csv_info = {
                                "filename": file_path.name,
                                "path": str(file_path),
                                "size_bytes": file_path.stat().st_size,
                                "delimiter": delimiter,
                                "headers": headers,
                                "header_count": len(headers),
                                "sample_lines": lines[:3],
                                "file_type": "csv"
                            }
                            
                            csv_files.append(csv_info)
                    
                except Exception as e:
                    logger.warning(
                        "Error analyzing CSV file",
                        file=str(file_path),
                        error=str(e)
                    )
        
        return csv_files
    
    def read_csv_file(
        self,
        csv_path: Path,
        delimiter: str = ',',
        encoding: str = 'utf-8',
        max_rows: Optional[int] = None
    ) -> pd.DataFrame:
        """Read a CSV file into a pandas DataFrame"""
        
        try:
            # Try different encodings if UTF-8 fails
            encodings = [encoding, 'latin-1', 'cp1252', 'iso-8859-1']
            
            for enc in encodings:
                try:
                    df = pd.read_csv(
                        csv_path,
                        delimiter=delimiter,
                        encoding=enc,
                        nrows=max_rows,
                        low_memory=False
                    )
                    
                    logger.info(
                        "CSV file read successfully",
                        file=str(csv_path),
                        encoding=enc,
                        rows=len(df),
                        columns=len(df.columns)
                    )
                    
                    return df
                    
                except UnicodeDecodeError:
                    continue
            
            raise Exception("Could not read file with any encoding")
            
        except Exception as e:
            logger.error(
                "Error reading CSV file",
                file=str(csv_path),
                error=str(e)
            )
            raise
    
    def process_zip_file(
        self,
        zip_path: Path,
        extract_to: Path = None,
        analyze_csv: bool = True
    ) -> Dict[str, Any]:
        """Complete processing of a ZIP file: extract and analyze"""
        
        # Extract the ZIP file
        extract_result = self.extract_zip(zip_path, extract_to)
        
        if not extract_result["success"]:
            return extract_result
        
        extract_dir = Path(extract_result["extract_to"])
        result = extract_result.copy()
        
        # Analyze CSV files if requested
        if analyze_csv:
            csv_files = self.identify_csv_files(extract_dir)
            result["csv_files"] = csv_files
            
            # Add summary
            result["csv_summary"] = {
                "total_csv_files": len(csv_files),
                "total_columns": sum(csv["header_count"] for csv in csv_files),
                "largest_file": max(csv_files, key=lambda x: x["size_bytes"]) if csv_files else None
            }
        
        return result
    
    def batch_process_zips(
        self,
        zip_pattern: str = "*.zip",
        extract_base_dir: Path = None
    ) -> Dict[str, Any]:
        """Process multiple ZIP files in batch"""
        
        if extract_base_dir is None:
            extract_base_dir = self.zip_dir / "extracted"
        
        zip_files = list(self.zip_dir.glob(zip_pattern))
        
        if not zip_files:
            logger.warning("No ZIP files found", pattern=zip_pattern, directory=str(self.zip_dir))
            return {
                "success": False,
                "error": "No ZIP files found",
                "pattern": zip_pattern,
                "directory": str(self.zip_dir)
            }
        
        results = {}
        
        for zip_file in zip_files:
            try:
                # Create unique extract directory for each ZIP
                extract_dir = extract_base_dir / zip_file.stem
                
                result = self.process_zip_file(zip_file, extract_dir)
                results[zip_file.name] = result
                
            except Exception as e:
                logger.error(
                    "Error processing ZIP file",
                    zip_file=str(zip_file),
                    error=str(e)
                )
                results[zip_file.name] = {
                    "success": False,
                    "error": str(e)
                }
        
        # Summary
        successful = sum(1 for r in results.values() if r.get("success", False))
        
        return {
            "success": successful > 0,
            "total_files": len(zip_files),
            "successful": successful,
            "failed": len(zip_files) - successful,
            "results": results
        }
