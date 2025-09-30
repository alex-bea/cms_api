"""
DIS-Compliant Data Adapters
Following Data Ingestion Standard PRD v1.0

This module provides header mapping, type conversion, and data adaptation
capabilities for different data sources and formats.
"""

import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Callable, Union
import structlog

logger = structlog.get_logger()


@dataclass
class ColumnMapping:
    """Column mapping specification"""
    source_name: str
    target_name: str
    data_type: str
    transform_func: Optional[Callable] = None
    required: bool = True
    default_value: Any = None


@dataclass
class AdapterConfig:
    """Configuration for data adapter"""
    source_format: str  # "csv", "excel", "fixed_width", "json"
    encoding: str = "utf-8"
    delimiter: str = ","
    quote_char: str = '"'
    skip_rows: int = 0
    header_row: int = 0
    column_mappings: List[ColumnMapping] = None
    data_type_overrides: Dict[str, str] = None


class DataAdapter(ABC):
    """Base class for data adapters"""
    
    def __init__(self, config: AdapterConfig):
        self.config = config
        self.column_mappings = {m.source_name: m for m in (config.column_mappings or [])}
    
    @abstractmethod
    def adapt(self, raw_data: bytes) -> pd.DataFrame:
        """Adapt raw data to standardized format"""
        pass
    
    def _apply_column_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply column name mappings and transformations"""
        if not self.column_mappings:
            return df
        
        # Rename columns
        rename_map = {}
        for mapping in self.column_mappings.values():
            if mapping.source_name in df.columns:
                rename_map[mapping.source_name] = mapping.target_name
        
        df = df.rename(columns=rename_map)
        
        # Apply transformations
        for mapping in self.column_mappings.values():
            if mapping.target_name in df.columns and mapping.transform_func:
                try:
                    df[mapping.target_name] = df[mapping.target_name].apply(mapping.transform_func)
                except Exception as e:
                    logger.warning(f"Failed to apply transform for {mapping.target_name}: {e}")
        
        # Set data types
        for mapping in self.column_mappings.values():
            if mapping.target_name in df.columns:
                try:
                    df[mapping.target_name] = df[mapping.target_name].astype(mapping.data_type)
                except Exception as e:
                    logger.warning(f"Failed to set data type for {mapping.target_name}: {e}")
        
        return df
    
    def _handle_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add missing required columns with default values"""
        for mapping in self.column_mappings.values():
            if mapping.required and mapping.target_name not in df.columns:
                logger.warning(f"Missing required column {mapping.target_name}, using default value")
                df[mapping.target_name] = mapping.default_value
        
        return df


class CSVAdapter(DataAdapter):
    """Adapter for CSV data"""
    
    def adapt(self, raw_data: bytes) -> pd.DataFrame:
        """Parse CSV data and apply mappings"""
        import io
        
        try:
            df = pd.read_csv(
                io.BytesIO(raw_data),
                encoding=self.config.encoding,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char,
                skiprows=self.config.skip_rows,
                header=self.config.header_row
            )
            
            # Apply column mappings
            df = self._apply_column_mappings(df)
            
            # Handle missing columns
            df = self._handle_missing_columns(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse CSV data: {e}")
            raise


class ExcelAdapter(DataAdapter):
    """Adapter for Excel data"""
    
    def adapt(self, raw_data: bytes) -> pd.DataFrame:
        """Parse Excel data and apply mappings"""
        import io
        
        try:
            df = pd.read_excel(
                io.BytesIO(raw_data),
                skiprows=self.config.skip_rows,
                header=self.config.header_row
            )
            
            # Apply column mappings
            df = self._apply_column_mappings(df)
            
            # Handle missing columns
            df = self._handle_missing_columns(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse Excel data: {e}")
            raise


class FixedWidthAdapter(DataAdapter):
    """Adapter for fixed-width data"""
    
    def __init__(self, config: AdapterConfig, layout: Dict[str, Dict[str, Any]]):
        super().__init__(config)
        self.layout = layout
    
    def adapt(self, raw_data: bytes) -> pd.DataFrame:
        """Parse fixed-width data and apply mappings"""
        import io
        
        try:
            lines = raw_data.decode(self.config.encoding).split('\n')
            records = []
            
            for line in lines:
                line = line.rstrip('\n\r')
                if len(line) < max(pos['end'] for pos in self.layout.values()):
                    continue
                
                record = {}
                for field_name, field_spec in self.layout.items():
                    start = field_spec['start']
                    end = field_spec['end']
                    value = line[start:end].strip()
                    
                    # Convert data type
                    if field_spec.get('type') == 'int' and value:
                        try:
                            value = int(value)
                        except ValueError:
                            value = None
                    elif field_spec.get('type') == 'float' and value:
                        try:
                            value = float(value)
                        except ValueError:
                            value = None
                    elif field_spec.get('type') == 'bool' and value:
                        value = value.upper() in ['Y', '1', 'T', 'TRUE']
                    
                    record[field_name] = value
                
                records.append(record)
            
            df = pd.DataFrame(records)
            
            # Apply column mappings
            df = self._apply_column_mappings(df)
            
            # Handle missing columns
            df = self._handle_missing_columns(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse fixed-width data: {e}")
            raise


class JSONAdapter(DataAdapter):
    """Adapter for JSON data"""
    
    def adapt(self, raw_data: bytes) -> pd.DataFrame:
        """Parse JSON data and apply mappings"""
        import io
        import json
        
        try:
            data = json.load(io.BytesIO(raw_data))
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")
            
            # Apply column mappings
            df = self._apply_column_mappings(df)
            
            # Handle missing columns
            df = self._handle_missing_columns(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse JSON data: {e}")
            raise


class AdapterFactory:
    """Factory for creating appropriate data adapters"""
    
    @staticmethod
    def create_adapter(config: AdapterConfig, layout: Dict[str, Dict[str, Any]] = None) -> DataAdapter:
        """Create appropriate adapter based on source format"""
        
        if config.source_format == "csv":
            return CSVAdapter(config)
        elif config.source_format == "excel":
            return ExcelAdapter(config)
        elif config.source_format == "fixed_width":
            if not layout:
                raise ValueError("Layout specification required for fixed-width adapter")
            return FixedWidthAdapter(config, layout)
        elif config.source_format == "json":
            return JSONAdapter(config)
        else:
            raise ValueError(f"Unsupported source format: {config.source_format}")


# Predefined adapter configurations for common CMS data sources

def get_cms_zip5_adapter_config() -> AdapterConfig:
    """Get adapter configuration for CMS ZIP5 data"""
    return AdapterConfig(
        source_format="excel",
        column_mappings=[
            ColumnMapping("ZIP CODE", "zip5", "str", required=True),
            ColumnMapping("STATE", "state", "str", required=True),
            ColumnMapping("LOCALITY", "locality", "str", required=True)
        ],
        data_type_overrides={
            "zip5": "str",
            "state": "str", 
            "locality": "str"
        }
    )


def get_cms_zip9_adapter_config() -> AdapterConfig:
    """Get adapter configuration for CMS ZIP9 data"""
    return AdapterConfig(
        source_format="fixed_width",
        column_mappings=[
            ColumnMapping("state", "state", "str", required=True),
            ColumnMapping("zip5", "zip5", "str", required=True),
            ColumnMapping("locality", "locality", "str", required=True),
            ColumnMapping("rural_flag", "rural_flag", "bool", required=False),
            ColumnMapping("zip9_low", "zip9_low", "str", required=True),
            ColumnMapping("zip9_high", "zip9_high", "str", required=True)
        ]
    )


def get_cms_zip9_fixed_width_layout() -> Dict[str, Dict[str, Any]]:
    """Get fixed-width layout for CMS ZIP9 data"""
    return {
        "state": {"start": 0, "end": 2, "type": "str"},
        "zip5": {"start": 2, "end": 7, "type": "str"},
        "carrier": {"start": 7, "end": 12, "type": "str"},
        "locality": {"start": 12, "end": 14, "type": "str"},
        "rural_flag": {"start": 14, "end": 15, "type": "str"},
        "plus4_flag": {"start": 20, "end": 21, "type": "str"},
        "plus4": {"start": 21, "end": 25, "type": "str"}
    }


def get_uds_crosswalk_adapter_config() -> AdapterConfig:
    """Get adapter configuration for UDS crosswalk data"""
    return AdapterConfig(
        source_format="excel",
        column_mappings=[
            ColumnMapping("ZIP_CODE", "zip5", "str", required=True),
            ColumnMapping("zcta", "zcta5", "str", required=True),
            ColumnMapping("zip_join_type", "relationship", "str", required=True),
            ColumnMapping("PO_NAME", "city", "str", required=False),
            ColumnMapping("STATE", "state", "str", required=True)
        ]
    )
