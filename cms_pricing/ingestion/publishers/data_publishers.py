"""
DIS-Compliant Data Publishers
Following Data Ingestion Standard PRD v1.0

This module provides snapshot and latest-effective view publishing capabilities
for curated datasets following DIS standards.
"""

import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import structlog

logger = structlog.get_logger()


@dataclass
class PublishSpec:
    """Specification for data publishing"""
    table_name: str
    partition_columns: List[str]
    output_format: str  # "parquet", "csv", "json"
    compression: str = "snappy"
    schema_evolution: bool = True
    create_latest_view: bool = True
    effective_date_column: str = "effective_from"
    business_key_columns: List[str] = None


@dataclass
class PublishResult:
    """Result of data publishing operation"""
    table_name: str
    record_count: int
    file_paths: List[str]
    partition_info: Dict[str, Any]
    view_created: bool = False
    view_name: Optional[str] = None


class DataPublisher(ABC):
    """Base class for data publishers"""
    
    def __init__(self, output_dir: str, db_session: Any = None):
        self.output_dir = Path(output_dir)
        self.db_session = db_session
        self.tool_version = "1.0.0"
    
    @abstractmethod
    def publish_snapshot(
        self, 
        df: pd.DataFrame, 
        spec: PublishSpec,
        vintage_date: date,
        release_id: str
    ) -> PublishResult:
        """Publish snapshot data"""
        pass
    
    @abstractmethod
    def create_latest_effective_view(
        self, 
        table_name: str,
        effective_date_column: str,
        business_key_columns: List[str]
    ) -> bool:
        """Create latest-effective view"""
        pass


class ParquetPublisher(DataPublisher):
    """Publisher for Parquet format following DIS standards"""
    
    def publish_snapshot(
        self, 
        df: pd.DataFrame, 
        spec: PublishSpec,
        vintage_date: date,
        release_id: str
    ) -> PublishResult:
        """Publish snapshot data as Parquet files"""
        
        # Create output directory structure
        output_dir = self.output_dir / "curated" / "payments" / spec.table_name / str(vintage_date)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Add metadata columns
        df = df.copy()
        df['vintage_date'] = vintage_date
        df['release_id'] = release_id
        df['published_at'] = datetime.utcnow()
        
        file_paths = []
        
        if spec.partition_columns:
            # Partitioned output
            for partition_values, partition_df in df.groupby(spec.partition_columns):
                if isinstance(partition_values, tuple):
                    partition_path = "/".join(f"{col}={val}" for col, val in zip(spec.partition_columns, partition_values))
                else:
                    partition_path = f"{spec.partition_columns[0]}={partition_values}"
                
                partition_dir = output_dir / partition_path
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                file_path = partition_dir / f"{spec.table_name}.parquet"
                partition_df.to_parquet(file_path, compression=spec.compression, index=False)
                file_paths.append(str(file_path))
        else:
            # Single file output
            file_path = output_dir / f"{spec.table_name}.parquet"
            df.to_parquet(file_path, compression=spec.compression, index=False)
            file_paths.append(str(file_path))
        
        # Create latest-effective view if requested
        view_created = False
        view_name = None
        if spec.create_latest_view:
            view_name = f"{spec.table_name}_latest"
            view_created = self.create_latest_effective_view(
                spec.table_name,
                spec.effective_date_column,
                spec.business_key_columns or []
            )
        
        return PublishResult(
            table_name=spec.table_name,
            record_count=len(df),
            file_paths=file_paths,
            partition_info={
                "partition_columns": spec.partition_columns,
                "vintage_date": str(vintage_date),
                "release_id": release_id
            },
            view_created=view_created,
            view_name=view_name
        )
    
    def create_latest_effective_view(
        self, 
        table_name: str,
        effective_date_column: str,
        business_key_columns: List[str]
    ) -> bool:
        """Create latest-effective view using window functions"""
        
        if not self.db_session:
            logger.warning("No database session available for view creation")
            return False
        
        try:
            view_name = f"{table_name}_latest"
            
            # Create SQL for latest-effective view
            if business_key_columns:
                # Use ROW_NUMBER() to get latest record per business key
                partition_cols = ", ".join(business_key_columns)
                sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition_cols}
                               ORDER BY {effective_date_column} DESC
                           ) as rn
                    FROM {table_name}
                ) ranked
                WHERE rn = 1
                """
            else:
                # Simple latest by effective date
                sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT *
                FROM {table_name}
                WHERE {effective_date_column} = (
                    SELECT MAX({effective_date_column})
                    FROM {table_name}
                )
                """
            
            # Execute the SQL
            self.db_session.execute(sql)
            self.db_session.commit()
            
            logger.info(f"Created latest-effective view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create latest-effective view: {e}")
            return False


class CSVPublisher(DataPublisher):
    """Publisher for CSV format"""
    
    def publish_snapshot(
        self, 
        df: pd.DataFrame, 
        spec: PublishSpec,
        vintage_date: date,
        release_id: str
    ) -> PublishResult:
        """Publish snapshot data as CSV files"""
        
        # Create output directory structure
        output_dir = self.output_dir / "curated" / "payments" / spec.table_name / str(vintage_date)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Add metadata columns
        df = df.copy()
        df['vintage_date'] = vintage_date
        df['release_id'] = release_id
        df['published_at'] = datetime.utcnow()
        
        file_paths = []
        
        if spec.partition_columns:
            # Partitioned output
            for partition_values, partition_df in df.groupby(spec.partition_columns):
                if isinstance(partition_values, tuple):
                    partition_path = "/".join(f"{col}={val}" for col, val in zip(spec.partition_columns, partition_values))
                else:
                    partition_path = f"{spec.partition_columns[0]}={partition_values}"
                
                partition_dir = output_dir / partition_path
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                file_path = partition_dir / f"{spec.table_name}.csv"
                partition_df.to_csv(file_path, index=False)
                file_paths.append(str(file_path))
        else:
            # Single file output
            file_path = output_dir / f"{spec.table_name}.csv"
            df.to_csv(file_path, index=False)
            file_paths.append(str(file_path))
        
        return PublishResult(
            table_name=spec.table_name,
            record_count=len(df),
            file_paths=file_paths,
            partition_info={
                "partition_columns": spec.partition_columns,
                "vintage_date": str(vintage_date),
                "release_id": release_id
            },
            view_created=False,  # CSV doesn't support views
            view_name=None
        )
    
    def create_latest_effective_view(
        self, 
        table_name: str,
        effective_date_column: str,
        business_key_columns: List[str]
    ) -> bool:
        """CSV format doesn't support views"""
        logger.warning("CSV format doesn't support database views")
        return False


class PublisherFactory:
    """Factory for creating appropriate publishers"""
    
    @staticmethod
    def create_publisher(output_format: str, output_dir: str, db_session: Any = None) -> DataPublisher:
        """Create appropriate publisher based on output format"""
        
        if output_format == "parquet":
            return ParquetPublisher(output_dir, db_session)
        elif output_format == "csv":
            return CSVPublisher(output_dir, db_session)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")


# Predefined publish specifications for common datasets

def get_cms_zip_locality_publish_spec() -> PublishSpec:
    """Get publish specification for CMS ZIP locality data"""
    return PublishSpec(
        table_name="cms_zip_locality",
        partition_columns=["state"],
        output_format="parquet",
        compression="snappy",
        schema_evolution=True,
        create_latest_view=True,
        effective_date_column="effective_from",
        business_key_columns=["zip5"]
    )


def get_zip9_overrides_publish_spec() -> PublishSpec:
    """Get publish specification for ZIP9 overrides data"""
    return PublishSpec(
        table_name="zip9_overrides",
        partition_columns=["state"],
        output_format="parquet",
        compression="snappy",
        schema_evolution=True,
        create_latest_view=True,
        effective_date_column="effective_from",
        business_key_columns=["zip9_low", "zip9_high"]
    )


def get_zip_to_zcta_publish_spec() -> PublishSpec:
    """Get publish specification for ZIP to ZCTA mapping data"""
    return PublishSpec(
        table_name="zip_to_zcta",
        partition_columns=["state"],
        output_format="parquet",
        compression="snappy",
        schema_evolution=True,
        create_latest_view=True,
        effective_date_column="effective_from",
        business_key_columns=["zip5"]
    )


def get_zcta_coords_publish_spec() -> PublishSpec:
    """Get publish specification for ZCTA coordinates data"""
    return PublishSpec(
        table_name="zcta_coords",
        partition_columns=[],
        output_format="parquet",
        compression="snappy",
        schema_evolution=True,
        create_latest_view=True,
        effective_date_column="effective_from",
        business_key_columns=["zcta5"]
    )
