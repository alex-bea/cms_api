"""
DIS-Compliant Observability System
Following Data Ingestion Standard PRD v1.0

This module provides five-pillar observability metrics collection
and monitoring capabilities for all DIS-compliant ingestors.
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Union
import structlog

logger = structlog.get_logger()


class MetricType(Enum):
    """Types of observability metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class Metric:
    """Individual metric measurement"""
    name: str
    value: Union[int, float]
    metric_type: MetricType
    labels: Dict[str, str]
    timestamp: datetime
    description: str = ""


@dataclass
class ObservabilityReport:
    """Complete observability report following DIS five-pillar framework"""
    dataset_name: str
    batch_id: str
    report_timestamp: datetime
    
    # Five Pillars of Observability
    freshness: Dict[str, Any]
    volume: Dict[str, Any]
    schema: Dict[str, Any]
    quality: Dict[str, Any]
    lineage: Dict[str, Any]
    
    # Additional metrics
    performance: Dict[str, Any]
    errors: List[Dict[str, Any]]


class MetricsCollector(ABC):
    """Base class for metrics collection"""
    
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.metrics: List[Metric] = []
        self.start_time = None
        self.end_time = None
    
    def start_timer(self):
        """Start performance timing"""
        self.start_time = time.time()
    
    def stop_timer(self):
        """Stop performance timing"""
        self.end_time = time.time()
    
    def add_metric(
        self, 
        name: str, 
        value: Union[int, float], 
        metric_type: MetricType,
        labels: Dict[str, str] = None,
        description: str = ""
    ):
        """Add a metric measurement"""
        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            labels=labels or {},
            timestamp=datetime.utcnow(),
            description=description
        )
        self.metrics.append(metric)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics"""
        summary = {}
        
        for metric in self.metrics:
            if metric.name not in summary:
                summary[metric.name] = {
                    "type": metric.metric_type.value,
                    "values": [],
                    "labels": metric.labels,
                    "description": metric.description
                }
            summary[metric.name]["values"].append(metric.value)
        
        return summary


class DISObservabilityCollector(MetricsCollector):
    """
    DIS-compliant observability collector implementing the five-pillar framework:
    1. Freshness - How recent is the data?
    2. Volume - How much data is there?
    3. Schema - Has the data structure changed?
    4. Quality - How good is the data?
    5. Lineage - Where did the data come from?
    """
    
    def __init__(self, dataset_name: str):
        super().__init__(dataset_name)
        self.ingestion_start = None
        self.ingestion_end = None
        self.source_files = []
        self.validation_results = {}
        self.enrichment_results = {}
        self.publish_results = {}
    
    def record_ingestion_start(self):
        """Record ingestion start time"""
        self.ingestion_start = datetime.utcnow()
        self.add_metric(
            "ingestion_start_timestamp",
            self.ingestion_start.timestamp(),
            MetricType.GAUGE,
            {"dataset": self.dataset_name},
            "Timestamp when ingestion started"
        )
    
    def record_ingestion_end(self):
        """Record ingestion end time"""
        self.ingestion_end = datetime.utcnow()
        self.add_metric(
            "ingestion_end_timestamp",
            self.ingestion_end.timestamp(),
            MetricType.GAUGE,
            {"dataset": self.dataset_name},
            "Timestamp when ingestion completed"
        )
        
        if self.ingestion_start:
            duration = (self.ingestion_end - self.ingestion_start).total_seconds()
            self.add_metric(
                "ingestion_duration_seconds",
                duration,
                MetricType.HISTOGRAM,
                {"dataset": self.dataset_name},
                "Total ingestion duration in seconds"
            )
    
    def record_source_files(self, source_files: List[Dict[str, Any]]):
        """Record source file information"""
        self.source_files = source_files
        
        self.add_metric(
            "source_files_count",
            len(source_files),
            MetricType.COUNTER,
            {"dataset": self.dataset_name},
            "Number of source files processed"
        )
        
        total_size = sum(f.get("size_bytes", 0) for f in source_files)
        self.add_metric(
            "source_files_size_bytes",
            total_size,
            MetricType.COUNTER,
            {"dataset": self.dataset_name},
            "Total size of source files in bytes"
        )
    
    def record_validation_results(self, validation_results: Dict[str, Any]):
        """Record validation results"""
        self.validation_results = validation_results
        
        # Quality metrics
        total_checks = validation_results.get("total_checks", 0)
        passed_checks = validation_results.get("passed_checks", 0)
        failed_checks = validation_results.get("failed_checks", 0)
        warning_checks = validation_results.get("warning_checks", 0)
        
        self.add_metric(
            "validation_checks_total",
            total_checks,
            MetricType.COUNTER,
            {"dataset": self.dataset_name, "type": "total"},
            "Total number of validation checks"
        )
        
        self.add_metric(
            "validation_checks_passed",
            passed_checks,
            MetricType.COUNTER,
            {"dataset": self.dataset_name, "type": "passed"},
            "Number of passed validation checks"
        )
        
        self.add_metric(
            "validation_checks_failed",
            failed_checks,
            MetricType.COUNTER,
            {"dataset": self.dataset_name, "type": "failed"},
            "Number of failed validation checks"
        )
        
        self.add_metric(
            "validation_checks_warnings",
            warning_checks,
            MetricType.COUNTER,
            {"dataset": self.dataset_name, "type": "warnings"},
            "Number of warning validation checks"
        )
        
        if total_checks > 0:
            quality_score = passed_checks / total_checks
            self.add_metric(
                "data_quality_score",
                quality_score,
                MetricType.GAUGE,
                {"dataset": self.dataset_name},
                "Overall data quality score (0-1)"
            )
    
    def record_enrichment_results(self, enrichment_results: Dict[str, Any]):
        """Record enrichment results"""
        self.enrichment_results = enrichment_results
        
        enrichment_count = enrichment_results.get("enrichment_count", 0)
        self.add_metric(
            "enrichment_records_processed",
            enrichment_count,
            MetricType.COUNTER,
            {"dataset": self.dataset_name},
            "Number of records processed during enrichment"
        )
    
    def record_publish_results(self, publish_results: Dict[str, Any]):
        """Record publish results"""
        self.publish_results = publish_results
        
        record_count = publish_results.get("record_count", 0)
        self.add_metric(
            "published_records_count",
            record_count,
            MetricType.COUNTER,
            {"dataset": self.dataset_name},
            "Number of records published to curated storage"
        )
        
        file_count = len(publish_results.get("file_paths", []))
        self.add_metric(
            "published_files_count",
            file_count,
            MetricType.COUNTER,
            {"dataset": self.dataset_name},
            "Number of files created during publishing"
        )
    
    def record_error(self, error_type: str, error_message: str, error_details: Dict[str, Any] = None):
        """Record an error occurrence"""
        self.add_metric(
            "errors_total",
            1,
            MetricType.COUNTER,
            {"dataset": self.dataset_name, "error_type": error_type},
            "Total number of errors"
        )
        
        logger.error(
            f"Error recorded: {error_type}",
            dataset=self.dataset_name,
            error_type=error_type,
            error_message=error_message,
            error_details=error_details
        )
    
    def generate_observability_report(self, batch_id: str) -> ObservabilityReport:
        """Generate complete observability report following DIS five-pillar framework"""
        
        # Freshness metrics
        freshness = self._calculate_freshness_metrics()
        
        # Volume metrics
        volume = self._calculate_volume_metrics()
        
        # Schema metrics
        schema = self._calculate_schema_metrics()
        
        # Quality metrics
        quality = self._calculate_quality_metrics()
        
        # Lineage metrics
        lineage = self._calculate_lineage_metrics()
        
        # Performance metrics
        performance = self._calculate_performance_metrics()
        
        # Error metrics
        errors = self._extract_error_metrics()
        
        return ObservabilityReport(
            dataset_name=self.dataset_name,
            batch_id=batch_id,
            report_timestamp=datetime.utcnow(),
            freshness=freshness,
            volume=volume,
            schema=schema,
            quality=quality,
            lineage=lineage,
            performance=performance,
            errors=errors
        )
    
    def _calculate_freshness_metrics(self) -> Dict[str, Any]:
        """Calculate freshness metrics (Pillar 1)"""
        if not self.ingestion_start or not self.ingestion_end:
            return {"status": "unknown", "age_hours": None}
        
        age_hours = (datetime.utcnow() - self.ingestion_end).total_seconds() / 3600
        
        # Determine freshness status
        if age_hours < 1:
            status = "fresh"
        elif age_hours < 24:
            status = "stale"
        else:
            status = "outdated"
        
        return {
            "status": status,
            "age_hours": age_hours,
            "last_updated": self.ingestion_end.isoformat(),
            "ingestion_duration_seconds": (self.ingestion_end - self.ingestion_start).total_seconds()
        }
    
    def _calculate_volume_metrics(self) -> Dict[str, Any]:
        """Calculate volume metrics (Pillar 2)"""
        total_records = self.publish_results.get("record_count", 0)
        total_files = len(self.source_files)
        total_size = sum(f.get("size_bytes", 0) for f in self.source_files)
        
        # Determine volume status
        if total_records == 0:
            status = "empty"
        elif total_records < 1000:
            status = "low"
        elif total_records < 100000:
            status = "normal"
        else:
            status = "high"
        
        return {
            "status": status,
            "total_records": total_records,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "avg_records_per_file": total_records / total_files if total_files > 0 else 0
        }
    
    def _calculate_schema_metrics(self) -> Dict[str, Any]:
        """Calculate schema metrics (Pillar 3)"""
        # This would typically compare against a reference schema
        # For now, we'll provide basic schema information
        
        return {
            "status": "stable",
            "version": "1.0",
            "drift_detected": False,
            "schema_evolution": True
        }
    
    def _calculate_quality_metrics(self) -> Dict[str, Any]:
        """Calculate quality metrics (Pillar 4)"""
        quality_score = self.validation_results.get("quality_score", 0.0)
        total_checks = self.validation_results.get("total_checks", 0)
        failed_checks = self.validation_results.get("failed_checks", 0)
        
        # Determine quality status
        if quality_score >= 0.95:
            status = "excellent"
        elif quality_score >= 0.90:
            status = "good"
        elif quality_score >= 0.80:
            status = "fair"
        else:
            status = "poor"
        
        return {
            "status": status,
            "quality_score": quality_score,
            "completeness_rate": 1.0 - (failed_checks / total_checks) if total_checks > 0 else 1.0,
            "validity_rate": quality_score,
            "total_checks": total_checks,
            "failed_checks": failed_checks
        }
    
    def _calculate_lineage_metrics(self) -> Dict[str, Any]:
        """Calculate lineage metrics (Pillar 5)"""
        source_urls = [f.get("url", "") for f in self.source_files]
        
        return {
            "status": "complete",
            "source_urls": source_urls,
            "processing_steps": ["land", "validate", "normalize", "enrich", "publish"],
            "tool_version": "1.0.0",
            "dis_version": "1.0"
        }
    
    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics"""
        if not self.start_time or not self.end_time:
            return {"duration_seconds": None}
        
        duration = self.end_time - self.start_time
        
        return {
            "duration_seconds": duration,
            "start_time": self.start_time,
            "end_time": self.end_time
        }
    
    def _extract_error_metrics(self) -> List[Dict[str, Any]]:
        """Extract error metrics from collected metrics"""
        errors = []
        
        for metric in self.metrics:
            if metric.name == "errors_total":
                errors.append({
                    "error_type": metric.labels.get("error_type", "unknown"),
                    "count": metric.value,
                    "timestamp": metric.timestamp.isoformat()
                })
        
        return errors
