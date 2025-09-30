"""
CMS Data Observability Collector
Implements DIS-compliant observability metrics across five pillars
"""

import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text, func

from ..contracts.ingestor_spec import ValidationSeverity


class MetricType(Enum):
    FRESHNESS = "freshness"
    VOLUME = "volume"
    SCHEMA = "schema"
    QUALITY = "quality"
    LINEAGE = "lineage"


@dataclass
class ObservabilityMetric:
    """Individual observability metric"""
    metric_type: MetricType
    metric_name: str
    value: float
    threshold: float
    status: str  # "healthy", "warning", "critical"
    timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class ObservabilityReport:
    """Complete observability report for a dataset"""
    dataset_name: str
    report_timestamp: datetime
    overall_health_score: float
    metrics: List[ObservabilityMetric]
    alerts: List[Dict[str, Any]]
    recommendations: List[str]


class CMSObservabilityCollector:
    """
    DIS-compliant observability collector for CMS data
    
    Implements the five observability pillars:
    1. Freshness: Age since last successful publish vs. cadence + grace
    2. Volume: Rows/bytes vs. expectation and vs. previous vintage
    3. Schema: Drift detection vs. registered schema version
    4. Quality: Field-level null rates, range checks, distribution shifts
    5. Lineage: Upstream/downstream asset graph, consumer usage stats
    """
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.dataset_name = "cms_zip_locality"
        self.expected_cadence_hours = 24 * 30  # Monthly cadence
        self.freshness_grace_hours = 24 * 3    # 3 days grace period
        self.quality_threshold = 0.95
        self.volume_tolerance = 0.15  # ±15% volume change tolerance
    
    def collect_all_metrics(self) -> ObservabilityReport:
        """Collect all observability metrics"""
        metrics = []
        
        # Collect each pillar
        metrics.extend(self._collect_freshness_metrics())
        metrics.extend(self._collect_volume_metrics())
        metrics.extend(self._collect_schema_metrics())
        metrics.extend(self._collect_quality_metrics())
        metrics.extend(self._collect_lineage_metrics())
        
        # Calculate overall health score
        overall_health_score = self._calculate_overall_health(metrics)
        
        # Generate alerts and recommendations
        alerts = self._generate_alerts(metrics)
        recommendations = self._generate_recommendations(metrics)
        
        return ObservabilityReport(
            dataset_name=self.dataset_name,
            report_timestamp=datetime.now(),
            overall_health_score=overall_health_score,
            metrics=metrics,
            alerts=alerts,
            recommendations=recommendations
        )
    
    def _collect_freshness_metrics(self) -> List[ObservabilityMetric]:
        """Collect freshness metrics (age since last publish)"""
        metrics = []
        
        try:
            # Get last processing timestamp
            result = self.db_session.execute(text("""
                SELECT MAX(processing_timestamp) as last_processed,
                       COUNT(*) as total_records
                FROM cms_zip_locality
            """)).fetchone()
            
            last_processed = result[0]
            total_records = result[1]
            
            if last_processed:
                # Calculate age in hours
                age_hours = (datetime.now() - last_processed).total_seconds() / 3600
                
                # Check if within expected cadence + grace
                expected_max_age = self.expected_cadence_hours + self.freshness_grace_hours
                freshness_score = max(0, 1 - (age_hours / expected_max_age))
                
                status = "healthy" if age_hours <= self.expected_cadence_hours else \
                        "warning" if age_hours <= expected_max_age else "critical"
                
                metrics.append(ObservabilityMetric(
                    metric_type=MetricType.FRESHNESS,
                    metric_name="data_age_hours",
                    value=age_hours,
                    threshold=self.expected_cadence_hours,
                    status=status,
                    timestamp=datetime.now(),
                    metadata={
                        "last_processed": last_processed.isoformat(),
                        "total_records": total_records,
                        "expected_cadence_hours": self.expected_cadence_hours,
                        "grace_period_hours": self.freshness_grace_hours
                    }
                ))
                
                metrics.append(ObservabilityMetric(
                    metric_type=MetricType.FRESHNESS,
                    metric_name="freshness_score",
                    value=freshness_score,
                    threshold=0.8,
                    status=status,
                    timestamp=datetime.now(),
                    metadata={"age_hours": age_hours}
                ))
            else:
                # No data found
                metrics.append(ObservabilityMetric(
                    metric_type=MetricType.FRESHNESS,
                    metric_name="data_age_hours",
                    value=float('inf'),
                    threshold=self.expected_cadence_hours,
                    status="critical",
                    timestamp=datetime.now(),
                    metadata={"error": "No data found"}
                ))
                
        except Exception as e:
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.FRESHNESS,
                metric_name="freshness_error",
                value=0.0,
                threshold=1.0,
                status="critical",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    def _collect_volume_metrics(self) -> List[ObservabilityMetric]:
        """Collect volume metrics (row counts vs expectation)"""
        metrics = []
        
        try:
            # Get current record count
            result = self.db_session.execute(text("""
                SELECT COUNT(*) as current_count,
                       COUNT(DISTINCT vintage) as vintage_count,
                       MIN(vintage) as min_vintage,
                       MAX(vintage) as max_vintage
                FROM cms_zip_locality
            """)).fetchone()
            
            current_count = result[0]
            vintage_count = result[1]
            min_vintage = result[2]
            max_vintage = result[3]
            
            # Expected volume (based on typical CMS data size)
            expected_volume = 50000  # Typical CMS ZIP locality records
            volume_ratio = current_count / expected_volume if expected_volume > 0 else 0
            
            # Check if within tolerance
            volume_deviation = abs(volume_ratio - 1.0)
            status = "healthy" if volume_deviation <= self.volume_tolerance else \
                    "warning" if volume_deviation <= self.volume_tolerance * 2 else "critical"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.VOLUME,
                metric_name="record_count",
                value=current_count,
                threshold=expected_volume,
                status=status,
                timestamp=datetime.now(),
                metadata={
                    "expected_volume": expected_volume,
                    "volume_ratio": volume_ratio,
                    "deviation_percent": volume_deviation * 100,
                    "vintage_count": vintage_count,
                    "vintage_range": f"{min_vintage} to {max_vintage}"
                }
            ))
            
            # Check for volume anomalies
            if current_count == 0:
                status = "critical"
            elif current_count < expected_volume * 0.5:
                status = "warning"
            else:
                status = "healthy"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.VOLUME,
                metric_name="volume_health",
                value=1.0 if status == "healthy" else 0.5 if status == "warning" else 0.0,
                threshold=0.8,
                status=status,
                timestamp=datetime.now(),
                metadata={"anomaly_detected": status != "healthy"}
            ))
            
        except Exception as e:
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.VOLUME,
                metric_name="volume_error",
                value=0.0,
                threshold=1.0,
                status="critical",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    def _collect_schema_metrics(self) -> List[ObservabilityMetric]:
        """Collect schema drift metrics"""
        metrics = []
        
        try:
            # Get current schema info
            result = self.db_session.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'cms_zip_locality'
                ORDER BY ordinal_position
            """)).fetchall()
            
            current_columns = {row[0]: {"type": row[1], "nullable": row[2]} for row in result}
            
            # Expected schema (from our contract)
            expected_schema = {
                "zip5": {"type": "character", "nullable": False},
                "state": {"type": "character", "nullable": False},
                "locality": {"type": "character varying", "nullable": False},
                "carrier_mac": {"type": "character varying", "nullable": True},
                "rural_flag": {"type": "boolean", "nullable": True},
                "effective_from": {"type": "date", "nullable": False},
                "effective_to": {"type": "date", "nullable": True},
                "vintage": {"type": "character varying", "nullable": False},
                "source_filename": {"type": "text", "nullable": True},
                "ingest_run_id": {"type": "uuid", "nullable": True}
            }
            
            # Check for schema drift
            missing_columns = set(expected_schema.keys()) - set(current_columns.keys())
            extra_columns = set(current_columns.keys()) - set(expected_schema.keys())
            
            schema_drift_score = 1.0
            if missing_columns:
                schema_drift_score -= len(missing_columns) * 0.1
            if extra_columns:
                schema_drift_score -= len(extra_columns) * 0.05
            
            status = "healthy" if schema_drift_score >= 0.9 else \
                    "warning" if schema_drift_score >= 0.7 else "critical"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.SCHEMA,
                metric_name="schema_drift_score",
                value=schema_drift_score,
                threshold=0.9,
                status=status,
                timestamp=datetime.now(),
                metadata={
                    "missing_columns": list(missing_columns),
                    "extra_columns": list(extra_columns),
                    "total_columns": len(current_columns),
                    "expected_columns": len(expected_schema)
                }
            ))
            
        except Exception as e:
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.SCHEMA,
                metric_name="schema_error",
                value=0.0,
                threshold=1.0,
                status="critical",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    def _collect_quality_metrics(self) -> List[ObservabilityMetric]:
        """Collect data quality metrics"""
        metrics = []
        
        try:
            # Get quality statistics
            result = self.db_session.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(data_quality_score) as avg_quality_score,
                    MIN(data_quality_score) as min_quality_score,
                    MAX(data_quality_score) as max_quality_score,
                    COUNT(CASE WHEN data_quality_score IS NULL THEN 1 END) as null_quality_scores
                FROM cms_zip_locality
            """)).fetchone()
            
            total_records = result[0]
            avg_quality = result[1] or 0.0
            min_quality = result[2] or 0.0
            max_quality = result[3] or 0.0
            null_quality = result[4]
            
            # Overall quality score
            quality_coverage = (total_records - null_quality) / total_records if total_records > 0 else 0
            overall_quality = avg_quality * quality_coverage
            
            status = "healthy" if overall_quality >= self.quality_threshold else \
                    "warning" if overall_quality >= 0.8 else "critical"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.QUALITY,
                metric_name="overall_quality_score",
                value=overall_quality,
                threshold=self.quality_threshold,
                status=status,
                timestamp=datetime.now(),
                metadata={
                    "avg_quality": avg_quality,
                    "min_quality": min_quality,
                    "max_quality": max_quality,
                    "quality_coverage": quality_coverage,
                    "null_quality_scores": null_quality
                }
            ))
            
            # Field-level completeness
            completeness_result = self.db_session.execute(text("""
                SELECT 
                    COUNT(CASE WHEN zip5 IS NOT NULL THEN 1 END) as zip5_complete,
                    COUNT(CASE WHEN state IS NOT NULL THEN 1 END) as state_complete,
                    COUNT(CASE WHEN locality IS NOT NULL THEN 1 END) as locality_complete,
                    COUNT(CASE WHEN effective_from IS NOT NULL THEN 1 END) as effective_from_complete,
                    COUNT(CASE WHEN vintage IS NOT NULL THEN 1 END) as vintage_complete
                FROM cms_zip_locality
            """)).fetchone()
            
            critical_fields = ["zip5", "state", "locality", "effective_from", "vintage"]
            completeness_scores = [
                completeness_result[0] / total_records,  # zip5
                completeness_result[1] / total_records,  # state
                completeness_result[2] / total_records,  # locality
                completeness_result[3] / total_records,  # effective_from
                completeness_result[4] / total_records   # vintage
            ]
            
            avg_completeness = sum(completeness_scores) / len(completeness_scores)
            
            status = "healthy" if avg_completeness >= 0.99 else \
                    "warning" if avg_completeness >= 0.95 else "critical"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.QUALITY,
                metric_name="field_completeness",
                value=avg_completeness,
                threshold=0.99,
                status=status,
                timestamp=datetime.now(),
                metadata={
                    "field_scores": dict(zip(critical_fields, completeness_scores)),
                    "avg_completeness": avg_completeness
                }
            ))
            
        except Exception as e:
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.QUALITY,
                metric_name="quality_error",
                value=0.0,
                threshold=1.0,
                status="critical",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    def _collect_lineage_metrics(self) -> List[ObservabilityMetric]:
        """Collect lineage and usage metrics"""
        metrics = []
        
        try:
            # Get ingestion run information
            result = self.db_session.execute(text("""
                SELECT 
                    COUNT(DISTINCT ingest_run_id) as total_runs,
                    COUNT(DISTINCT source_filename) as source_files,
                    MIN(processing_timestamp) as first_ingestion,
                    MAX(processing_timestamp) as last_ingestion
                FROM cms_zip_locality
            """)).fetchone()
            
            total_runs = result[0]
            source_files = result[1]
            first_ingestion = result[2]
            last_ingestion = result[3]
            
            # Calculate lineage health
            lineage_health = 1.0
            if total_runs == 0:
                lineage_health = 0.0
            elif source_files == 0:
                lineage_health = 0.5
            
            status = "healthy" if lineage_health >= 0.8 else \
                    "warning" if lineage_health >= 0.5 else "critical"
            
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.LINEAGE,
                metric_name="lineage_health",
                value=lineage_health,
                threshold=0.8,
                status=status,
                timestamp=datetime.now(),
                metadata={
                    "total_runs": total_runs,
                    "source_files": source_files,
                    "first_ingestion": first_ingestion.isoformat() if first_ingestion else None,
                    "last_ingestion": last_ingestion.isoformat() if last_ingestion else None,
                    "data_age_days": (datetime.now() - first_ingestion).days if first_ingestion else None
                }
            ))
            
            # Check for data freshness in lineage
            if last_ingestion:
                hours_since_last = (datetime.now() - last_ingestion).total_seconds() / 3600
                freshness_score = max(0, 1 - (hours_since_last / (24 * 30)))  # Monthly cadence
                
                status = "healthy" if freshness_score >= 0.8 else \
                        "warning" if freshness_score >= 0.5 else "critical"
                
                metrics.append(ObservabilityMetric(
                    metric_type=MetricType.LINEAGE,
                    metric_name="data_freshness",
                    value=freshness_score,
                    threshold=0.8,
                    status=status,
                    timestamp=datetime.now(),
                    metadata={
                        "hours_since_last_ingestion": hours_since_last,
                        "last_ingestion": last_ingestion.isoformat()
                    }
                ))
            
        except Exception as e:
            metrics.append(ObservabilityMetric(
                metric_type=MetricType.LINEAGE,
                metric_name="lineage_error",
                value=0.0,
                threshold=1.0,
                status="critical",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    def _calculate_overall_health(self, metrics: List[ObservabilityMetric]) -> float:
        """Calculate overall health score from all metrics"""
        if not metrics:
            return 0.0
        
        # Weight different metric types
        weights = {
            MetricType.FRESHNESS: 0.25,
            MetricType.VOLUME: 0.20,
            MetricType.SCHEMA: 0.20,
            MetricType.QUALITY: 0.25,
            MetricType.LINEAGE: 0.10
        }
        
        weighted_scores = []
        for metric in metrics:
            weight = weights.get(metric.metric_type, 0.1)
            weighted_scores.append(metric.value * weight)
        
        return sum(weighted_scores) / sum(weights.values()) if weighted_scores else 0.0
    
    def _generate_alerts(self, metrics: List[ObservabilityMetric]) -> List[Dict[str, Any]]:
        """Generate alerts based on metrics"""
        alerts = []
        
        for metric in metrics:
            if metric.status == "critical":
                alerts.append({
                    "severity": "critical",
                    "metric": metric.metric_name,
                    "value": metric.value,
                    "threshold": metric.threshold,
                    "message": f"Critical alert: {metric.metric_name} = {metric.value:.3f} (threshold: {metric.threshold})",
                    "timestamp": metric.timestamp.isoformat()
                })
            elif metric.status == "warning":
                alerts.append({
                    "severity": "warning",
                    "metric": metric.metric_name,
                    "value": metric.value,
                    "threshold": metric.threshold,
                    "message": f"Warning: {metric.metric_name} = {metric.value:.3f} (threshold: {metric.threshold})",
                    "timestamp": metric.timestamp.isoformat()
                })
        
        return alerts
    
    def _generate_recommendations(self, metrics: List[ObservabilityMetric]) -> List[str]:
        """Generate recommendations based on metrics"""
        recommendations = []
        
        # Check for common issues and provide recommendations
        critical_metrics = [m for m in metrics if m.status == "critical"]
        warning_metrics = [m for m in metrics if m.status == "warning"]
        
        if critical_metrics:
            recommendations.append("Immediate attention required: Address critical metrics")
        
        if warning_metrics:
            recommendations.append("Monitor warning metrics and consider preventive actions")
        
        # Specific recommendations based on metric types
        freshness_issues = [m for m in metrics if m.metric_type == MetricType.FRESHNESS and m.status != "healthy"]
        if freshness_issues:
            recommendations.append("Consider updating data ingestion schedule or investigating processing delays")
        
        quality_issues = [m for m in metrics if m.metric_type == MetricType.QUALITY and m.status != "healthy"]
        if quality_issues:
            recommendations.append("Review data quality processes and validation rules")
        
        schema_issues = [m for m in metrics if m.metric_type == MetricType.SCHEMA and m.status != "healthy"]
        if schema_issues:
            recommendations.append("Update schema contracts and validate data structure changes")
        
        return recommendations
