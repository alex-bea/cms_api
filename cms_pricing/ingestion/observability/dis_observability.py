"""
DIS-Compliant 5-Pillar Observability System
Following Data Ingestion Standard PRD v1.0

This module implements the 5 pillars of observability for data ingestion:
1. Freshness - Data recency and update frequency
2. Volume - Data size and record counts
3. Schema - Schema validation and evolution
4. Quality - Data quality metrics and thresholds
5. Lineage - Data provenance and transformation tracking
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import structlog

logger = structlog.get_logger()


@dataclass
class FreshnessMetrics:
    """Freshness pillar metrics - data recency and update frequency"""
    last_updated: datetime
    expected_frequency_hours: float
    actual_frequency_hours: Optional[float] = None
    staleness_hours: Optional[float] = None
    freshness_score: float = 1.0
    alerts: List[str] = None
    
    def __post_init__(self):
        if self.alerts is None:
            self.alerts = []
        
        # Calculate staleness
        if self.last_updated:
            now = datetime.utcnow()
            self.staleness_hours = (now - self.last_updated).total_seconds() / 3600
            
            # Calculate freshness score (1.0 = fresh, 0.0 = stale)
            if self.expected_frequency_hours > 0:
                expected_staleness = self.expected_frequency_hours * 1.5  # 50% grace period
                if self.staleness_hours <= expected_staleness:
                    self.freshness_score = 1.0
                else:
                    self.freshness_score = max(0.0, 1.0 - (self.staleness_hours - expected_staleness) / expected_staleness)
            
            # Generate alerts
            if self.staleness_hours > self.expected_frequency_hours * 2:
                self.alerts.append(f"Data is {self.staleness_hours:.1f} hours stale (expected: {self.expected_frequency_hours:.1f}h)")
            elif self.staleness_hours > self.expected_frequency_hours * 1.5:
                self.alerts.append(f"Data is approaching staleness: {self.staleness_hours:.1f} hours old")


@dataclass
class VolumeMetrics:
    """Volume pillar metrics - data size and record counts"""
    total_records: int
    total_size_bytes: int
    expected_records: Optional[int] = None
    expected_size_bytes: Optional[int] = None
    record_growth_rate: Optional[float] = None
    size_growth_rate: Optional[float] = None
    volume_score: float = 1.0
    alerts: List[str] = None
    
    def __post_init__(self):
        if self.alerts is None:
            self.alerts = []
        
        # Calculate volume score
        if self.expected_records and self.expected_records > 0:
            record_ratio = self.total_records / self.expected_records
            if 0.8 <= record_ratio <= 1.2:  # Within 20% tolerance
                self.volume_score = 1.0
            elif 0.5 <= record_ratio <= 1.5:  # Within 50% tolerance
                self.volume_score = 0.7
            else:
                self.volume_score = 0.3
                
            # Generate alerts
            if record_ratio < 0.8:
                self.alerts.append(f"Record count {self.total_records} is {((1-record_ratio)*100):.1f}% below expected {self.expected_records}")
            elif record_ratio > 1.2:
                self.alerts.append(f"Record count {self.total_records} is {((record_ratio-1)*100):.1f}% above expected {self.expected_records}")


@dataclass
class SchemaMetrics:
    """Schema pillar metrics - schema validation and evolution"""
    schema_version: str
    schema_contract_valid: bool
    schema_evolution_detected: bool = False
    breaking_changes: int = 0
    non_breaking_changes: int = 0
    schema_score: float = 1.0
    alerts: List[str] = None
    
    def __post_init__(self):
        if self.alerts is None:
            self.alerts = []
        
        # Calculate schema score
        if not self.schema_contract_valid:
            self.schema_score = 0.0
        elif self.breaking_changes > 0:
            self.schema_score = 0.3
        elif self.non_breaking_changes > 5:
            self.schema_score = 0.7
        else:
            self.schema_score = 1.0
            
        # Generate alerts
        if not self.schema_contract_valid:
            self.alerts.append("Schema contract validation failed")
        if self.breaking_changes > 0:
            self.alerts.append(f"Schema has {self.breaking_changes} breaking changes")
        if self.schema_evolution_detected:
            self.alerts.append("Schema evolution detected - review required")


@dataclass
class QualityMetrics:
    """Quality pillar metrics - data quality and validation"""
    quality_score: float
    validation_rules_passed: int
    validation_rules_failed: int
    null_rate: float
    duplicate_rate: float
    completeness_rate: float
    accuracy_rate: float
    quality_threshold: float = 0.95
    alerts: List[str] = None
    
    def __post_init__(self):
        if self.alerts is None:
            self.alerts = []
        
        # Generate alerts
        if self.quality_score < self.quality_threshold:
            self.alerts.append(f"Quality score {self.quality_score:.3f} below threshold {self.quality_threshold}")
        if self.null_rate > 0.1:
            self.alerts.append(f"High null rate: {self.null_rate:.1%}")
        if self.duplicate_rate > 0.05:
            self.alerts.append(f"High duplicate rate: {self.duplicate_rate:.1%}")
        if self.completeness_rate < 0.9:
            self.alerts.append(f"Low completeness: {self.completeness_rate:.1%}")


@dataclass
class LineageMetrics:
    """Lineage pillar metrics - data provenance and transformation tracking"""
    source_urls: List[str]
    source_checksums: List[str]
    transformation_steps: List[str]
    processing_timestamp: datetime
    ingest_run_id: str
    batch_id: str
    release_id: str
    lineage_score: float = 1.0
    alerts: List[str] = None
    
    def __post_init__(self):
        if self.alerts is None:
            self.alerts = []
        
        # Calculate lineage score
        if not self.source_urls or not self.source_checksums:
            self.lineage_score = 0.5
            self.alerts.append("Incomplete source provenance")
        elif len(self.transformation_steps) < 3:  # Expect at least Land, Validate, Normalize
            self.lineage_score = 0.7
            self.alerts.append("Incomplete transformation tracking")
        else:
            self.lineage_score = 1.0


@dataclass
class AlertDecision:
    """Represents alert routing decisions for a run."""

    severity: str
    should_page: bool
    should_block_run: bool
    triggers: List[str] = field(default_factory=list)
    evaluated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DISObservabilityReport:
    """Complete 5-pillar observability report"""
    dataset_name: str
    report_timestamp: datetime
    freshness: FreshnessMetrics
    volume: VolumeMetrics
    schema: SchemaMetrics
    quality: QualityMetrics
    lineage: LineageMetrics
    overall_score: float
    critical_alerts: List[str]
    warnings: List[str]
    alert_decision: AlertDecision = None

    def __post_init__(self):
        # Calculate overall score as weighted average
        weights = {
            'freshness': 0.25,
            'volume': 0.20,
            'schema': 0.20,
            'quality': 0.25,
            'lineage': 0.10
        }
        
        self.overall_score = (
            self.freshness.freshness_score * weights['freshness'] +
            self.volume.volume_score * weights['volume'] +
            self.schema.schema_score * weights['schema'] +
            self.quality.quality_score * weights['quality'] +
            self.lineage.lineage_score * weights['lineage']
        )
        
        # Collect all alerts
        all_alerts = []
        all_alerts.extend(self.freshness.alerts or [])
        all_alerts.extend(self.volume.alerts or [])
        all_alerts.extend(self.schema.alerts or [])
        all_alerts.extend(self.quality.alerts or [])
        all_alerts.extend(self.lineage.alerts or [])
        
        # Categorize alerts
        self.critical_alerts = [alert for alert in all_alerts if any(keyword in alert.lower() for keyword in ['failed', 'breaking', 'critical', 'stale'])]
        self.warnings = [alert for alert in all_alerts if alert not in self.critical_alerts]

        # Determine alert routing
        severity = "normal"
        should_page = False
        should_block = False
        triggers: List[str] = []

        if self.critical_alerts:
            severity = "critical"
            should_page = True
            should_block = True
            triggers = list(self.critical_alerts)
        elif self.warnings:
            severity = "warning"
            triggers = list(self.warnings)

        if self.overall_score < 0.5 and severity == "normal":
            severity = "degraded"
            triggers.append(
                f"Overall score {self.overall_score:.2f} below 0.50 threshold"
            )

        self.alert_decision = AlertDecision(
            severity=severity,
            should_page=should_page,
            should_block_run=should_block,
            triggers=triggers,
        )


class ObservabilityHistoryStore:
    """Persistent storage for observability report history."""

    def __init__(self, output_dir: Path, filename: str = "observability_history.json"):
        self.output_dir = output_dir
        self.index_path = self.output_dir / filename
        self._cache: Optional[List[Dict[str, Any]]] = None

    def load_index(self) -> List[Dict[str, Any]]:
        if self._cache is not None:
            return self._cache

        if not self.index_path.exists():
            self._cache = []
            return self._cache

        try:
            with open(self.index_path, "r") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    self._cache = data
                else:
                    logger.warning(
                        "Observability history index invalid format; resetting",
                        path=str(self.index_path),
                    )
                    self._cache = []
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(
                "Failed to load observability history index; starting fresh",
                path=str(self.index_path),
                error=str(exc),
            )
            self._cache = []

        return self._cache

    def append_entry(self, entry: Dict[str, Any]) -> None:
        history = list(self.load_index())
        history.append(entry)
        self._write(history)
        self._cache = history

    def _write(self, history: List[Dict[str, Any]]) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with open(self.index_path, "w") as fh:
            json.dump(history, fh, indent=2)


class DISObservabilityCollector:
    """
    Collects and manages 5-pillar observability metrics for DIS-compliant ingestors
    """
    
    def __init__(self, output_dir: str = "data/observability"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._history_store = ObservabilityHistoryStore(self.output_dir)
        self.metrics_history: List[DISObservabilityReport] = self._load_history()
        self.last_alert_decision: Optional[AlertDecision] = None
    
    def collect_freshness_metrics(
        self,
        dataset_name: str,
        last_updated: datetime,
        expected_frequency_hours: float,
        previous_update: Optional[datetime] = None
    ) -> FreshnessMetrics:
        """Collect freshness metrics"""
        actual_frequency = None
        if previous_update:
            actual_frequency = (last_updated - previous_update).total_seconds() / 3600
        
        return FreshnessMetrics(
            last_updated=last_updated,
            expected_frequency_hours=expected_frequency_hours,
            actual_frequency_hours=actual_frequency
        )
    
    def collect_volume_metrics(
        self,
        total_records: int,
        total_size_bytes: int,
        expected_records: Optional[int] = None,
        expected_size_bytes: Optional[int] = None,
        previous_metrics: Optional[VolumeMetrics] = None
    ) -> VolumeMetrics:
        """Collect volume metrics"""
        record_growth_rate = None
        size_growth_rate = None
        
        if previous_metrics:
            if previous_metrics.total_records > 0:
                record_growth_rate = (total_records - previous_metrics.total_records) / previous_metrics.total_records
            if previous_metrics.total_size_bytes > 0:
                size_growth_rate = (total_size_bytes - previous_metrics.total_size_bytes) / previous_metrics.total_size_bytes
        
        return VolumeMetrics(
            total_records=total_records,
            total_size_bytes=total_size_bytes,
            expected_records=expected_records,
            expected_size_bytes=expected_size_bytes,
            record_growth_rate=record_growth_rate,
            size_growth_rate=size_growth_rate
        )
    
    def collect_schema_metrics(
        self,
        schema_version: str,
        validation_results: Dict[str, Any],
        previous_schema_version: Optional[str] = None
    ) -> SchemaMetrics:
        """Collect schema metrics"""
        schema_contract_valid = validation_results.get("valid", False)
        breaking_changes = validation_results.get("breaking_changes", 0)
        non_breaking_changes = validation_results.get("non_breaking_changes", 0)
        schema_evolution_detected = previous_schema_version and previous_schema_version != schema_version
        
        return SchemaMetrics(
            schema_version=schema_version,
            schema_contract_valid=schema_contract_valid,
            schema_evolution_detected=schema_evolution_detected,
            breaking_changes=breaking_changes,
            non_breaking_changes=non_breaking_changes
        )
    
    def collect_quality_metrics(
        self,
        validation_results: Dict[str, Any],
        quality_threshold: float = 0.95
    ) -> QualityMetrics:
        """Collect quality metrics"""
        quality_score = validation_results.get("quality_score", 0.0)
        rules_passed = validation_results.get("rules_passed", 0)
        rules_failed = validation_results.get("rules_failed", 0)
        
        # Calculate rates
        total_rules = rules_passed + rules_failed
        null_rate = validation_results.get("metrics", {}).get("null_rate", 0.0)
        duplicate_rate = validation_results.get("metrics", {}).get("duplicate_rate", 0.0)
        completeness_rate = 1.0 - null_rate
        accuracy_rate = rules_passed / total_rules if total_rules > 0 else 1.0
        
        return QualityMetrics(
            quality_score=quality_score,
            validation_rules_passed=rules_passed,
            validation_rules_failed=rules_failed,
            null_rate=null_rate,
            duplicate_rate=duplicate_rate,
            completeness_rate=completeness_rate,
            accuracy_rate=accuracy_rate,
            quality_threshold=quality_threshold
        )
    
    def collect_lineage_metrics(
        self,
        source_files: List[Dict[str, Any]],
        transformation_steps: List[str],
        processing_timestamp: datetime,
        ingest_run_id: str,
        batch_id: str,
        release_id: str
    ) -> LineageMetrics:
        """Collect lineage metrics"""
        source_urls = [sf.get("url", "") for sf in source_files]
        source_checksums = [sf.get("sha256", "") for sf in source_files]
        
        return LineageMetrics(
            source_urls=source_urls,
            source_checksums=source_checksums,
            transformation_steps=transformation_steps,
            processing_timestamp=processing_timestamp,
            ingest_run_id=ingest_run_id,
            batch_id=batch_id,
            release_id=release_id
        )
    
    def generate_observability_report(
        self,
        dataset_name: str,
        freshness: FreshnessMetrics,
        volume: VolumeMetrics,
        schema: SchemaMetrics,
        quality: QualityMetrics,
        lineage: LineageMetrics
    ) -> DISObservabilityReport:
        """Generate complete 5-pillar observability report"""
        report = DISObservabilityReport(
            dataset_name=dataset_name,
            report_timestamp=datetime.utcnow(),
            freshness=freshness,
            volume=volume,
            schema=schema,
            quality=quality,
            lineage=lineage,
            overall_score=0.0,  # Will be calculated in __post_init__
            critical_alerts=[],
            warnings=[]
        )
        
        # Store in history
        self.metrics_history.append(report)

        # Save to disk
        self._save_report(report)
        self._persist_history_entry(report)
        self.last_alert_decision = report.alert_decision
        self._dispatch_alerts(report)

        return report

    def _save_report(self, report: DISObservabilityReport):
        """Save observability report to disk"""
        timestamp = report.report_timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"{report.dataset_name}_observability_{timestamp}.json"
        filepath = self.output_dir / filename

        # Convert to dict for JSON serialization
        report_dict = self._serialize_dataclass(report)

        with open(filepath, 'w') as f:
            json.dump(report_dict, f, indent=2)

        logger.info(f"Saved observability report: {filepath}")

    def _serialize_dataclass(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "__dataclass_fields__"):
            return {
                key: self._serialize_dataclass(getattr(obj, key))
                for key in obj.__dataclass_fields__.keys()
            }
        if isinstance(obj, dict):
            return {key: self._serialize_dataclass(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self._serialize_dataclass(item) for item in obj]
        return obj

    def _persist_history_entry(self, report: DISObservabilityReport) -> None:
        entry = {
            "run_id": report.lineage.ingest_run_id,
            "dataset": report.dataset_name,
            "release_id": report.lineage.release_id,
            "report_timestamp": report.report_timestamp.isoformat(),
            "overall_score": report.overall_score,
            "critical_alerts": list(report.critical_alerts),
            "warnings": list(report.warnings),
            "alert_decision": self._serialize_dataclass(report.alert_decision)
            if report.alert_decision
            else None,
            "report": self._serialize_dataclass(report),
        }

        self._history_store.append_entry(entry)

    def _load_history(self) -> List[DISObservabilityReport]:
        reports: List[DISObservabilityReport] = []
        for entry in self._history_store.load_index():
            payload = entry.get("report")
            if not payload:
                continue
            try:
                reports.append(self._report_from_dict(payload))
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Failed to hydrate observability report from history",
                    run_id=entry.get("run_id"),
                    error=str(exc),
                )
        return reports

    def _parse_datetime(self, value: Optional[str], *, default: Optional[datetime] = None) -> Optional[datetime]:
        if value is None:
            return default
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return default

    def _report_from_dict(self, data: Dict[str, Any]) -> DISObservabilityReport:
        freshness_dict = data.get("freshness", {})
        freshness = FreshnessMetrics(
            last_updated=self._parse_datetime(freshness_dict.get("last_updated"), default=datetime.utcnow()),
            expected_frequency_hours=freshness_dict.get("expected_frequency_hours", 0.0),
            actual_frequency_hours=freshness_dict.get("actual_frequency_hours"),
        )

        volume_dict = data.get("volume", {})
        volume = VolumeMetrics(
            total_records=volume_dict.get("total_records", 0),
            total_size_bytes=volume_dict.get("total_size_bytes", 0),
            expected_records=volume_dict.get("expected_records"),
            expected_size_bytes=volume_dict.get("expected_size_bytes"),
            record_growth_rate=volume_dict.get("record_growth_rate"),
            size_growth_rate=volume_dict.get("size_growth_rate"),
        )

        schema_dict = data.get("schema", {})
        schema = SchemaMetrics(
            schema_version=schema_dict.get("schema_version", ""),
            schema_contract_valid=schema_dict.get("schema_contract_valid", True),
            schema_evolution_detected=schema_dict.get("schema_evolution_detected", False),
            breaking_changes=schema_dict.get("breaking_changes", 0),
            non_breaking_changes=schema_dict.get("non_breaking_changes", 0),
        )

        quality_dict = data.get("quality", {})
        quality = QualityMetrics(
            quality_score=quality_dict.get("quality_score", 1.0),
            validation_rules_passed=quality_dict.get("validation_rules_passed", 0),
            validation_rules_failed=quality_dict.get("validation_rules_failed", 0),
            null_rate=quality_dict.get("null_rate", 0.0),
            duplicate_rate=quality_dict.get("duplicate_rate", 0.0),
            completeness_rate=quality_dict.get("completeness_rate", 1.0),
            accuracy_rate=quality_dict.get("accuracy_rate", 1.0),
            quality_threshold=quality_dict.get("quality_threshold", 0.95),
        )

        lineage_dict = data.get("lineage", {})
        lineage = LineageMetrics(
            source_urls=lineage_dict.get("source_urls", []),
            source_checksums=lineage_dict.get("source_checksums", []),
            transformation_steps=lineage_dict.get("transformation_steps", []),
            processing_timestamp=self._parse_datetime(
                lineage_dict.get("processing_timestamp"), default=datetime.utcnow()
            ),
            ingest_run_id=lineage_dict.get("ingest_run_id", ""),
            batch_id=lineage_dict.get("batch_id", ""),
            release_id=lineage_dict.get("release_id", ""),
        )

        report_timestamp = self._parse_datetime(
            data.get("report_timestamp"), default=datetime.utcnow()
        ) or datetime.utcnow()

        report = DISObservabilityReport(
            dataset_name=data.get("dataset_name", ""),
            report_timestamp=report_timestamp,
            freshness=freshness,
            volume=volume,
            schema=schema,
            quality=quality,
            lineage=lineage,
            overall_score=data.get("overall_score", 0.0),
            critical_alerts=data.get("critical_alerts", []),
            warnings=data.get("warnings", []),
        )

        return report

    def _dispatch_alerts(self, report: DISObservabilityReport) -> None:
        decision = report.alert_decision
        if not decision:
            return

        context = {
            "dataset": report.dataset_name,
            "run_id": report.lineage.ingest_run_id,
            "release_id": report.lineage.release_id,
            "severity": decision.severity,
            "triggers": decision.triggers,
        }

        if decision.should_block_run:
            logger.error("Blocking run due to observability alerts", **context)
        elif decision.should_page:
            logger.warning("Paging on observability alerts", **context)
        elif decision.triggers:
            logger.info("Observability warnings recorded", **context)
        else:
            logger.info("Observability run healthy", **context)
    
    def get_latest_report(self, dataset_name: str) -> Optional[DISObservabilityReport]:
        """Get the latest observability report for a dataset"""
        dataset_reports = [r for r in self.metrics_history if r.dataset_name == dataset_name]
        return max(dataset_reports, key=lambda r: r.report_timestamp) if dataset_reports else None
    
    def get_observability_summary(self) -> Dict[str, Any]:
        """Get summary of all observability metrics"""
        if not self.metrics_history:
            return {"message": "No observability data available"}
        
        latest_reports = {}
        for dataset_name in set(r.dataset_name for r in self.metrics_history):
            latest_reports[dataset_name] = self.get_latest_report(dataset_name)
        
        summary = {
            "total_datasets": len(latest_reports),
            "datasets": {},
            "overall_health": "healthy"
        }
        
        critical_count = 0
        warning_count = 0
        
        for dataset_name, report in latest_reports.items():
            if report:
                summary["datasets"][dataset_name] = {
                    "overall_score": report.overall_score,
                    "critical_alerts": len(report.critical_alerts),
                    "warnings": len(report.warnings),
                    "last_updated": report.report_timestamp.isoformat(),
                    "alert_severity": report.alert_decision.severity if report.alert_decision else "unknown",
                    "block_run": report.alert_decision.should_block_run if report.alert_decision else False,
                }

                critical_count += len(report.critical_alerts)
                warning_count += len(report.warnings)
        
        # Determine overall health
        if critical_count > 0:
            summary["overall_health"] = "critical"
        elif warning_count > 5:
            summary["overall_health"] = "degraded"
        
        summary["total_critical_alerts"] = critical_count
        summary["total_warnings"] = warning_count
        
        return summary


# Global observability collector instance
observability_collector = DISObservabilityCollector()
