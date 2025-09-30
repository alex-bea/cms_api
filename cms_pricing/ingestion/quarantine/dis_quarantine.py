"""
DIS-Compliant Quarantine System
Following Data Ingestion Standard PRD v1.0

This module implements comprehensive quarantine workflow for handling rejected records
with proper triage, review, and remediation processes.
"""

import json
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import structlog

logger = structlog.get_logger()


class QuarantineStatus(Enum):
    """Quarantine status levels"""
    NEW = "new"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    REMEDIATED = "remediated"
    ESCALATED = "escalated"


class QuarantineSeverity(Enum):
    """Quarantine severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class QuarantineCategory(Enum):
    """Quarantine categories"""
    SCHEMA_VIOLATION = "schema_violation"
    DATA_QUALITY = "data_quality"
    BUSINESS_RULE = "business_rule"
    FORMAT_ERROR = "format_error"
    VALIDATION_FAILURE = "validation_failure"
    DUPLICATE = "duplicate"
    MISSING_REQUIRED = "missing_required"
    OUT_OF_RANGE = "out_of_range"
    INVALID_FORMAT = "invalid_format"


@dataclass
class QuarantineRecord:
    """Individual quarantined record"""
    record_id: str
    dataset_name: str
    batch_id: str
    release_id: str
    quarantine_timestamp: datetime
    status: QuarantineStatus
    severity: QuarantineSeverity
    category: QuarantineCategory
    rule_id: str
    rule_name: str
    error_message: str
    error_code: str
    raw_data: Dict[str, Any]
    processed_data: Optional[Dict[str, Any]] = None
    remediation_notes: Optional[str] = None
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    remediation_actions: List[str] = None
    
    def __post_init__(self):
        if self.remediation_actions is None:
            self.remediation_actions = []


@dataclass
class QuarantineBatch:
    """Batch of quarantined records"""
    batch_id: str
    dataset_name: str
    release_id: str
    quarantine_timestamp: datetime
    total_records: int
    records: List[QuarantineRecord]
    summary: Dict[str, Any]
    triage_priority: str
    estimated_remediation_time: Optional[timedelta] = None


@dataclass
class QuarantineMetrics:
    """Quarantine system metrics"""
    total_quarantined: int
    by_status: Dict[str, int]
    by_severity: Dict[str, int]
    by_category: Dict[str, int]
    avg_remediation_time_hours: float
    remediation_success_rate: float
    escalation_rate: float
    oldest_unresolved_days: float


class QuarantineTriage:
    """Triage system for quarantined records"""
    
    def __init__(self):
        self.triage_rules = self._setup_triage_rules()
    
    def _setup_triage_rules(self) -> Dict[str, Dict[str, Any]]:
        """Setup triage rules for different error types"""
        return {
            "schema_violation": {
                "severity": QuarantineSeverity.HIGH,
                "priority": "high",
                "auto_remediation": False,
                "requires_review": True,
                "estimated_time": timedelta(hours=2)
            },
            "data_quality": {
                "severity": QuarantineSeverity.MEDIUM,
                "priority": "medium",
                "auto_remediation": True,
                "requires_review": False,
                "estimated_time": timedelta(minutes=30)
            },
            "business_rule": {
                "severity": QuarantineSeverity.CRITICAL,
                "priority": "critical",
                "auto_remediation": False,
                "requires_review": True,
                "estimated_time": timedelta(hours=4)
            },
            "format_error": {
                "severity": QuarantineSeverity.LOW,
                "priority": "low",
                "auto_remediation": True,
                "requires_review": False,
                "estimated_time": timedelta(minutes=15)
            },
            "validation_failure": {
                "severity": QuarantineSeverity.MEDIUM,
                "priority": "medium",
                "auto_remediation": False,
                "requires_review": True,
                "estimated_time": timedelta(hours=1)
            },
            "duplicate": {
                "severity": QuarantineSeverity.LOW,
                "priority": "low",
                "auto_remediation": True,
                "requires_review": False,
                "estimated_time": timedelta(minutes=5)
            },
            "missing_required": {
                "severity": QuarantineSeverity.HIGH,
                "priority": "high",
                "auto_remediation": False,
                "requires_review": True,
                "estimated_time": timedelta(hours=1)
            },
            "out_of_range": {
                "severity": QuarantineSeverity.MEDIUM,
                "priority": "medium",
                "auto_remediation": True,
                "requires_review": False,
                "estimated_time": timedelta(minutes=20)
            },
            "invalid_format": {
                "severity": QuarantineSeverity.LOW,
                "priority": "low",
                "auto_remediation": True,
                "requires_review": False,
                "estimated_time": timedelta(minutes=10)
            }
        }
    
    def triage_record(self, error_code: str, error_message: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Triage a quarantined record based on error type"""
        
        # Determine category from error code
        category = self._categorize_error(error_code, error_message)
        
        # Get triage rules for category
        rules = self.triage_rules.get(category.value, self.triage_rules["validation_failure"])
        
        # Calculate severity based on data impact
        severity = self._calculate_severity(category, raw_data, rules["severity"])
        
        # Determine if auto-remediation is possible
        auto_remediation = self._can_auto_remediate(category, error_code, raw_data, rules["auto_remediation"])
        
        return {
            "category": category,
            "severity": severity,
            "priority": rules["priority"],
            "auto_remediation": auto_remediation,
            "requires_review": rules["requires_review"],
            "estimated_time": rules["estimated_time"],
            "remediation_suggestions": self._get_remediation_suggestions(category, error_code, raw_data)
        }
    
    def _categorize_error(self, error_code: str, error_message: str) -> QuarantineCategory:
        """Categorize error based on code and message"""
        error_lower = error_message.lower()
        
        if "schema" in error_lower or "column" in error_lower:
            return QuarantineCategory.SCHEMA_VIOLATION
        elif "quality" in error_lower or "null" in error_lower:
            return QuarantineCategory.DATA_QUALITY
        elif "business" in error_lower or "rule" in error_lower:
            return QuarantineCategory.BUSINESS_RULE
        elif "format" in error_lower or "parse" in error_lower:
            return QuarantineCategory.FORMAT_ERROR
        elif "validation" in error_lower or "validate" in error_lower:
            return QuarantineCategory.VALIDATION_FAILURE
        elif "duplicate" in error_lower:
            return QuarantineCategory.DUPLICATE
        elif "missing" in error_lower or "required" in error_lower:
            return QuarantineCategory.MISSING_REQUIRED
        elif "range" in error_lower or "out of" in error_lower:
            return QuarantineCategory.OUT_OF_RANGE
        else:
            return QuarantineCategory.INVALID_FORMAT
    
    def _calculate_severity(self, category: QuarantineCategory, raw_data: Dict[str, Any], base_severity: QuarantineSeverity) -> QuarantineSeverity:
        """Calculate severity based on data impact"""
        
        # Check for critical fields
        critical_fields = ["hcpcs", "locality_code", "state_fips", "effective_from"]
        has_critical_fields = any(field in raw_data for field in critical_fields)
        
        # Check data volume impact
        data_size = len(str(raw_data))
        
        # Escalate severity for critical fields or large data
        if has_critical_fields and base_severity == QuarantineSeverity.MEDIUM:
            return QuarantineSeverity.HIGH
        elif data_size > 1000 and base_severity == QuarantineSeverity.LOW:
            return QuarantineSeverity.MEDIUM
        else:
            return base_severity
    
    def _can_auto_remediate(self, category: QuarantineCategory, error_code: str, raw_data: Dict[str, Any], base_auto: bool) -> bool:
        """Determine if record can be auto-remediated"""
        
        if not base_auto:
            return False
        
        # Check for complex remediation requirements
        if category in [QuarantineCategory.BUSINESS_RULE, QuarantineCategory.SCHEMA_VIOLATION]:
            return False
        
        # Check if data has enough information for remediation
        if len(raw_data) < 3:
            return False
        
        return True
    
    def _get_remediation_suggestions(self, category: QuarantineCategory, error_code: str, raw_data: Dict[str, Any]) -> List[str]:
        """Get remediation suggestions for the record"""
        suggestions = []
        
        if category == QuarantineCategory.FORMAT_ERROR:
            suggestions.append("Check data format and encoding")
            suggestions.append("Validate field lengths and types")
        elif category == QuarantineCategory.DATA_QUALITY:
            suggestions.append("Review null value handling")
            suggestions.append("Check data completeness")
        elif category == QuarantineCategory.DUPLICATE:
            suggestions.append("Remove duplicate records")
            suggestions.append("Merge or deduplicate data")
        elif category == QuarantineCategory.OUT_OF_RANGE:
            suggestions.append("Validate value ranges")
            suggestions.append("Check for data entry errors")
        elif category == QuarantineCategory.MISSING_REQUIRED:
            suggestions.append("Fill in missing required fields")
            suggestions.append("Check data source completeness")
        else:
            suggestions.append("Review error details and data context")
            suggestions.append("Consult business rules documentation")
        
        return suggestions


class QuarantineManager:
    """
    Manages quarantine workflow for DIS-compliant ingestors
    """
    
    def __init__(self, output_dir: str = "data/quarantine"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.triage = QuarantineTriage()
        self.quarantine_batches: List[QuarantineBatch] = []
        self.quarantine_records: List[QuarantineRecord] = []
    
    def quarantine_records(
        self,
        dataset_name: str,
        batch_id: str,
        release_id: str,
        validation_results: Dict[str, Any],
        raw_data: List[Dict[str, Any]]
    ) -> QuarantineBatch:
        """Quarantine records that failed validation"""
        
        quarantine_timestamp = datetime.utcnow()
        quarantined_records = []
        
        # Process each validation rule result
        for rule_result in validation_results.get("validation_rules", []):
            if rule_result.get("violations", 0) > 0:
                # Get reject data for this rule
                reject_data = self._get_reject_data(rule_result, raw_data)
                
                for reject_record in reject_data:
                    # Triage the record
                    triage_result = self.triage.triage_record(
                        error_code=rule_result.get("rule_id", "unknown"),
                        error_message=rule_result.get("message", "Validation failed"),
                        raw_data=reject_record
                    )
                    
                    # Create quarantine record
                    quarantine_record = QuarantineRecord(
                        record_id=self._generate_record_id(reject_record),
                        dataset_name=dataset_name,
                        batch_id=batch_id,
                        release_id=release_id,
                        quarantine_timestamp=quarantine_timestamp,
                        status=QuarantineStatus.NEW,
                        severity=triage_result["severity"],
                        category=triage_result["category"],
                        rule_id=rule_result.get("rule_id", "unknown"),
                        rule_name=rule_result.get("name", "Unknown Rule"),
                        error_message=rule_result.get("message", "Validation failed"),
                        error_code=rule_result.get("rule_id", "unknown"),
                        raw_data=reject_record,
                        remediation_actions=triage_result["remediation_suggestions"]
                    )
                    
                    quarantined_records.append(quarantine_record)
        
        # Create quarantine batch
        quarantine_batch = QuarantineBatch(
            batch_id=batch_id,
            dataset_name=dataset_name,
            release_id=release_id,
            quarantine_timestamp=quarantine_timestamp,
            total_records=len(quarantined_records),
            records=quarantined_records,
            summary=self._generate_batch_summary(quarantined_records),
            triage_priority=self._calculate_batch_priority(quarantined_records)
        )
        
        # Store quarantine batch
        self.quarantine_batches.append(quarantine_batch)
        self.quarantine_records.extend(quarantined_records)
        
        # Save to disk
        self._save_quarantine_batch(quarantine_batch)
        
        logger.info("Records quarantined",
                   dataset=dataset_name,
                   batch_id=batch_id,
                   total_records=len(quarantined_records),
                   priority=quarantine_batch.triage_priority)
        
        return quarantine_batch
    
    def _get_reject_data(self, rule_result: Dict[str, Any], raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get reject data for a specific rule"""
        # This would typically come from the validation engine
        # For now, return a sample of raw data as reject data
        return raw_data[:rule_result.get("violations", 0)]
    
    def _generate_record_id(self, raw_data: Dict[str, Any]) -> str:
        """Generate unique record ID for quarantine"""
        data_str = json.dumps(raw_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:16]
    
    def _generate_batch_summary(self, records: List[QuarantineRecord]) -> Dict[str, Any]:
        """Generate summary for quarantine batch"""
        summary = {
            "total_records": len(records),
            "by_status": {},
            "by_severity": {},
            "by_category": {},
            "by_rule": {}
        }
        
        for record in records:
            # Count by status
            status = record.status.value
            summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
            
            # Count by severity
            severity = record.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1
            
            # Count by category
            category = record.category.value
            summary["by_category"][category] = summary["by_category"].get(category, 0) + 1
            
            # Count by rule
            rule = record.rule_name
            summary["by_rule"][rule] = summary["by_rule"].get(rule, 0) + 1
        
        return summary
    
    def _calculate_batch_priority(self, records: List[QuarantineRecord]) -> str:
        """Calculate batch priority based on record severity"""
        if not records:
            return "low"
        
        severities = [record.severity for record in records]
        
        if QuarantineSeverity.CRITICAL in severities:
            return "critical"
        elif QuarantineSeverity.HIGH in severities:
            return "high"
        elif QuarantineSeverity.MEDIUM in severities:
            return "medium"
        else:
            return "low"
    
    def _save_quarantine_batch(self, batch: QuarantineBatch):
        """Save quarantine batch to disk"""
        batch_dir = self.output_dir / batch.dataset_name / batch.release_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Save batch metadata
        batch_file = batch_dir / f"quarantine_batch_{batch.batch_id}.json"
        with open(batch_file, 'w') as f:
            json.dump(asdict(batch), f, indent=2, default=str)
        
        # Save individual records
        records_dir = batch_dir / "records"
        records_dir.mkdir(exist_ok=True)
        
        for record in batch.records:
            record_file = records_dir / f"record_{record.record_id}.json"
            with open(record_file, 'w') as f:
                json.dump(asdict(record), f, indent=2, default=str)
    
    def get_quarantine_metrics(self) -> QuarantineMetrics:
        """Get quarantine system metrics"""
        total_quarantined = len(self.quarantine_records)
        
        # Calculate metrics by status
        by_status = {}
        by_severity = {}
        by_category = {}
        
        for record in self.quarantine_records:
            status = record.status.value
            severity = record.severity.value
            category = record.category.value
            
            by_status[status] = by_status.get(status, 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_category[category] = by_category.get(category, 0) + 1
        
        # Calculate remediation metrics
        remediated_count = by_status.get("remediated", 0)
        total_processed = total_quarantined - by_status.get("new", 0)
        remediation_success_rate = remediated_count / total_processed if total_processed > 0 else 0.0
        
        # Calculate escalation rate
        escalated_count = by_status.get("escalated", 0)
        escalation_rate = escalated_count / total_quarantined if total_quarantined > 0 else 0.0
        
        # Calculate oldest unresolved
        unresolved_records = [r for r in self.quarantine_records if r.status in [QuarantineStatus.NEW, QuarantineStatus.UNDER_REVIEW]]
        if unresolved_records:
            oldest_timestamp = min(r.quarantine_timestamp for r in unresolved_records)
            oldest_unresolved_days = (datetime.utcnow() - oldest_timestamp).total_seconds() / 86400
        else:
            oldest_unresolved_days = 0.0
        
        return QuarantineMetrics(
            total_quarantined=total_quarantined,
            by_status=by_status,
            by_severity=by_severity,
            by_category=by_category,
            avg_remediation_time_hours=0.0,  # Would be calculated from actual remediation times
            remediation_success_rate=remediation_success_rate,
            escalation_rate=escalation_rate,
            oldest_unresolved_days=oldest_unresolved_days
        )
    
    def get_quarantine_dashboard(self) -> Dict[str, Any]:
        """Get quarantine dashboard data"""
        metrics = self.get_quarantine_metrics()
        
        # Get recent batches
        recent_batches = sorted(self.quarantine_batches, key=lambda b: b.quarantine_timestamp, reverse=True)[:10]
        
        # Get high priority items
        high_priority = [r for r in self.quarantine_records if r.severity in [QuarantineSeverity.HIGH, QuarantineSeverity.CRITICAL]]
        
        return {
            "metrics": asdict(metrics),
            "recent_batches": [asdict(batch) for batch in recent_batches],
            "high_priority_items": len(high_priority),
            "requires_attention": len([r for r in self.quarantine_records if r.status == QuarantineStatus.NEW]),
            "last_updated": datetime.utcnow().isoformat()
        }
    
    def remediate_record(self, record_id: str, remediation_notes: str, remediated_by: str) -> bool:
        """Mark a record as remediated"""
        for record in self.quarantine_records:
            if record.record_id == record_id:
                record.status = QuarantineStatus.REMEDIATED
                record.remediation_notes = remediation_notes
                record.reviewed_by = remediated_by
                record.reviewed_at = datetime.utcnow()
                return True
        return False
    
    def escalate_record(self, record_id: str, escalation_reason: str, escalated_by: str) -> bool:
        """Escalate a record for higher-level review"""
        for record in self.quarantine_records:
            if record.record_id == record_id:
                record.status = QuarantineStatus.ESCALATED
                record.remediation_notes = f"ESCALATED: {escalation_reason}"
                record.reviewed_by = escalated_by
                record.reviewed_at = datetime.utcnow()
                return True
        return False


# Global quarantine manager instance
quarantine_manager = QuarantineManager()
