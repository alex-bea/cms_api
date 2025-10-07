"""
Alert System

Monitors system health and sends alerts for failures and anomalies
"""

import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release
from .run_manifest import RunManifestGenerator
from .anomaly_detector import AnomalyDetector
import logging

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Represents an alert"""
    alert_id: str
    alert_type: str  # 'ingestion_failure', 'anomaly_detected', 'performance_degradation', 'system_error'
    severity: str  # 'low', 'medium', 'high', 'critical'
    title: str
    description: str
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class AlertRule:
    """Defines when to trigger an alert"""
    rule_id: str
    rule_name: str
    alert_type: str
    severity: str
    condition: str  # 'error_count > 10', 'anomaly_severity == critical', etc.
    enabled: bool = True
    cooldown_minutes: int = 60  # Prevent spam


class AlertSystem:
    """Alert system for monitoring and notifications"""
    
    def __init__(self, config_file: str = "data/RVU/alerts/config.json"):
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.db = SessionLocal()
        self.manifest_generator = RunManifestGenerator()
        self.anomaly_detector = AnomalyDetector()
        
        # Load configuration
        self.config = self._load_config()
        
        # Load alert rules
        self.alert_rules = self._load_alert_rules()
        
        # Load existing alerts
        self.alerts = self._load_alerts()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load alert system configuration"""
        
        default_config = {
            "email": {
                "enabled": False,
                "smtp_server": "localhost",
                "smtp_port": 587,
                "username": "",
                "password": "",
                "from_address": "alerts@cms-pricing.com",
                "to_addresses": ["admin@cms-pricing.com"]
            },
            "slack": {
                "enabled": False,
                "webhook_url": "",
                "channel": "#alerts"
            },
            "webhook": {
                "enabled": False,
                "url": "",
                "headers": {}
            },
            "alert_retention_days": 30,
            "max_alerts_per_hour": 10
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                # Merge with defaults
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
            except Exception as e:
                logger.error(f"Failed to load config: {e}")
                return default_config
        
        # Save default config
        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def _load_alert_rules(self) -> List[AlertRule]:
        """Load alert rules"""
        
        default_rules = [
            AlertRule(
                rule_id="ingestion_failure",
                rule_name="Ingestion Failure",
                alert_type="ingestion_failure",
                severity="critical",
                condition="overall_status == 'failed'",
                cooldown_minutes=30
            ),
            AlertRule(
                rule_id="high_error_rate",
                rule_name="High Error Rate",
                alert_type="performance_degradation",
                severity="high",
                condition="total_errors > 100",
                cooldown_minutes=60
            ),
            AlertRule(
                rule_id="critical_anomaly",
                rule_name="Critical Anomaly Detected",
                alert_type="anomaly_detected",
                severity="critical",
                condition="anomaly_severity == 'critical'",
                cooldown_minutes=15
            ),
            AlertRule(
                rule_id="high_anomaly_count",
                rule_name="High Anomaly Count",
                alert_type="anomaly_detected",
                severity="medium",
                condition="anomaly_count > 50",
                cooldown_minutes=120
            ),
            AlertRule(
                rule_id="no_recent_ingestion",
                rule_name="No Recent Ingestion",
                alert_type="system_error",
                severity="high",
                condition="hours_since_last_ingestion > 24",
                cooldown_minutes=240
            )
        ]
        
        return default_rules
    
    def _load_alerts(self) -> List[Alert]:
        """Load existing alerts from file"""
        
        alerts_file = self.config_file.parent / "alerts.json"
        
        if not alerts_file.exists():
            return []
        
        try:
            with open(alerts_file, 'r') as f:
                alerts_data = json.load(f)
            
            alerts = []
            for alert_data in alerts_data:
                # Convert datetime strings back to datetime objects
                alert_data['timestamp'] = datetime.fromisoformat(alert_data['timestamp'])
                if alert_data.get('resolved_at'):
                    alert_data['resolved_at'] = datetime.fromisoformat(alert_data['resolved_at'])
                
                alerts.append(Alert(**alert_data))
            
            return alerts
        
        except Exception as e:
            logger.error(f"Failed to load alerts: {e}")
            return []
    
    def _save_alerts(self):
        """Save alerts to file"""
        
        alerts_file = self.config_file.parent / "alerts.json"
        
        # Convert to serializable format
        alerts_data = []
        for alert in self.alerts:
            alert_dict = asdict(alert)
            # Convert datetime objects to ISO format
            alert_dict['timestamp'] = alert_dict['timestamp'].isoformat()
            if alert_dict.get('resolved_at'):
                alert_dict['resolved_at'] = alert_dict['resolved_at'].isoformat()
            alerts_data.append(alert_dict)
        
        with open(alerts_file, 'w') as f:
            json.dump(alerts_data, f, indent=2)
    
    def check_alerts(self) -> List[Alert]:
        """Check for conditions that should trigger alerts"""
        
        print("🔔 Checking for alert conditions...")
        
        new_alerts = []
        
        # Get recent manifests
        recent_manifests = self.manifest_generator.get_run_history(limit=5)
        
        # Check each rule
        for rule in self.alert_rules:
            if not rule.enabled:
                continue
            
            # Check if we're in cooldown period
            if self._is_in_cooldown(rule):
                continue
            
            # Evaluate rule condition
            if self._evaluate_rule(rule, recent_manifests):
                alert = self._create_alert(rule, recent_manifests)
                if alert:
                    new_alerts.append(alert)
                    self.alerts.append(alert)
        
        # Check for anomalies
        anomaly_alerts = self._check_anomaly_alerts()
        new_alerts.extend(anomaly_alerts)
        self.alerts.extend(anomaly_alerts)
        
        # Save alerts
        if new_alerts:
            self._save_alerts()
        
        # Send notifications
        for alert in new_alerts:
            self._send_notification(alert)
        
        print(f"✅ Generated {len(new_alerts)} new alerts")
        
        return new_alerts
    
    def _is_in_cooldown(self, rule: AlertRule) -> bool:
        """Check if rule is in cooldown period"""
        
        cutoff_time = datetime.now() - timedelta(minutes=rule.cooldown_minutes)
        
        # Check if there's a recent alert of this type
        for alert in self.alerts:
            if (alert.alert_type == rule.alert_type and 
                alert.timestamp >= cutoff_time and 
                not alert.resolved):
                return True
        
        return False
    
    def _evaluate_rule(self, rule: AlertRule, manifests: List) -> bool:
        """Evaluate if a rule condition is met"""
        
        try:
            # Simple condition evaluation (in practice, would use a proper expression evaluator)
            if rule.condition == "overall_status == 'failed'":
                return any(m.overall_status == 'failed' for m in manifests)
            
            elif rule.condition == "total_errors > 100":
                return any(m.total_errors > 100 for m in manifests)
            
            elif rule.condition == "hours_since_last_ingestion > 24":
                if not manifests:
                    return True
                last_ingestion = manifests[0].started_at
                hours_since = (datetime.now() - last_ingestion).total_seconds() / 3600
                return hours_since > 24
            
            # Add more conditions as needed
            
        except Exception as e:
            logger.error(f"Failed to evaluate rule {rule.rule_id}: {e}")
        
        return False
    
    def _create_alert(self, rule: AlertRule, manifests: List) -> Optional[Alert]:
        """Create an alert based on a rule"""
        
        alert_id = f"{rule.rule_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Generate alert description based on rule type
        if rule.alert_type == "ingestion_failure":
            failed_manifests = [m for m in manifests if m.overall_status == 'failed']
            description = f"Ingestion failed for {len(failed_manifests)} recent runs"
            metadata = {"failed_runs": [m.run_id for m in failed_manifests]}
        
        elif rule.alert_type == "performance_degradation":
            high_error_manifests = [m for m in manifests if m.total_errors > 100]
            total_errors = sum(m.total_errors for m in high_error_manifests)
            description = f"High error rate detected: {total_errors} errors in recent runs"
            metadata = {"error_count": total_errors}
        
        elif rule.alert_type == "system_error":
            description = "No recent ingestion activity detected"
            metadata = {"last_ingestion": manifests[0].started_at.isoformat() if manifests else None}
        
        else:
            description = f"Alert condition met: {rule.condition}"
            metadata = {}
        
        return Alert(
            alert_id=alert_id,
            alert_type=rule.alert_type,
            severity=rule.severity,
            title=rule.rule_name,
            description=description,
            timestamp=datetime.now(),
            metadata=metadata
        )
    
    def _check_anomaly_alerts(self) -> List[Alert]:
        """Check for anomaly-based alerts"""
        
        anomaly_alerts = []
        
        # Get recent releases
        recent_releases = self.db.query(Release).order_by(Release.imported_at.desc()).limit(3).all()
        
        for release in recent_releases:
            try:
                anomalies = self.anomaly_detector.detect_anomalies(release.id)
                
                # Check for critical anomalies
                critical_anomalies = [a for a in anomalies if a.severity == 'critical']
                if critical_anomalies:
                    alert = Alert(
                        alert_id=f"anomaly_critical_{release.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        alert_type="anomaly_detected",
                        severity="critical",
                        title="Critical Anomaly Detected",
                        description=f"Found {len(critical_anomalies)} critical anomalies in release {release.source_version}",
                        timestamp=datetime.now(),
                        metadata={
                            "release_id": str(release.id),
                            "source_version": release.source_version,
                            "anomaly_count": len(critical_anomalies),
                            "anomalies": [asdict(a) for a in critical_anomalies]
                        }
                    )
                    anomaly_alerts.append(alert)
                
                # Check for high anomaly count
                if len(anomalies) > 50:
                    alert = Alert(
                        alert_id=f"anomaly_high_count_{release.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        alert_type="anomaly_detected",
                        severity="medium",
                        title="High Anomaly Count",
                        description=f"Found {len(anomalies)} anomalies in release {release.source_version}",
                        timestamp=datetime.now(),
                        metadata={
                            "release_id": str(release.id),
                            "source_version": release.source_version,
                            "anomaly_count": len(anomalies)
                        }
                    )
                    anomaly_alerts.append(alert)
            
            except Exception as e:
                logger.error(f"Failed to check anomalies for release {release.id}: {e}")
        
        return anomaly_alerts
    
    def _send_notification(self, alert: Alert):
        """Send notification for an alert"""
        
        print(f"📧 Sending alert notification: {alert.title}")
        
        # Email notification
        if self.config['email']['enabled']:
            self._send_email_alert(alert)
        
        # Slack notification
        if self.config['slack']['enabled']:
            self._send_slack_alert(alert)
        
        # Webhook notification
        if self.config['webhook']['enabled']:
            self._send_webhook_alert(alert)
    
    def _send_email_alert(self, alert: Alert):
        """Send email alert"""
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['from_address']
            msg['To'] = ', '.join(self.config['email']['to_addresses'])
            msg['Subject'] = f"[{alert.severity.upper()}] {alert.title}"
            
            body = f"""
Alert: {alert.title}
Severity: {alert.severity.upper()}
Time: {alert.timestamp.isoformat()}
Description: {alert.description}

Alert ID: {alert.alert_id}
Type: {alert.alert_type}

This is an automated alert from the RVU Data System.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port'])
            server.starttls()
            if self.config['email']['username']:
                server.login(self.config['email']['username'], self.config['email']['password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent: {alert.alert_id}")
        
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
    
    def _send_slack_alert(self, alert: Alert):
        """Send Slack alert"""
        
        try:
            import requests
            
            severity_colors = {
                'critical': '#dc3545',
                'high': '#fd7e14',
                'medium': '#ffc107',
                'low': '#6c757d'
            }
            
            payload = {
                "channel": self.config['slack']['channel'],
                "attachments": [{
                    "color": severity_colors.get(alert.severity, '#6c757d'),
                    "title": alert.title,
                    "text": alert.description,
                    "fields": [
                        {"title": "Severity", "value": alert.severity.upper(), "short": True},
                        {"title": "Type", "value": alert.alert_type, "short": True},
                        {"title": "Time", "value": alert.timestamp.isoformat(), "short": False}
                    ],
                    "footer": "RVU Data System",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }
            
            response = requests.post(self.config['slack']['webhook_url'], json=payload)
            response.raise_for_status()
            
            logger.info(f"Slack alert sent: {alert.alert_id}")
        
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    def _send_webhook_alert(self, alert: Alert):
        """Send webhook alert"""
        
        try:
            import requests
            
            payload = asdict(alert)
            # Convert datetime to ISO format
            payload['timestamp'] = alert.timestamp.isoformat()
            if payload.get('resolved_at'):
                payload['resolved_at'] = alert.resolved_at.isoformat()
            
            headers = self.config['webhook']['headers']
            headers['Content-Type'] = 'application/json'
            
            response = requests.post(self.config['webhook']['url'], json=payload, headers=headers)
            response.raise_for_status()
            
            logger.info(f"Webhook alert sent: {alert.alert_id}")
        
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        
        for alert in self.alerts:
            if alert.alert_id == alert_id and not alert.resolved:
                alert.resolved = True
                alert.resolved_at = datetime.now()
                self._save_alerts()
                logger.info(f"Alert resolved: {alert_id}")
                return True
        
        return False
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active (unresolved) alerts"""
        
        return [alert for alert in self.alerts if not alert.resolved]
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary statistics"""
        
        total_alerts = len(self.alerts)
        active_alerts = len(self.get_active_alerts())
        resolved_alerts = total_alerts - active_alerts
        
        # Group by severity
        by_severity = {}
        for alert in self.alerts:
            if alert.severity not in by_severity:
                by_severity[alert.severity] = 0
            by_severity[alert.severity] += 1
        
        # Group by type
        by_type = {}
        for alert in self.alerts:
            if alert.alert_type not in by_type:
                by_type[alert.alert_type] = 0
            by_type[alert.alert_type] += 1
        
        return {
            "total_alerts": total_alerts,
            "active_alerts": active_alerts,
            "resolved_alerts": resolved_alerts,
            "by_severity": by_severity,
            "by_type": by_type,
            "recent_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "title": alert.title,
                    "severity": alert.severity,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved
                }
                for alert in sorted(self.alerts, key=lambda x: x.timestamp, reverse=True)[:10]
            ]
        }
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.manifest_generator.close()
        self.anomaly_detector.close()



