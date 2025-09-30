"""
Operational Dashboard

Real-time monitoring of system health, ingestion status, and performance metrics
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
from .run_manifest import RunManifestGenerator
from .anomaly_detector import AnomalyDetector
import logging

logger = logging.getLogger(__name__)


@dataclass
class SystemHealth:
    """System health status"""
    status: str  # 'healthy', 'degraded', 'critical'
    last_ingestion: Optional[datetime]
    total_releases: int
    total_rvu_items: int
    total_gpci_items: int
    total_oppscap_items: int
    total_anes_items: int
    total_locco_items: int
    recent_errors: int
    recent_warnings: int
    uptime_hours: float


@dataclass
class PerformanceMetrics:
    """Performance metrics"""
    avg_ingestion_time_seconds: float
    avg_query_time_ms: float
    total_data_size_mb: float
    index_utilization: float
    cache_hit_rate: float
    error_rate: float


class OperationalDashboard:
    """Operational dashboard for monitoring system health"""
    
    def __init__(self, output_dir: str = "data/RVU/dashboard"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db = SessionLocal()
        self.manifest_generator = RunManifestGenerator()
        self.anomaly_detector = AnomalyDetector()
    
    def get_system_health(self) -> SystemHealth:
        """Get current system health status"""
        
        # Get latest release
        latest_release = self.db.query(Release).order_by(Release.imported_at.desc()).first()
        
        # Count total records
        total_releases = self.db.query(Release).count()
        total_rvu_items = self.db.query(RVUItem).count()
        total_gpci_items = self.db.query(GPCIIndex).count()
        total_oppscap_items = self.db.query(OPPSCap).count()
        total_anes_items = self.db.query(AnesCF).count()
        total_locco_items = self.db.query(LocalityCounty).count()
        
        # Get recent errors/warnings from manifests
        recent_manifests = self.manifest_generator.get_run_history(limit=5)
        recent_errors = sum(m.total_errors for m in recent_manifests)
        recent_warnings = sum(m.total_warnings for m in recent_manifests)
        
        # Determine system status
        status = "healthy"
        if recent_errors > 100:
            status = "critical"
        elif recent_errors > 10 or recent_warnings > 100:
            status = "degraded"
        
        # Calculate uptime (simplified - would track actual system start time)
        uptime_hours = 24.0  # Placeholder
        
        return SystemHealth(
            status=status,
            last_ingestion=latest_release.imported_at if latest_release else None,
            total_releases=total_releases,
            total_rvu_items=total_rvu_items,
            total_gpci_items=total_gpci_items,
            total_oppscap_items=total_oppscap_items,
            total_anes_items=total_anes_items,
            total_locco_items=total_locco_items,
            recent_errors=recent_errors,
            recent_warnings=recent_warnings,
            uptime_hours=uptime_hours
        )
    
    def get_performance_metrics(self) -> PerformanceMetrics:
        """Get current performance metrics"""
        
        # Get recent manifests for performance data
        recent_manifests = self.manifest_generator.get_run_history(limit=10)
        
        if not recent_manifests:
            return PerformanceMetrics(
                avg_ingestion_time_seconds=0.0,
                avg_query_time_ms=0.0,
                total_data_size_mb=0.0,
                index_utilization=0.0,
                cache_hit_rate=0.0,
                error_rate=0.0
            )
        
        # Calculate average ingestion time
        avg_ingestion_time = sum(m.total_duration_seconds for m in recent_manifests) / len(recent_manifests)
        
        # Calculate error rate
        total_runs = len(recent_manifests)
        failed_runs = len([m for m in recent_manifests if m.overall_status == 'failed'])
        error_rate = failed_runs / total_runs if total_runs > 0 else 0.0
        
        # Estimate data size (simplified calculation)
        total_rows = sum(m.total_rows for m in recent_manifests)
        estimated_size_mb = total_rows * 0.001  # Rough estimate: 1KB per row
        
        return PerformanceMetrics(
            avg_ingestion_time_seconds=avg_ingestion_time,
            avg_query_time_ms=50.0,  # Placeholder - would measure actual query times
            total_data_size_mb=estimated_size_mb,
            index_utilization=0.85,  # Placeholder - would calculate actual index usage
            cache_hit_rate=0.92,  # Placeholder - would measure cache performance
            error_rate=error_rate
        )
    
    def get_recent_activity(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent system activity"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Get recent releases
        recent_releases = self.db.query(Release).filter(
            Release.imported_at >= cutoff_time
        ).order_by(Release.imported_at.desc()).all()
        
        activities = []
        for release in recent_releases:
            activities.append({
                "timestamp": release.imported_at.isoformat(),
                "type": "ingestion",
                "release_id": str(release.id),
                "source_version": release.source_version,
                "status": "completed" if release.published_at else "in_progress",
                "description": f"Ingested {release.source_version} data"
            })
        
        # Get recent manifests for more detailed activity
        recent_manifests = self.manifest_generator.get_run_history(limit=20)
        for manifest in recent_manifests:
            if manifest.started_at >= cutoff_time:
                activities.append({
                    "timestamp": manifest.started_at.isoformat(),
                    "type": "run",
                    "run_id": manifest.run_id,
                    "status": manifest.overall_status,
                    "duration": manifest.total_duration_seconds,
                    "rows": manifest.total_rows,
                    "errors": manifest.total_errors,
                    "description": f"Run {manifest.run_id} - {manifest.total_rows:,} rows, {manifest.total_errors} errors"
                })
        
        # Sort by timestamp
        activities.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return activities
    
    def get_anomaly_summary(self) -> Dict[str, Any]:
        """Get summary of recent anomalies"""
        
        # Get recent releases
        recent_releases = self.db.query(Release).order_by(Release.imported_at.desc()).limit(5).all()
        
        total_anomalies = 0
        critical_anomalies = 0
        high_anomalies = 0
        medium_anomalies = 0
        low_anomalies = 0
        
        anomaly_details = []
        
        for release in recent_releases:
            try:
                anomalies = self.anomaly_detector.detect_anomalies(release.id)
                total_anomalies += len(anomalies)
                
                for anomaly in anomalies:
                    if anomaly.severity == 'critical':
                        critical_anomalies += 1
                    elif anomaly.severity == 'high':
                        high_anomalies += 1
                    elif anomaly.severity == 'medium':
                        medium_anomalies += 1
                    else:
                        low_anomalies += 1
                    
                    anomaly_details.append({
                        "release_id": str(release.id),
                        "source_version": release.source_version,
                        "type": anomaly.anomaly_type,
                        "severity": anomaly.severity,
                        "description": anomaly.description,
                        "affected_records": anomaly.affected_records,
                        "dataset": anomaly.dataset
                    })
            
            except Exception as e:
                logger.error(f"Failed to detect anomalies for release {release.id}: {e}")
        
        return {
            "total_anomalies": total_anomalies,
            "by_severity": {
                "critical": critical_anomalies,
                "high": high_anomalies,
                "medium": medium_anomalies,
                "low": low_anomalies
            },
            "recent_anomalies": anomaly_details[:10]  # Last 10 anomalies
        }
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """Generate complete dashboard data"""
        
        print("📊 Generating operational dashboard data...")
        
        system_health = self.get_system_health()
        performance_metrics = self.get_performance_metrics()
        recent_activity = self.get_recent_activity()
        anomaly_summary = self.get_anomaly_summary()
        
        dashboard_data = {
            "generated_at": datetime.now().isoformat(),
            "system_health": asdict(system_health),
            "performance_metrics": asdict(performance_metrics),
            "recent_activity": recent_activity,
            "anomaly_summary": anomaly_summary
        }
        
        # Convert datetime objects to ISO format
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj
        
        def convert_recursive(obj):
            if isinstance(obj, dict):
                return {k: convert_recursive(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_recursive(item) for item in obj]
            else:
                return convert_datetime(obj)
        
        dashboard_data = convert_recursive(dashboard_data)
        
        # Save dashboard data
        dashboard_file = self.output_dir / "dashboard_data.json"
        with open(dashboard_file, 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
        print(f"✅ Dashboard data saved to {dashboard_file}")
        
        return dashboard_data
    
    def generate_html_dashboard(self) -> str:
        """Generate HTML dashboard"""
        
        dashboard_data = self.generate_dashboard_data()
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RVU Data System Dashboard</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .status-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            color: white;
            margin: 10px 0;
        }}
        .status-healthy {{ background-color: #28a745; }}
        .status-degraded {{ background-color: #ffc107; color: #212529; }}
        .status-critical {{ background-color: #dc3545; }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metric-card h3 {{
            margin-top: 0;
            color: #333;
        }}
        .metric-value {{
            font-size: 2em;
            font-weight: bold;
            color: #007bff;
        }}
        .activity-list {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .activity-item {{
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }}
        .activity-item:last-child {{
            border-bottom: none;
        }}
        .activity-timestamp {{
            color: #666;
            font-size: 0.9em;
        }}
        .anomaly-summary {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .anomaly-item {{
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }}
        .severity-critical {{ color: #dc3545; font-weight: bold; }}
        .severity-high {{ color: #fd7e14; font-weight: bold; }}
        .severity-medium {{ color: #ffc107; }}
        .severity-low {{ color: #6c757d; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>RVU Data System Dashboard</h1>
            <div class="status-badge status-{dashboard_data['system_health']['status']}">
                System Status: {dashboard_data['system_health']['status'].upper()}
            </div>
            <p>Last updated: {dashboard_data['generated_at']}</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Data Volume</h3>
                <div class="metric-value">{dashboard_data['system_health']['total_rvu_items']:,}</div>
                <p>RVU Items</p>
                <div class="metric-value">{dashboard_data['system_health']['total_gpci_items']:,}</div>
                <p>GPCI Items</p>
            </div>
            
            <div class="metric-card">
                <h3>Performance</h3>
                <div class="metric-value">{dashboard_data['performance_metrics']['avg_ingestion_time_seconds']:.1f}s</div>
                <p>Avg Ingestion Time</p>
                <div class="metric-value">{dashboard_data['performance_metrics']['avg_query_time_ms']:.1f}ms</div>
                <p>Avg Query Time</p>
            </div>
            
            <div class="metric-card">
                <h3>Data Quality</h3>
                <div class="metric-value">{dashboard_data['system_health']['recent_errors']}</div>
                <p>Recent Errors</p>
                <div class="metric-value">{dashboard_data['system_health']['recent_warnings']}</div>
                <p>Recent Warnings</p>
            </div>
            
            <div class="metric-card">
                <h3>System Health</h3>
                <div class="metric-value">{dashboard_data['system_health']['total_releases']}</div>
                <p>Total Releases</p>
                <div class="metric-value">{dashboard_data['system_health']['uptime_hours']:.1f}h</div>
                <p>Uptime</p>
            </div>
        </div>
        
        <div class="activity-list">
            <h3>Recent Activity</h3>
            {self._generate_activity_html(dashboard_data['recent_activity'])}
        </div>
        
        <div class="anomaly-summary">
            <h3>Anomaly Summary</h3>
            <p>Total Anomalies: {dashboard_data['anomaly_summary']['total_anomalies']}</p>
            <div style="margin: 10px 0;">
                <span class="severity-critical">Critical: {dashboard_data['anomaly_summary']['by_severity']['critical']}</span> |
                <span class="severity-high">High: {dashboard_data['anomaly_summary']['by_severity']['high']}</span> |
                <span class="severity-medium">Medium: {dashboard_data['anomaly_summary']['by_severity']['medium']}</span> |
                <span class="severity-low">Low: {dashboard_data['anomaly_summary']['by_severity']['low']}</span>
            </div>
            {self._generate_anomaly_html(dashboard_data['anomaly_summary']['recent_anomalies'])}
        </div>
    </div>
</body>
</html>
        """
        
        # Save HTML dashboard
        dashboard_file = self.output_dir / "dashboard.html"
        with open(dashboard_file, 'w') as f:
            f.write(html_content)
        
        print(f"✅ HTML dashboard saved to {dashboard_file}")
        
        return str(dashboard_file)
    
    def _generate_activity_html(self, activities: List[Dict[str, Any]]) -> str:
        """Generate HTML for activity list"""
        
        if not activities:
            return "<p>No recent activity</p>"
        
        html = ""
        for activity in activities[:10]:  # Show last 10 activities
            status_class = "status-" + activity.get('status', 'unknown')
            html += f"""
            <div class="activity-item">
                <div class="activity-timestamp">{activity['timestamp']}</div>
                <strong>{activity['type'].title()}</strong> - {activity.get('description', 'No description')}
            </div>
            """
        
        return html
    
    def _generate_anomaly_html(self, anomalies: List[Dict[str, Any]]) -> str:
        """Generate HTML for anomaly list"""
        
        if not anomalies:
            return "<p>No recent anomalies</p>"
        
        html = ""
        for anomaly in anomalies[:5]:  # Show last 5 anomalies
            severity_class = f"severity-{anomaly['severity']}"
            html += f"""
            <div class="anomaly-item">
                <span class="{severity_class}">{anomaly['severity'].upper()}</span> - 
                {anomaly['description']} ({anomaly['affected_records']} records)
            </div>
            """
        
        return html
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.manifest_generator.close()
        self.anomaly_detector.close()

