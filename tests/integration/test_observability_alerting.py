#!/usr/bin/env python3
"""
OPPS Scraper Observability & Alerting Tests
============================================

Tests for observability and alerting following QTS v1.1 standards.
Includes five-pillar dashboard, alerting rules, and ownership routing.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import pytest
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from unittest.mock import Mock, patch
from dataclasses import dataclass


@dataclass
class DashboardMetric:
    """Dashboard metric data structure."""
    metric_name: str
    value: float
    timestamp: datetime
    tags: Dict[str, str]
    unit: str


@dataclass
class AlertRule:
    """Alert rule data structure."""
    rule_name: str
    condition: str
    threshold: float
    severity: str
    owner: str
    channel: str


class TestOPPSScraperObservability:
    """Tests for OPPS scraper observability."""
    
    @pytest.fixture
    def sample_metrics(self):
        """Sample metrics for testing."""
        return {
            'freshness': {
                'last_successful_publish': datetime.now(timezone.utc) - timedelta(hours=2),
                'expected_cadence_hours': 24,
                'grace_window_hours': 3
            },
            'volume': {
                'current_rows': 120345,
                'expected_rows': 125000,
                'previous_vintage_rows': 118500
            },
            'schema': {
                'current_version': '1.0.0',
                'registered_version': '1.0.0',
                'drift_detected': False,
                'new_fields': [],
                'removed_fields': []
            },
            'quality': {
                'null_rates': {
                    'hcpcs': 0.0,
                    'modifier': 15.2,
                    'apc_code': 0.0,
                    'payment_rate': 0.0
                },
                'range_checks': {
                    'payment_rate_min': 0.0,
                    'payment_rate_max': 999999.99,
                    'violations': 0
                },
                'uniqueness_violations': 0,
                'categorical_distributions': {
                    'hcpcs_length_5': 95.5,
                    'hcpcs_length_other': 4.5
                }
            },
            'lineage': {
                'upstream_status': 'success',
                'downstream_consumers': [
                    {'name': 'opps_payment_rates_api', 'status': 'success', 'last_run': datetime.now(timezone.utc) - timedelta(minutes=5)},
                    {'name': 'medicare_cost_analysis', 'status': 'success', 'last_run': datetime.now(timezone.utc) - timedelta(minutes=10)}
                ]
            }
        }
    
    def test_five_pillar_dashboard_freshness(self, sample_metrics):
        """Test freshness pillar dashboard metrics."""
        freshness = sample_metrics['freshness']
        
        # Calculate freshness metrics
        last_publish = freshness['last_successful_publish']
        expected_cadence = freshness['expected_cadence_hours']
        grace_window = freshness['grace_window_hours']
        
        hours_since_publish = (datetime.now(timezone.utc) - last_publish).total_seconds() / 3600
        sla_threshold = expected_cadence + grace_window
        
        # Test freshness calculation
        assert hours_since_publish < sla_threshold, f"Freshness breach: {hours_since_publish:.1f}h > {sla_threshold}h"
        
        # Test freshness status
        if hours_since_publish > expected_cadence:
            freshness_status = "warning"
        elif hours_since_publish > sla_threshold:
            freshness_status = "critical"
        else:
            freshness_status = "healthy"
        
        assert freshness_status in ["healthy", "warning", "critical"]
        
        # Test freshness dashboard widget
        freshness_widget = {
            'metric_name': 'freshness_hours',
            'value': hours_since_publish,
            'status': freshness_status,
            'threshold': sla_threshold,
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'metric_name' in freshness_widget
        assert 'value' in freshness_widget
        assert 'status' in freshness_widget
        assert 'threshold' in freshness_widget
        assert 'last_update' in freshness_widget
    
    def test_five_pillar_dashboard_volume(self, sample_metrics):
        """Test volume pillar dashboard metrics."""
        volume = sample_metrics['volume']
        
        current_rows = volume['current_rows']
        expected_rows = volume['expected_rows']
        previous_rows = volume['previous_vintage_rows']
        
        # Calculate volume drift
        expected_drift_pct = ((current_rows - expected_rows) / expected_rows) * 100
        previous_drift_pct = ((current_rows - previous_rows) / previous_rows) * 100
        
        # Test volume thresholds
        warn_threshold = 15  # ±15% warn
        fail_threshold = 30  # ±30% fail
        
        if abs(expected_drift_pct) > fail_threshold:
            volume_status = "critical"
        elif abs(expected_drift_pct) > warn_threshold:
            volume_status = "warning"
        else:
            volume_status = "healthy"
        
        assert volume_status in ["healthy", "warning", "critical"]
        
        # Test volume dashboard widget
        volume_widget = {
            'metric_name': 'volume_drift_pct',
            'value': expected_drift_pct,
            'status': volume_status,
            'current_rows': current_rows,
            'expected_rows': expected_rows,
            'previous_rows': previous_rows,
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'metric_name' in volume_widget
        assert 'value' in volume_widget
        assert 'status' in volume_widget
        assert 'current_rows' in volume_widget
        assert 'expected_rows' in volume_widget
        assert 'previous_rows' in volume_widget
    
    def test_five_pillar_dashboard_schema(self, sample_metrics):
        """Test schema pillar dashboard metrics."""
        schema = sample_metrics['schema']
        
        current_version = schema['current_version']
        registered_version = schema['registered_version']
        drift_detected = schema['drift_detected']
        new_fields = schema['new_fields']
        removed_fields = schema['removed_fields']
        
        # Test schema drift detection
        if drift_detected or new_fields or removed_fields:
            schema_status = "critical"
        elif current_version != registered_version:
            schema_status = "warning"
        else:
            schema_status = "healthy"
        
        assert schema_status in ["healthy", "warning", "critical"]
        
        # Test schema dashboard widget
        schema_widget = {
            'metric_name': 'schema_drift',
            'status': schema_status,
            'current_version': current_version,
            'registered_version': registered_version,
            'drift_detected': drift_detected,
            'new_fields': new_fields,
            'removed_fields': removed_fields,
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'metric_name' in schema_widget
        assert 'status' in schema_widget
        assert 'current_version' in schema_widget
        assert 'registered_version' in schema_widget
        assert 'drift_detected' in schema_widget
    
    def test_five_pillar_dashboard_quality(self, sample_metrics):
        """Test quality pillar dashboard metrics."""
        quality = sample_metrics['quality']
        
        null_rates = quality['null_rates']
        range_checks = quality['range_checks']
        uniqueness_violations = quality['uniqueness_violations']
        categorical_distributions = quality['categorical_distributions']
        
        # Test null rate thresholds
        critical_null_threshold = 5.0  # 5% critical
        warning_null_threshold = 2.0   # 2% warning
        
        quality_status = "healthy"
        quality_issues = []
        
        for field, null_rate in null_rates.items():
            if null_rate > critical_null_threshold:
                quality_status = "critical"
                quality_issues.append(f"{field} null rate {null_rate}% > {critical_null_threshold}%")
            elif null_rate > warning_null_threshold:
                if quality_status == "healthy":
                    quality_status = "warning"
                quality_issues.append(f"{field} null rate {null_rate}% > {warning_null_threshold}%")
        
        # Test range check violations
        if range_checks['violations'] > 0:
            quality_status = "critical"
            quality_issues.append(f"Range check violations: {range_checks['violations']}")
        
        # Test uniqueness violations
        if uniqueness_violations > 0:
            quality_status = "critical"
            quality_issues.append(f"Uniqueness violations: {uniqueness_violations}")
        
        assert quality_status in ["healthy", "warning", "critical"]
        
        # Test quality dashboard widget
        quality_widget = {
            'metric_name': 'quality_score',
            'status': quality_status,
            'issues': quality_issues,
            'null_rates': null_rates,
            'range_checks': range_checks,
            'uniqueness_violations': uniqueness_violations,
            'categorical_distributions': categorical_distributions,
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'metric_name' in quality_widget
        assert 'status' in quality_widget
        assert 'issues' in quality_widget
        assert 'null_rates' in quality_widget
        assert 'range_checks' in quality_widget
    
    def test_five_pillar_dashboard_lineage(self, sample_metrics):
        """Test lineage pillar dashboard metrics."""
        lineage = sample_metrics['lineage']
        
        upstream_status = lineage['upstream_status']
        downstream_consumers = lineage['downstream_consumers']
        
        # Test upstream status
        if upstream_status != 'success':
            lineage_status = "critical"
        else:
            # Check downstream consumers
            failed_consumers = [c for c in downstream_consumers if c['status'] != 'success']
            if failed_consumers:
                lineage_status = "warning"
            else:
                lineage_status = "healthy"
        
        assert lineage_status in ["healthy", "warning", "critical"]
        
        # Test lineage dashboard widget
        lineage_widget = {
            'metric_name': 'lineage_health',
            'status': lineage_status,
            'upstream_status': upstream_status,
            'downstream_consumers': downstream_consumers,
            'failed_consumers': [c for c in downstream_consumers if c['status'] != 'success'],
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'metric_name' in lineage_widget
        assert 'status' in lineage_widget
        assert 'upstream_status' in lineage_widget
        assert 'downstream_consumers' in lineage_widget
    
    def test_alerting_rules_pager(self, sample_metrics):
        """Test pager alerting rules."""
        alert_rules = [
            AlertRule(
                rule_name="freshness_breach",
                condition="freshness_hours > sla_threshold",
                threshold=27.0,  # 24h + 3h grace
                severity="critical",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-alerts"
            ),
            AlertRule(
                rule_name="schema_drift",
                condition="schema_drift_detected == true",
                threshold=0.0,
                severity="critical",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-alerts"
            ),
            AlertRule(
                rule_name="quality_fail",
                condition="quality_score < 95.0",
                threshold=95.0,
                severity="critical",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-alerts"
            ),
            AlertRule(
                rule_name="failed_publish",
                condition="publish_outcome == 'failed'",
                threshold=0.0,
                severity="critical",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-alerts"
            )
        ]
        
        # Test alert rule structure
        for rule in alert_rules:
            assert hasattr(rule, 'rule_name')
            assert hasattr(rule, 'condition')
            assert hasattr(rule, 'threshold')
            assert hasattr(rule, 'severity')
            assert hasattr(rule, 'owner')
            assert hasattr(rule, 'channel')
            
            assert rule.severity == "critical"
            assert rule.owner == "Platform/Data Engineering"
            assert rule.channel == "#cms-pricing-alerts"
        
        # Test alert evaluation
        test_metrics = {
            'freshness_hours': 30.0,  # Should trigger freshness breach
            'schema_drift_detected': False,  # Should not trigger schema drift
            'quality_score': 98.5,  # Should not trigger quality fail
            'publish_outcome': 'success'  # Should not trigger failed publish
        }
        
        triggered_alerts = []
        for rule in alert_rules:
            if rule.rule_name == "freshness_breach" and test_metrics['freshness_hours'] > rule.threshold:
                triggered_alerts.append(rule)
            elif rule.rule_name == "schema_drift" and test_metrics['schema_drift_detected']:
                triggered_alerts.append(rule)
            elif rule.rule_name == "quality_fail" and test_metrics['quality_score'] < rule.threshold:
                triggered_alerts.append(rule)
            elif rule.rule_name == "failed_publish" and test_metrics['publish_outcome'] == 'failed':
                triggered_alerts.append(rule)
        
        # Should trigger freshness breach alert
        assert len(triggered_alerts) == 1
        assert triggered_alerts[0].rule_name == "freshness_breach"
    
    def test_alerting_rules_slack(self, sample_metrics):
        """Test Slack alerting rules."""
        slack_alert_rules = [
            AlertRule(
                rule_name="volume_drift_warning",
                condition="abs(volume_drift_pct) > 15",
                threshold=15.0,
                severity="warning",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-warnings"
            ),
            AlertRule(
                rule_name="quality_warning",
                condition="quality_score < 98.0",
                threshold=98.0,
                severity="warning",
                owner="Platform/Data Engineering",
                channel="#cms-pricing-warnings"
            )
        ]
        
        # Test Slack alert rule structure
        for rule in slack_alert_rules:
            assert rule.severity == "warning"
            assert rule.channel == "#cms-pricing-warnings"
        
        # Test alert evaluation
        test_metrics = {
            'volume_drift_pct': 20.0,  # Should trigger volume drift warning
            'quality_score': 97.5  # Should trigger quality warning
        }
        
        triggered_slack_alerts = []
        for rule in slack_alert_rules:
            if rule.rule_name == "volume_drift_warning" and abs(test_metrics['volume_drift_pct']) > rule.threshold:
                triggered_slack_alerts.append(rule)
            elif rule.rule_name == "quality_warning" and test_metrics['quality_score'] < rule.threshold:
                triggered_slack_alerts.append(rule)
        
        # Should trigger both warnings
        assert len(triggered_slack_alerts) == 2
        assert any(rule.rule_name == "volume_drift_warning" for rule in triggered_slack_alerts)
        assert any(rule.rule_name == "quality_warning" for rule in triggered_slack_alerts)
    
    def test_ownership_routing(self, sample_metrics):
        """Test ownership routing for alerts."""
        ownership_config = {
            'dataset_owner': 'Platform/Data Engineering',
            'data_steward': 'Medicare SME',
            'escalation_channel': '#cms-pricing-alerts',
            'on_call_rotation': 'platform-team',
            'business_hours': '9AM-5PM EST',
            'escalation_timeout': '30 minutes'
        }
        
        # Test ownership structure
        assert 'dataset_owner' in ownership_config
        assert 'data_steward' in ownership_config
        assert 'escalation_channel' in ownership_config
        assert 'on_call_rotation' in ownership_config
        
        # Test alert routing
        alert_routing = {
            'critical_alerts': {
                'primary': ownership_config['dataset_owner'],
                'secondary': ownership_config['data_steward'],
                'channel': ownership_config['escalation_channel'],
                'escalation_timeout': ownership_config['escalation_timeout']
            },
            'warning_alerts': {
                'primary': ownership_config['data_steward'],
                'channel': '#cms-pricing-warnings',
                'escalation_timeout': '2 hours'
            }
        }
        
        assert 'critical_alerts' in alert_routing
        assert 'warning_alerts' in alert_routing
        
        # Test critical alert routing
        critical_routing = alert_routing['critical_alerts']
        assert critical_routing['primary'] == 'Platform/Data Engineering'
        assert critical_routing['secondary'] == 'Medicare SME'
        assert critical_routing['channel'] == '#cms-pricing-alerts'
        
        # Test warning alert routing
        warning_routing = alert_routing['warning_alerts']
        assert warning_routing['primary'] == 'Medicare SME'
        assert warning_routing['channel'] == '#cms-pricing-warnings'
    
    def test_dashboard_aggregation(self, sample_metrics):
        """Test dashboard metric aggregation."""
        # Aggregate metrics from last three vintages
        vintage_metrics = [
            {
                'vintage': '2025Q1',
                'freshness_hours': 2.5,
                'volume_drift_pct': 5.2,
                'quality_score': 98.5,
                'schema_status': 'healthy'
            },
            {
                'vintage': '2024Q4',
                'freshness_hours': 1.8,
                'volume_drift_pct': -3.1,
                'quality_score': 97.8,
                'schema_status': 'healthy'
            },
            {
                'vintage': '2024Q3',
                'freshness_hours': 3.2,
                'volume_drift_pct': 8.7,
                'quality_score': 96.9,
                'schema_status': 'warning'
            }
        ]
        
        # Calculate aggregated metrics
        aggregated = {
            'avg_freshness_hours': sum(v['freshness_hours'] for v in vintage_metrics) / len(vintage_metrics),
            'avg_volume_drift_pct': sum(v['volume_drift_pct'] for v in vintage_metrics) / len(vintage_metrics),
            'avg_quality_score': sum(v['quality_score'] for v in vintage_metrics) / len(vintage_metrics),
            'schema_health_rate': sum(1 for v in vintage_metrics if v['schema_status'] == 'healthy') / len(vintage_metrics) * 100
        }
        
        # Test aggregated metrics
        assert 'avg_freshness_hours' in aggregated
        assert 'avg_volume_drift_pct' in aggregated
        assert 'avg_quality_score' in aggregated
        assert 'schema_health_rate' in aggregated
        
        # Test aggregated values
        assert 0 < aggregated['avg_freshness_hours'] < 24
        assert -20 < aggregated['avg_volume_drift_pct'] < 20
        assert 90 < aggregated['avg_quality_score'] < 100
        assert 0 <= aggregated['schema_health_rate'] <= 100
        
        # Test dashboard widget
        dashboard_widget = {
            'title': 'OPPS Scraper Health (Last 3 Vintages)',
            'metrics': aggregated,
            'vintage_count': len(vintage_metrics),
            'last_update': datetime.now(timezone.utc).isoformat()
        }
        
        assert 'title' in dashboard_widget
        assert 'metrics' in dashboard_widget
        assert 'vintage_count' in dashboard_widget
        assert 'last_update' in dashboard_widget
    
    def test_observability_metrics_export(self, sample_metrics):
        """Test observability metrics export."""
        # Export metrics to observability collector
        metrics_export = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'dataset': 'cms_opps_quarterly_addenda',
            'pipeline': 'cms_opps_scraper',
            'metrics': {
                'freshness_hours': 2.5,
                'volume_drift_pct': 5.2,
                'quality_score': 98.5,
                'schema_drift_detected': False,
                'lineage_health': 'healthy'
            },
            'tags': {
                'environment': 'production',
                'quarter': '2025Q1',
                'version': '1.0.0'
            }
        }
        
        # Test export structure
        assert 'timestamp' in metrics_export
        assert 'dataset' in metrics_export
        assert 'pipeline' in metrics_export
        assert 'metrics' in metrics_export
        assert 'tags' in metrics_export
        
        # Test metrics structure
        metrics = metrics_export['metrics']
        assert 'freshness_hours' in metrics
        assert 'volume_drift_pct' in metrics
        assert 'quality_score' in metrics
        assert 'schema_drift_detected' in metrics
        assert 'lineage_health' in metrics
        
        # Test tags structure
        tags = metrics_export['tags']
        assert 'environment' in tags
        assert 'quarter' in tags
        assert 'version' in tags
        
        # Test JSON serialization
        json_export = json.dumps(metrics_export, indent=2)
        assert isinstance(json_export, str)
        
        # Test JSON deserialization
        parsed_export = json.loads(json_export)
        assert parsed_export['dataset'] == 'cms_opps_quarterly_addenda'
        assert parsed_export['pipeline'] == 'cms_opps_scraper'


if __name__ == "__main__":
    """Run observability tests."""
    pytest.main([__file__, "-v"])
