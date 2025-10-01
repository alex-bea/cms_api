#!/usr/bin/env python3
"""
Performance Regression Checker
===============================

Checks for performance regressions against baseline metrics.
Follows QTS v1.1 requirements for performance monitoring.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass


@dataclass
class PerformanceMetric:
    """Performance metric data structure."""
    name: str
    current_value: float
    baseline_value: float
    threshold_pct: float
    regression_pct: float
    status: str


class PerformanceRegressionChecker:
    """Checks for performance regressions."""
    
    def __init__(self, regression_threshold: float = 20.0):
        """Initialize checker with regression threshold."""
        self.regression_threshold = regression_threshold
    
    def load_benchmark_results(self, file_path: Path) -> Dict[str, Any]:
        """Load benchmark results from JSON file."""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def extract_metrics(self, benchmark_data: Dict[str, Any]) -> List[PerformanceMetric]:
        """Extract performance metrics from benchmark data."""
        metrics = []
        
        for benchmark in benchmark_data.get('benchmarks', []):
            name = benchmark.get('name', 'unknown')
            stats = benchmark.get('stats', {})
            
            current_value = stats.get('mean', 0.0)
            baseline_value = stats.get('mean', 0.0)  # Will be updated with baseline
            
            regression_pct = 0.0
            if baseline_value > 0:
                regression_pct = ((current_value - baseline_value) / baseline_value) * 100
            
            status = "healthy"
            if regression_pct > self.regression_threshold:
                status = "regression"
            elif regression_pct < -10:  # 10% improvement
                status = "improvement"
            
            metric = PerformanceMetric(
                name=name,
                current_value=current_value,
                baseline_value=baseline_value,
                threshold_pct=self.regression_threshold,
                regression_pct=regression_pct,
                status=status
            )
            metrics.append(metric)
        
        return metrics
    
    def compare_with_baseline(self, 
                            current_file: Path, 
                            baseline_file: Path) -> List[PerformanceMetric]:
        """Compare current results with baseline."""
        current_data = self.load_benchmark_results(current_file)
        baseline_data = self.load_benchmark_results(baseline_file)
        
        current_metrics = self.extract_metrics(current_data)
        baseline_metrics = self.extract_metrics(baseline_data)
        
        # Update baseline values in current metrics
        baseline_map = {m.name: m.current_value for m in baseline_metrics}
        
        for metric in current_metrics:
            if metric.name in baseline_map:
                metric.baseline_value = baseline_map[metric.name]
                if metric.baseline_value > 0:
                    metric.regression_pct = ((metric.current_value - metric.baseline_value) / metric.baseline_value) * 100
                
                if metric.regression_pct > self.regression_threshold:
                    metric.status = "regression"
                elif metric.regression_pct < -10:
                    metric.status = "improvement"
                else:
                    metric.status = "healthy"
        
        return current_metrics
    
    def check_regressions(self, metrics: List[PerformanceMetric]) -> Dict[str, Any]:
        """Check for performance regressions."""
        regressions = [m for m in metrics if m.status == "regression"]
        improvements = [m for m in metrics if m.status == "improvement"]
        healthy = [m for m in metrics if m.status == "healthy"]
        
        return {
            "total_metrics": len(metrics),
            "regressions": len(regressions),
            "improvements": len(improvements),
            "healthy": len(healthy),
            "regression_details": [
                {
                    "name": m.name,
                    "current_value": m.current_value,
                    "baseline_value": m.baseline_value,
                    "regression_pct": m.regression_pct,
                    "threshold_pct": m.threshold_pct
                }
                for m in regressions
            ],
            "improvement_details": [
                {
                    "name": m.name,
                    "current_value": m.current_value,
                    "baseline_value": m.baseline_value,
                    "improvement_pct": abs(m.regression_pct),
                    "threshold_pct": m.threshold_pct
                }
                for m in improvements
            ],
            "overall_status": "failed" if regressions else "passed"
        }
    
    def generate_report(self, check_results: Dict[str, Any]) -> str:
        """Generate performance regression report."""
        report = []
        report.append("Performance Regression Check Report")
        report.append("=" * 40)
        report.append(f"Overall Status: {check_results['overall_status'].upper()}")
        report.append(f"Total Metrics: {check_results['total_metrics']}")
        report.append(f"Regressions: {check_results['regressions']}")
        report.append(f"Improvements: {check_results['improvements']}")
        report.append(f"Healthy: {check_results['healthy']}")
        report.append("")
        
        if check_results['regressions']:
            report.append("PERFORMANCE REGRESSIONS:")
            report.append("-" * 25)
            for reg in check_results['regression_details']:
                report.append(f"  {reg['name']}:")
                report.append(f"    Current: {reg['current_value']:.6f}s")
                report.append(f"    Baseline: {reg['baseline_value']:.6f}s")
                report.append(f"    Regression: {reg['regression_pct']:.1f}%")
                report.append(f"    Threshold: {reg['threshold_pct']:.1f}%")
                report.append("")
        
        if check_results['improvements']:
            report.append("PERFORMANCE IMPROVEMENTS:")
            report.append("-" * 26)
            for imp in check_results['improvement_details']:
                report.append(f"  {imp['name']}:")
                report.append(f"    Current: {imp['current_value']:.6f}s")
                report.append(f"    Baseline: {imp['baseline_value']:.6f}s")
                report.append(f"    Improvement: {imp['improvement_pct']:.1f}%")
                report.append("")
        
        return "\n".join(report)


def main():
    """Main function for performance regression checking."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check for performance regressions")
    parser.add_argument("current_file", type=Path, help="Current benchmark results file")
    parser.add_argument("baseline_file", type=Path, help="Baseline benchmark results file")
    parser.add_argument("--threshold", type=float, default=20.0, help="Regression threshold percentage")
    parser.add_argument("--output", type=Path, help="Output report file")
    parser.add_argument("--json", action="store_true", help="Output JSON format")
    
    args = parser.parse_args()
    
    # Check if files exist
    if not args.current_file.exists():
        print(f"Error: Current file not found: {args.current_file}")
        sys.exit(1)
    
    if not args.baseline_file.exists():
        print(f"Error: Baseline file not found: {args.baseline_file}")
        sys.exit(1)
    
    # Run performance regression check
    checker = PerformanceRegressionChecker(args.threshold)
    
    try:
        metrics = checker.compare_with_baseline(args.current_file, args.baseline_file)
        check_results = checker.check_regressions(metrics)
        
        if args.json:
            # Output JSON format
            output = json.dumps(check_results, indent=2)
        else:
            # Output human-readable format
            output = checker.generate_report(check_results)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
        else:
            print(output)
        
        # Exit with error code if regressions found
        if check_results['overall_status'] == 'failed':
            sys.exit(1)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
