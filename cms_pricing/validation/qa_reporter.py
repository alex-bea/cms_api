"""
QA Report Generator for RVU Data Validation

Generates HTML and JSON reports of validation results as specified in PRD Section 2.4
"""

import json
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path
from .types import ValidationResult, ValidationLevel


class QAReportGenerator:
    """Generates QA reports for validation results"""
    
    def __init__(self, output_dir: str = "data/RVU/qa_reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_reports(self, release_id: str, validation_results: Dict[str, Any]) -> Dict[str, str]:
        """Generate both HTML and JSON QA reports"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate JSON report
        json_path = self.output_dir / f"qa_report_{release_id}_{timestamp}.json"
        self._generate_json_report(json_path, validation_results)
        
        # Generate HTML report
        html_path = self.output_dir / f"qa_report_{release_id}_{timestamp}.html"
        self._generate_html_report(html_path, validation_results)
        
        return {
            "json_report": str(json_path),
            "html_report": str(html_path)
        }
    
    def _generate_json_report(self, file_path: Path, validation_results: Dict[str, Any]):
        """Generate JSON QA report"""
        
        # Convert ValidationResult objects to dictionaries
        def serialize_validation_results(results: List[ValidationResult]) -> List[Dict[str, Any]]:
            return [
                {
                    "level": result.level.value,
                    "rule_name": result.rule_name,
                    "message": result.message,
                    "record_id": result.record_id,
                    "field_name": result.field_name,
                    "actual_value": result.actual_value,
                    "expected_value": result.expected_value
                }
                for result in results
            ]
        
        # Prepare report data
        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "release_id": str(validation_results.get('release_id', 'unknown')),
                "report_type": "RVU Data Validation QA Report"
            },
            "validation_summary": {
                "structural": {
                    "status": validation_results['structural']['status'],
                    "error_count": len(validation_results['structural']['errors'])
                },
                "content": {
                    "status": validation_results['content']['status'],
                    "error_count": len(validation_results['content']['errors'])
                },
                "referential": {
                    "status": validation_results['referential']['status'],
                    "error_count": len(validation_results['referential']['errors'])
                },
                "business_rules": {
                    "status": validation_results['business_rules']['status'],
                    "total_validated": validation_results['business_rules'].get('total_validated', 0),
                    "error_count": len(validation_results['business_rules'].get('errors', [])),
                    "warning_count": len(validation_results['business_rules'].get('warnings', []))
                }
            },
            "detailed_results": {
                "structural_errors": validation_results['structural']['errors'],
                "content_errors": validation_results['content']['errors'],
                "referential_errors": validation_results['referential']['errors'],
                "business_rule_errors": serialize_validation_results(validation_results['business_rules'].get('errors', [])),
                "business_rule_warnings": serialize_validation_results(validation_results['business_rules'].get('warnings', []))
            }
        }
        
        # Write JSON report
        with open(file_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
    
    def _generate_html_report(self, file_path: Path, validation_results: Dict[str, Any]):
        """Generate HTML QA report"""
        
        # Get summary data
        structural = validation_results['structural']
        content = validation_results['content']
        referential = validation_results['referential']
        business_rules = validation_results['business_rules']
        
        # Calculate overall status
        overall_status = "PASS" if all([
            structural['status'] == 'pass',
            content['status'] == 'pass',
            referential['status'] == 'pass',
            business_rules['status'] == 'pass'
        ]) else "FAIL"
        
        # Generate HTML content
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RVU Data Validation QA Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 2px solid #eee;
        }}
        .status-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            color: white;
            margin: 10px 0;
        }}
        .status-pass {{
            background-color: #28a745;
        }}
        .status-fail {{
            background-color: #dc3545;
        }}
        .status-warn {{
            background-color: #ffc107;
            color: #212529;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            border: 1px solid #ddd;
            border-radius: 6px;
            padding: 15px;
            background-color: #fafafa;
        }}
        .summary-card h3 {{
            margin-top: 0;
            color: #333;
        }}
        .error-list {{
            background-color: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 15px;
            margin: 10px 0;
        }}
        .error-item {{
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }}
        .error-item:last-child {{
            border-bottom: none;
        }}
        .error-level {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
            margin-right: 8px;
        }}
        .level-error {{
            background-color: #dc3545;
            color: white;
        }}
        .level-warn {{
            background-color: #ffc107;
            color: #212529;
        }}
        .level-info {{
            background-color: #17a2b8;
            color: white;
        }}
        .metadata {{
            background-color: #e9ecef;
            padding: 10px;
            border-radius: 4px;
            font-size: 0.9em;
            color: #6c757d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>RVU Data Validation QA Report</h1>
            <div class="status-badge {'status-pass' if overall_status == 'PASS' else 'status-fail'}">
                Overall Status: {overall_status}
            </div>
            <div class="metadata">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                Release ID: {validation_results.get('release_id', 'Unknown')}
            </div>
        </div>
        
        <div class="summary-grid">
            <div class="summary-card">
                <h3>Structural Validation</h3>
                <div class="status-badge {'status-pass' if structural['status'] == 'pass' else 'status-fail'}">
                    {structural['status'].upper()}
                </div>
                <p>Errors: {len(structural['errors'])}</p>
            </div>
            
            <div class="summary-card">
                <h3>Content Validation</h3>
                <div class="status-badge {'status-pass' if content['status'] == 'pass' else 'status-fail'}">
                    {content['status'].upper()}
                </div>
                <p>Errors: {len(content['errors'])}</p>
            </div>
            
            <div class="summary-card">
                <h3>Referential Validation</h3>
                <div class="status-badge {'status-pass' if referential['status'] == 'pass' else 'status-fail'}">
                    {referential['status'].upper()}
                </div>
                <p>Errors: {len(referential['errors'])}</p>
            </div>
            
            <div class="summary-card">
                <h3>Business Rules</h3>
                <div class="status-badge {'status-pass' if business_rules['status'] == 'pass' else 'status-fail'}">
                    {business_rules['status'].upper()}
                </div>
                <p>Validated: {business_rules.get('total_validated', 0)} items</p>
                <p>Errors: {len(business_rules.get('errors', []))}</p>
                <p>Warnings: {len(business_rules.get('warnings', []))}</p>
            </div>
        </div>
        
        <h2>Detailed Results</h2>
        
        {self._generate_errors_section('Structural Errors', structural['errors'], 'error')}
        {self._generate_errors_section('Content Errors', content['errors'], 'error')}
        {self._generate_errors_section('Referential Errors', referential['errors'], 'error')}
        {self._generate_validation_results_section('Business Rule Errors', business_rules.get('errors', []), 'error')}
        {self._generate_validation_results_section('Business Rule Warnings', business_rules.get('warnings', []), 'warn')}
    </div>
</body>
</html>
        """
        
        # Write HTML report
        with open(file_path, 'w') as f:
            f.write(html_content)
    
    def _generate_errors_section(self, title: str, errors: List[str], level: str) -> str:
        """Generate HTML section for simple error lists"""
        if not errors:
            return f'<h3>{title}</h3><p>No {level}s found.</p>'
        
        error_items = ''.join([
            f'<div class="error-item">{error}</div>'
            for error in errors
        ])
        
        return f'''
        <h3>{title}</h3>
        <div class="error-list">
            {error_items}
        </div>
        '''
    
    def _generate_validation_results_section(self, title: str, results: List[ValidationResult], level: str) -> str:
        """Generate HTML section for ValidationResult objects"""
        if not results:
            return f'<h3>{title}</h3><p>No {level}s found.</p>'
        
        result_items = ''.join([
            f'''
            <div class="error-item">
                <span class="error-level level-{result.level.value}">{result.level.value.upper()}</span>
                <strong>{result.rule_name}:</strong> {result.message}
                {f'<br><small>Record: {result.record_id}' if result.record_id else ''}
                {f' | Field: {result.field_name}' if result.field_name else ''}
                {f' | Actual: {result.actual_value}' if result.actual_value is not None else ''}
                {f' | Expected: {result.expected_value}' if result.expected_value is not None else ''}
            </div>
            '''
            for result in results
        ])
        
        return f'''
        <h3>{title}</h3>
        <div class="error-list">
            {result_items}
        </div>
        '''
