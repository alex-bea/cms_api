#!/usr/bin/env python3
"""
Audit Code Patterns - Detect stale function signatures and deprecated patterns.

Prevents code drift by finding:
- Old function signatures that should be updated
- Deprecated patterns that should be refactored
- Inconsistent usage across codebase

Per STD-doc-governance-prd-v1.0.md: Automated validation of code consistency.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass


@dataclass
class PatternCheck:
    """Single pattern check result."""
    file: str
    line_num: int
    pattern_name: str
    old_pattern: str
    new_pattern: str
    severity: str  # ERROR, WARNING, INFO
    context: str


# ============================================================================
# Pattern Definitions
# ============================================================================

DEPRECATED_PATTERNS = [
    {
        "name": "enforce_categorical_dtypes_old_signature",
        "old_pattern": r"enforce_categorical_dtypes\s*\(\s*df\s*,\s*categorical_spec",
        "new_pattern": "enforce_categorical_dtypes(df, schema_contract, natural_keys, ...)",
        "severity": "ERROR",
        "description": "Old signature uses categorical_spec dict, new uses schema_contract + ValidationResult",
        "exclude_files": ["_parser_kit.py"],  # Exclude implementation file
    },
    {
        "name": "route_to_parser_tuple_unpacking",
        "old_pattern": r"(dataset|ds|d)\s*,\s*(schema_id|schema|s)\s*,\s*(status|st)\s*=\s*route_to_parser",
        "new_pattern": "decision = route_to_parser(...)",
        "severity": "ERROR",
        "description": "Old tuple unpacking, new uses RouteDecision NamedTuple",
        "exclude_files": ["__init__.py"],  # Exclude router implementation
    },
    {
        "name": "validation_severity_string",
        "old_pattern": r'severity\s*=\s*["\']WARN["\']',
        "new_pattern": "severity=ValidationSeverity.WARN",
        "severity": "WARNING",
        "description": "Use ValidationSeverity enum instead of string literal",
        "exclude_files": [],
    },
    {
        "name": "bare_tuple_validation_return",
        "old_pattern": r"return\s+\(\s*valid_df\s*,\s*rejects_df\s*\)",
        "new_pattern": "return ValidationResult(valid_df=..., rejects_df=..., metrics=...)",
        "severity": "WARNING",
        "description": "Use ValidationResult NamedTuple instead of bare tuple",
        "exclude_files": ["_parser_kit.py"],
    },
]


# ============================================================================
# Audit Functions
# ============================================================================

def scan_file_for_patterns(
    file_path: Path,
    patterns: List[Dict[str, Any]]
) -> List[PatternCheck]:
    """
    Scan a single file for deprecated patterns.
    
    Args:
        file_path: Path to file to scan
        patterns: List of pattern definitions
        
    Returns:
        List of PatternCheck results
    """
    results = []
    
    # Skip non-Python files
    if file_path.suffix not in ['.py']:
        return results
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"⚠️  Error reading {file_path}: {e}")
        return results
    
    for pattern_def in patterns:
        # Check if file is excluded
        if any(excl in str(file_path) for excl in pattern_def.get("exclude_files", [])):
            continue
        
        pattern = re.compile(pattern_def["old_pattern"])
        
        for line_num, line in enumerate(lines, start=1):
            if pattern.search(line):
                # Get context (3 lines)
                context_start = max(0, line_num - 2)
                context_end = min(len(lines), line_num + 1)
                context = "".join(lines[context_start:context_end])
                
                results.append(PatternCheck(
                    file=str(file_path.relative_to(Path.cwd())),
                    line_num=line_num,
                    pattern_name=pattern_def["name"],
                    old_pattern=pattern_def["old_pattern"],
                    new_pattern=pattern_def["new_pattern"],
                    severity=pattern_def["severity"],
                    context=context.strip()
                ))
    
    return results


def scan_codebase(
    patterns: List[Dict[str, Any]],
    target_dirs: List[str] = None
) -> List[PatternCheck]:
    """
    Scan entire codebase for deprecated patterns.
    
    Args:
        patterns: List of pattern definitions
        target_dirs: Directories to scan (default: cms_pricing, tests)
        
    Returns:
        List of all pattern check results
    """
    if target_dirs is None:
        target_dirs = ["cms_pricing", "tests"]
    
    all_results = []
    
    for target_dir in target_dirs:
        target_path = Path(target_dir)
        if not target_path.exists():
            continue
        
        # Recursively find all Python files
        for py_file in target_path.rglob("*.py"):
            # Skip __pycache__
            if "__pycache__" in str(py_file):
                continue
            
            results = scan_file_for_patterns(py_file, patterns)
            all_results.extend(results)
    
    return all_results


def group_results_by_severity(
    results: List[PatternCheck]
) -> Dict[str, List[PatternCheck]]:
    """Group results by severity level."""
    grouped = {"ERROR": [], "WARNING": [], "INFO": []}
    
    for result in results:
        grouped[result.severity].append(result)
    
    return grouped


def print_results(results: List[PatternCheck]):
    """
    Print audit results in human-readable format.
    
    Args:
        results: List of pattern check results
    """
    if not results:
        print("✅ No deprecated patterns found!\n")
        return
    
    grouped = group_results_by_severity(results)
    
    total_issues = len(results)
    errors = len(grouped["ERROR"])
    warnings = len(grouped["WARNING"])
    
    print(f"\n{'='*80}")
    print(f"Code Pattern Audit Results")
    print(f"{'='*80}\n")
    
    print(f"Total Issues: {total_issues}")
    print(f"  - Errors: {errors}")
    print(f"  - Warnings: {warnings}")
    print(f"  - Info: {len(grouped['INFO'])}\n")
    
    # Print by severity
    for severity in ["ERROR", "WARNING", "INFO"]:
        issues = grouped[severity]
        if not issues:
            continue
        
        icon = "❌" if severity == "ERROR" else "⚠️" if severity == "WARNING" else "ℹ️"
        print(f"\n{icon} {severity}S ({len(issues)}):\n")
        
        # Group by pattern name
        by_pattern = {}
        for issue in issues:
            if issue.pattern_name not in by_pattern:
                by_pattern[issue.pattern_name] = []
            by_pattern[issue.pattern_name].append(issue)
        
        for pattern_name, pattern_issues in by_pattern.items():
            print(f"  {pattern_name} ({len(pattern_issues)} occurrences):")
            print(f"    → Use: {pattern_issues[0].new_pattern}\n")
            
            for issue in pattern_issues[:5]:  # Show first 5
                print(f"    {issue.file}:{issue.line_num}")
                # Show context (indented)
                for ctx_line in issue.context.split('\n'):
                    print(f"      {ctx_line}")
                print()
            
            if len(pattern_issues) > 5:
                print(f"    ... and {len(pattern_issues) - 5} more\n")
    
    print(f"{'='*80}\n")


def main():
    """Run code pattern audit."""
    print("🔍 Scanning codebase for deprecated patterns...\n")
    
    results = scan_codebase(DEPRECATED_PATTERNS)
    
    print_results(results)
    
    # Exit code
    grouped = group_results_by_severity(results)
    errors = len(grouped["ERROR"])
    
    if errors > 0:
        print(f"❌ Found {errors} ERROR-level issues")
        sys.exit(1)
    else:
        print("✅ No critical issues found")
        sys.exit(0)


if __name__ == "__main__":
    main()

