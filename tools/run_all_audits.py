#!/usr/bin/env python3
"""
Comprehensive Audit Suite Runner

Runs all documentation audits AND related tests to ensure
governance compliance and implementation correctness.

Usage:
    python tools/run_all_audits.py
    python tools/run_all_audits.py --with-tests
    python tools/run_all_audits.py --quick (skip slow tests)
    python tools/run_all_audits.py --help
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


class AuditRunner:
    """Runs comprehensive audit suite"""
    
    def __init__(self, with_tests: bool = False, quick: bool = False):
        self.with_tests = with_tests
        self.quick = quick
        self.results = []
    
    def run_audit(self, name: str, command: List[str], category: str = "audit") -> Tuple[str, int, str]:
        """Run an audit command and return results"""
        print(f"\n{'='*60}")
        print(f"[{category.upper()}] {name}")
        print(f"{'='*60}")
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            return name, result.returncode, result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            return name, 1, f"ERROR: {name} timed out after 5 minutes"
        except Exception as e:
            return name, 1, f"ERROR: {name} failed with exception: {str(e)}"
    
    def run_all(self) -> int:
        """Run all audits and optionally tests"""
        
        # Core documentation audits
        audits = [
            ("Documentation Catalog", ["python", "tools/audit_doc_catalog.py"], "audit"),
            ("Documentation Links", ["python", "tools/audit_doc_links.py"], "audit"),
            ("Cross-References", ["python", "tools/audit_cross_references.py"], "audit"),
            ("Documentation Metadata", ["python", "tools/audit_doc_metadata.py"], "audit"),
            ("Companion Documents", ["python", "tools/audit_companion_docs.py"], "audit"),
            ("Source Map Verification", ["python", "tools/verify_source_map.py"], "audit"),
        ]
        
        # Run documentation tests if requested
        if self.with_tests:
            test_audits = [
                ("PRD Documentation Tests", ["pytest", "tests/prd_docs", "-v", "--tb=short"], "test"),
            ]
            
            if not self.quick:
                test_audits.extend([
                    ("Scraper Tests", ["pytest", "tests/scrapers", "-v", "--tb=short", "-k", "not performance"], "test"),
                    ("Ingestor Tests (Quick)", ["pytest", "tests/ingestors", "-k", "not e2e", "-v", "--tb=short"], "test"),
                ])
            
            audits.extend(test_audits)
        
        # Run all audits
        for name, command, category in audits:
            audit_name, exit_code, output = self.run_audit(name, command, category)
            self.results.append((audit_name, exit_code, output, category))
            
            if exit_code != 0:
                print(f"❌ {audit_name} FAILED")
                if output:
                    print(output)
            else:
                print(f"✅ {audit_name} PASSED")
        
        # Print summary
        self.print_summary()
        
        # Return exit code
        failures = [r for r in self.results if r[1] != 0]
        return 1 if failures else 0
    
    def print_summary(self):
        """Print audit summary"""
        print(f"\n{'='*60}")
        print("AUDIT SUMMARY")
        print(f"{'='*60}")
        
        # Group by category
        audits = [(n, e, c) for n, e, _, c in self.results if c == "audit"]
        tests = [(n, e, c) for n, e, _, c in self.results if c == "test"]
        
        if audits:
            print("\nDocumentation Audits:")
            for name, exit_code, _ in audits:
                status = "✅ PASS" if exit_code == 0 else "❌ FAIL"
                print(f"  {status} - {name}")
        
        if tests:
            print("\nTests:")
            for name, exit_code, _ in tests:
                status = "✅ PASS" if exit_code == 0 else "❌ FAIL"
                print(f"  {status} - {name}")
        
        # Overall summary
        failures = [r for r in self.results if r[1] != 0]
        total = len(self.results)
        passed = total - len(failures)
        
        print(f"\n{'='*60}")
        if failures:
            print(f"❌ {len(failures)}/{total} checks failed")
            print(f"\nFailed checks:")
            for name, _, _, category in failures:
                print(f"  - [{category}] {name}")
        else:
            print(f"✅ All {total} checks passed!")
        print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run comprehensive audit suite for CMS API documentation"
    )
    parser.add_argument(
        "--with-tests",
        action="store_true",
        help="Run documentation tests in addition to audits"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Skip slow tests (only with --with-tests)"
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("CMS API Comprehensive Audit Suite")
    print("="*60)
    print(f"Mode: {'Quick' if args.quick else 'Full'}")
    print(f"Tests: {'Enabled' if args.with_tests else 'Disabled'}")
    print()
    
    runner = AuditRunner(with_tests=args.with_tests, quick=args.quick)
    exit_code = runner.run_all()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

