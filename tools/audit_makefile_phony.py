#!/usr/bin/env python3
"""
Makefile .PHONY Audit Tool

Validates that all Makefile targets are declared as .PHONY
(since they don't produce files with the same name).

Can also auto-fix the Makefile by adding missing .PHONY declarations.

Usage:
    python tools/audit_makefile_phony.py              # Check only
    python tools/audit_makefile_phony.py --fix        # Auto-fix
    python tools/audit_makefile_phony.py --verbose    # Show all targets
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Set, Tuple


class MakefilePhonyAuditor:
    """Audits and fixes .PHONY declarations in Makefile"""
    
    def __init__(self, makefile_path: str = "Makefile"):
        self.makefile_path = Path(makefile_path)
        self.target_pattern = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_-]*):.*?(?:##|$)', re.MULTILINE)
        self.phony_pattern = re.compile(r'^\.PHONY:\s*(.+)$', re.MULTILINE)
    
    def extract_targets(self, content: str) -> Set[str]:
        """Extract all target names from Makefile"""
        targets = set()
        for match in self.target_pattern.finditer(content):
            target = match.group(1).strip()
            # Skip special targets
            if target not in ['.PHONY', '.DEFAULT_GOAL', '.SUFFIXES']:
                targets.add(target)
        return targets
    
    def extract_phony_declarations(self, content: str) -> Set[str]:
        """Extract all targets declared as .PHONY"""
        phony_targets = set()
        for match in self.phony_pattern.finditer(content):
            # Split on whitespace to get all targets in this .PHONY line
            targets = match.group(1).strip().split()
            phony_targets.update(targets)
        return phony_targets
    
    def audit(self, verbose: bool = False) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Audit Makefile for missing .PHONY declarations
        
        Returns:
            (all_targets, phony_targets, missing_phony)
        """
        if not self.makefile_path.exists():
            print(f"❌ ERROR: {self.makefile_path} not found")
            sys.exit(1)
        
        content = self.makefile_path.read_text()
        
        all_targets = self.extract_targets(content)
        phony_targets = self.extract_phony_declarations(content)
        missing_phony = all_targets - phony_targets
        
        if verbose:
            print(f"📋 All targets ({len(all_targets)}):")
            for target in sorted(all_targets):
                status = "✅" if target in phony_targets else "❌"
                print(f"  {status} {target}")
            print()
        
        return all_targets, phony_targets, missing_phony
    
    def report(self, all_targets: Set[str], phony_targets: Set[str], missing_phony: Set[str]) -> int:
        """Print audit report and return exit code"""
        print("="*60)
        print("Makefile .PHONY Audit")
        print("="*60)
        print()
        print(f"Total targets: {len(all_targets)}")
        print(f"Declared as .PHONY: {len(phony_targets)}")
        print(f"Missing .PHONY: {len(missing_phony)}")
        print()
        
        if missing_phony:
            print("❌ Missing .PHONY declarations:")
            for target in sorted(missing_phony):
                print(f"  - {target}")
            print()
            print("💡 Run with --fix to automatically add missing declarations")
            return 1
        else:
            print("✅ All targets are properly declared as .PHONY!")
            return 0
    
    def fix(self, missing_phony: Set[str]) -> bool:
        """Auto-fix Makefile by adding missing .PHONY declarations"""
        if not missing_phony:
            print("✅ No fixes needed - all targets already declared as .PHONY")
            return True
        
        content = self.makefile_path.read_text()
        lines = content.splitlines()
        
        # Find the last .PHONY line
        last_phony_idx = -1
        for i, line in enumerate(lines):
            if line.startswith('.PHONY:'):
                last_phony_idx = i
        
        if last_phony_idx == -1:
            print("❌ ERROR: No .PHONY declarations found in Makefile")
            print("💡 Add at least one .PHONY line first, then re-run --fix")
            return False
        
        # Group missing targets by category (heuristic)
        # We'll add them as a new .PHONY line after the last one
        missing_sorted = sorted(missing_phony)
        
        # Create new .PHONY line
        new_phony_line = f".PHONY: {' '.join(missing_sorted)}"
        
        # Insert after last .PHONY line
        lines.insert(last_phony_idx + 1, new_phony_line)
        
        # Write back
        new_content = '\n'.join(lines) + '\n'
        self.makefile_path.write_text(new_content)
        
        print(f"✅ Added .PHONY declaration for {len(missing_phony)} targets:")
        for target in missing_sorted:
            print(f"  - {target}")
        print()
        print(f"📝 Updated: {self.makefile_path}")
        
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Audit and fix .PHONY declarations in Makefile"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Automatically add missing .PHONY declarations"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show all targets and their .PHONY status"
    )
    parser.add_argument(
        "--makefile",
        default="Makefile",
        help="Path to Makefile (default: Makefile)"
    )
    
    args = parser.parse_args()
    
    auditor = MakefilePhonyAuditor(args.makefile)
    all_targets, phony_targets, missing_phony = auditor.audit(verbose=args.verbose)
    
    if args.fix:
        if auditor.fix(missing_phony):
            # Re-audit to verify
            print("\n🔍 Verifying fix...")
            all_targets, phony_targets, missing_phony = auditor.audit()
            return auditor.report(all_targets, phony_targets, missing_phony)
        else:
            return 1
    else:
        return auditor.report(all_targets, phony_targets, missing_phony)


if __name__ == "__main__":
    sys.exit(main())

