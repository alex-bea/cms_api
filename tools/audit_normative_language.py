#!/usr/bin/env python3
"""
Audit PRD documents for normative language violations.

Per STD-doc-governance-prd-v1.0.md, documents should follow these conventions:
- STD-* (Standards): Normative language (MUST, SHALL, REQUIRED) allowed
- REF-* (References): Descriptive language only (should, recommended)
- RUN-* (Runbooks): Descriptive language only (should, recommended)
- PRD-* (Products): Context-dependent (generally descriptive)

This tool enforces governance separation by flagging normative language in
guidance documents (REF, RUN).

Usage:
    python tools/audit_normative_language.py

Exit codes:
    0: Passed (no violations)
    1: Failed (violations found)

Cross-References:
- STD-doc-governance-prd-v1.0.md §1.1 (Document type prefixes)
- Parser Contracts v2.0 modularization (prevents policy drift)
"""

import re
from pathlib import Path
import sys

# Normative keywords (RFC 2119 language)
NORMATIVE_WORDS = ['MUST', 'SHALL', 'REQUIRED', 'MUST NOT', 'SHALL NOT', 'SHOULD NOT']

# Documents where normative language is ALLOWED
ALLOWED_PREFIXES = ['STD-']

# Documents where normative language is PROHIBITED
PROHIBITED_PREFIXES = ['REF-', 'RUN-']


def is_in_code_block(line_num, lines):
    """
    Check if line is inside a code block (```).
    
    Args:
        line_num: Current line number (0-indexed)
        lines: All lines in file
        
    Returns:
        True if line is inside a code block
    """
    in_block = False
    for i in range(line_num):
        if lines[i].strip().startswith('```'):
            in_block = not in_block
    return in_block


def is_in_table(line):
    """Check if line is part of a markdown table."""
    # Tables use | for column separators
    return line.strip().startswith('|') and '|' in line


def audit_file(filepath):
    """
    Audit single file for normative language violations.
    
    Args:
        filepath: Path to markdown file
        
    Returns:
        List of (line_num, word, line_content) violations
    """
    try:
        lines = filepath.read_text(encoding='utf-8').splitlines()
    except UnicodeDecodeError:
        print(f"[WARN] Could not decode {filepath.name} (skipping)")
        return []
    
    violations = []
    
    for i, line in enumerate(lines, 1):
        # Skip code blocks (```)
        if is_in_code_block(i-1, lines):
            continue
        
        # Skip tables (often have MUST in "MUST Rules" column headers)
        if is_in_table(line):
            continue
        
        # Skip inline code (`normative word`)
        clean_line = re.sub(r'`[^`]+`', '', line)
        
        # Skip quoted text ("MUST do X" as example)
        clean_line = re.sub(r'"[^"]*"', '', clean_line)
        
        # Check for normative words
        for word in NORMATIVE_WORDS:
            pattern = r'\b' + word + r'\b'
            if re.search(pattern, clean_line):
                violations.append((i, word, line.strip()[:80]))  # Truncate long lines
    
    return violations


def main():
    """Main audit function."""
    prds_dir = Path('prds')
    
    if not prds_dir.exists():
        print("[ERROR] prds/ directory not found. Run from repo root.")
        sys.exit(1)
    
    errors = []
    checked_count = 0
    
    # Check REF-* and RUN-* docs (normative language prohibited)
    for prefix in PROHIBITED_PREFIXES:
        pattern = f'{prefix}*.md'
        for doc in prds_dir.glob(pattern):
            # Skip archived documents
            if 'ARCHIVED' in doc.name:
                continue
                
            checked_count += 1
            violations = audit_file(doc)
            if violations:
                errors.append((doc.name, violations))
    
    # Report results
    if errors:
        print(f"[ERROR] Normative language found in {len(errors)} guidance document(s):")
        print()
        for doc, violations in errors:
            print(f"  {doc} ({len(violations)} violation{'s' if len(violations) > 1 else ''}):")
            for line_num, word, line in violations[:5]:  # Show first 5 per doc
                print(f"    Line {line_num}: '{word}' in \"{line}...\"")
            if len(violations) > 5:
                print(f"    ... and {len(violations) - 5} more")
            print()
        
        print("[FIX] Use descriptive language instead:")
        print("  MUST → should, recommended, expected, required")
        print("  SHALL → will, is required to")
        print("  MUST NOT → should not, is prohibited")
        print()
        print(f"[INFO] Checked {checked_count} REF-/RUN- documents")
        sys.exit(1)
    else:
        print(f"[INFO] Normative language audit passed ({checked_count} docs checked).")
        sys.exit(0)


if __name__ == '__main__':
    main()
