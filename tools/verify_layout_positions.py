#!/usr/bin/env python3
"""
Verify fixed-width layout positions against sample file.

Usage:
    python tools/verify_layout_positions.py <layout.json> <sample.txt> [num_lines]

Example:
    python tools/verify_layout_positions.py \\
        cms_pricing/ingestion/parsers/layouts/gpci_2025d.json \\
        sample_data/rvu25d_0/GPCI2025.txt \\
        5

Purpose:
    Verify column positions BEFORE writing parser code to prevent hours
    of debugging. Shows extracted values for manual verification.

Per: STD-parser-contracts v1.10 §21.4 (Format Verification Pre-Implementation)
"""

import json
import sys
import pathlib
from typing import Dict, Any, Optional


def slice_(s: str, start: int, end: Optional[int]) -> str:
    """
    Extract substring with exclusive end (Python slice convention).
    Right-strips whitespace for display.
    """
    if end is None:
        return s[start:].rstrip()
    return s[start:end].rstrip()  # end = EXCLUSIVE


def main(layout_path: str, sample_path: str, n: int = 5) -> None:
    """
    Verify layout positions against real sample lines.
    
    Args:
        layout_path: Path to layout JSON (from layout_registry.py)
        sample_path: Path to sample TXT file
        n: Number of lines to verify (default: 5)
    """
    
    # Load layout
    try:
        layout = json.loads(pathlib.Path(layout_path).read_text())
    except Exception as e:
        print(f"❌ Failed to load layout: {e}")
        sys.exit(1)
    
    cols = layout.get("columns", {})
    if not cols:
        print("❌ No columns found in layout")
        sys.exit(1)
    
    # Load sample file
    try:
        lines = pathlib.Path(sample_path).read_text(errors="ignore").splitlines()
    except Exception as e:
        print(f"❌ Failed to load sample file: {e}")
        sys.exit(1)
    
    # Filter to data lines (skip headers)
    data = [
        ln for ln in lines 
        if ln.strip() 
        and not ln.startswith(("HDR", "----", "Medicare", "Locality", "HCPCS"))
    ][:n]
    
    if not data:
        print("❌ No data lines found in sample file")
        sys.exit(1)
    
    print(f"📋 Layout: {layout.get('version', 'unknown')}")
    print(f"📄 Sample: {sample_path}")
    print(f"📊 Verifying {len(data)} line(s)")
    print("=" * 80)
    
    # Verify each line
    for i, ln in enumerate(data, 1):
        print(f"\n# Sample line {i} (length={len(ln)})")
        print(f"Raw: {ln[:80]}{'...' if len(ln) > 80 else ''}")
        print()
        
        for name, spec in cols.items():
            start = spec.get("start", 0)
            end = spec.get("end")
            
            value = slice_(ln, start, end if end else len(ln))
            end_display = end if end else "END"
            
            print(f"  {name:<20} [{start:>3}, {end_display:<4}) → \"{value}\"")
        
        # Guardrails
        min_len = layout.get("min_line_length", 0)
        max_end = max((c.get("end", 0) for c in cols.values() if c.get("end")), default=0)
        
        if len(ln) < min_len:
            print(f"\n  ⚠️  WARN: Line shorter than min_line_length={min_len}")
        
        if len(ln) < max_end:
            print(f"\n  ⚠️  WARN: Line shorter than max column end={max_end}")
    
    print("\n" + "=" * 80)
    print("\n🔍 MANUAL VERIFICATION CHECKLIST")
    print("=" * 80)
    print("\nReview the extractions above and answer these questions:")
    print()
    
    # Domain-specific verification questions
    verification_questions = []
    
    for name in cols.keys():
        if 'mac' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain only 5-digit MAC codes?")
        elif 'locality' in name.lower() and 'code' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain 2-digit locality codes?")
        elif 'state' in name.lower() or 'fips' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain state names or FIPS codes (may be blank)?")
        elif 'hcpcs' in name.lower() or 'cpt' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain 5-character HCPCS codes?")
        elif 'county' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain county names or FIPS codes?")
        elif 'rvu' in name.lower() or 'gpci' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain decimal numbers (RVU/GPCI values)?")
        elif 'modifier' in name.lower():
            verification_questions.append(f"  ❓ Does '{name}' contain 2-character modifiers (or blank)?")
        else:
            verification_questions.append(f"  ❓ Does '{name}' contain expected content?")
    
    for q in verification_questions:
        print(q)
    
    print()
    print("  ❓ Are any values truncated (cut off mid-word)?")
    print("  ❓ Are any values spanning multiple columns (wrong boundaries)?")
    print("  ❓ Are all end indices EXCLUSIVE (not inclusive)?")
    print()
    print("=" * 80)
    print("\n✅ If all answers are YES:")
    print("   → Layout positions are correct")
    print("   → Proceed to parser implementation")
    print()
    print("❌ If any answer is NO:")
    print("   → Adjust layout positions in layout_registry.py")
    print("   → Re-run this tool to verify fixes")
    print("   → Repeat until all extractions are correct")
    print()
    print("💡 Pro tip: Test with edge cases (blank fields, max-length values)")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/verify_layout_positions.py <layout.json> <sample.txt> [num_lines]")
        print()
        print("Example:")
        print("  python tools/verify_layout_positions.py \\")
        print("    cms_pricing/ingestion/parsers/layouts/gpci_2025d.json \\")
        print("    sample_data/rvu25d_0/GPCI2025.txt \\")
        print("    5")
        sys.exit(1)
    
    layout_path = sys.argv[1]
    sample_path = sys.argv[2]
    num_lines = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    main(layout_path, sample_path, num_lines)

