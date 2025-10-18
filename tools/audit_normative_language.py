#!/usr/bin/env python3
"""Audit governance docs for misplaced normative language."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

NORMATIVE_WORDS = ["MUST", "SHALL", "REQUIRED", "MUST NOT", "SHALL NOT", "SHOULD NOT"]

PRDS_DIR = Path("prds")


def parse_metadata(lines: List[str]) -> Dict[str, object]:
    meta: Dict[str, object] = {}
    idx = 0
    started = False
    while idx < len(lines):
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped:
            if started:
                break
            idx += 1
            continue
        if stripped.startswith("#"):
            break
        if ":" not in raw:
            break
        key, value = raw.split(":", 1)
        key = key.strip()
        value = value.strip()
        started = True
        if key == "requires":
            requires: List[str] = []
            idx += 1
            while idx < len(lines):
                lookahead = lines[idx]
                if not lookahead.strip():
                    break
                if lookahead.lstrip().startswith("- "):
                    requires.append(lookahead.strip().lstrip("-").strip())
                    idx += 1
                    continue
                break
            meta["requires"] = requires
            continue
        meta[key] = value
        idx += 1
    return meta


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


def audit_file(filepath: Path) -> List[Tuple[int, str, str]]:
    try:
        lines = filepath.read_text(encoding="utf-8").splitlines()
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
            pattern = r"\b" + word + r"\b"
            if re.search(pattern, clean_line):
                violations.append((i, word, line.strip()[:80]))  # Truncate long lines
    
    return violations


def main() -> None:
    if not PRDS_DIR.exists():
        print("[ERROR] prds/ directory not found. Run from repo root.")
        sys.exit(1)
    
    errors = []
    metadata_errors = []
    checked_count = 0
    
    for doc in sorted(PRDS_DIR.glob("*.md")):
        if "ARCHIVED" in doc.name.upper():
            continue

        lines = doc.read_text(encoding="utf-8").splitlines()
        metadata = parse_metadata(lines)
        doc_type = metadata.get("doc_type")
        normative_flag = str(metadata.get("normative", "false")).strip().lower()

        if not doc_type:
            if doc.name.startswith(("STD-parser", "REF-parser", "RUN-parser")):
                metadata_errors.append((doc, "Missing `doc_type` metadata."))
            continue

        if normative_flag not in {"true", "false"}:
            metadata_errors.append((doc, "Invalid `normative` flag (expected true/false)."))
            continue

        is_normative = normative_flag == "true"
        if doc_type != "STD" and is_normative:
            metadata_errors.append(
                (doc, "`normative: true` is only allowed for STD documents.")
            )
            continue

        if doc_type in {"REF", "RUN", "STD"} and not is_normative:
            checked_count += 1
            violations = audit_file(doc)
            if violations:
                errors.append((doc.name, violations))

    if metadata_errors:
        print("[ERROR] Metadata validation failed for documentation header(s):\n")
        for doc, message in metadata_errors:
            print(f"  {doc.name}: {message}")
        sys.exit(1)

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


if __name__ == "__main__":
    main()
