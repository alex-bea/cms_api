# OPPSCAP Parser Fix Plan

**Error:** `No layout found for OPPSCAP 2025 Q`  
**Filename:** `OPPSCAP_JAN.xlsx`  
**Date:** 2025-11-04  
**Status:** ✅ **IMPLEMENTED** (2025-01-15)

---

## Problem Analysis

### Root Cause
The error occurs because:
1. **Filename parsing:** `OPPSCAP_JAN.xlsx` contains month name "JAN" but `extract_vintage_metadata()` doesn't handle month names → `quarter_vintage` becomes empty/None
2. **Missing layout entries:** Layout registry only has OPPSCAP layouts for quarters C and D, missing A and B
3. **Empty quarter_vintage:** When `quarter_vintage` is empty string, `get_layout()` normalizes it to `None`, but no annual fallback exists for OPPSCAP

### Error Flow
```
OPPSCAP_JAN.xlsx
  ↓
extract_vintage_metadata() → quarter_vintage = "" (month name not parsed)
  ↓
oppscap_parser._parse_txt_fixed_width() → metadata.get('quarter_vintage', 'D') → gets "" from metadata
  ↓
get_layout('2025', '', 'oppscap') → normalizes "" → None
  ↓
LAYOUT_REGISTRY lookup: ('oppscap', '2025', None) → NOT FOUND
  ↓
Error: "No layout found for OPPSCAP 2025 Q"
```

---

## Solution Plan

### Fix 1: Add Month Name → Quarter Mapping (Priority: HIGH)

**File:** `cms_pricing/ingestion/metadata/vintage_extractor.py`

**Change:** Extend `extract_quarter_from_filename()` to handle month names

```python
def extract_quarter_from_filename(filename: str) -> Optional[str]:
    """
    Extract quarter from filename.
    
    Handles: Q1-Q4, A-D notation, month names (JAN, JANUARY, etc.)
    
    Args:
        filename: Filename to parse
        
    Returns:
        Quarter string (e.g., "Q1") or None
    """
    # Try explicit quarter
    quarter_match = re.search(r'[Qq]([1-4])', filename)
    if quarter_match:
        return f"Q{quarter_match.group(1)}"
    
    # Try revision letter
    revision_match = re.search(r'([ABCD])(?:\.|_|$|\s)', filename, re.I)
    if revision_match:
        quarter_map = {'A': 'Q1', 'B': 'Q2', 'C': 'Q3', 'D': 'Q4'}
        return quarter_map[revision_match.group(1).upper()]
    
    # NEW: Try month names
    month_map = {
        'jan': 'Q1', 'january': 'Q1',
        'apr': 'Q2', 'april': 'Q2',
        'jul': 'Q3', 'july': 'Q3',
        'oct': 'Q4', 'october': 'Q4'
    }
    filename_lower = filename.lower()
    for month, quarter in month_map.items():
        if month in filename_lower:
            return quarter
    
    return None
```

**Also update `extract_vintage_metadata()` to use this enhanced function:**

```python
# In extract_vintage_metadata(), replace lines 102-112 with:
quarter = extract_quarter_from_filename(source)
if quarter:
    # Map Q1-Q4 to revision letters
    quarter_to_revision = {'Q1': 'A', 'Q2': 'B', 'Q3': 'C', 'Q4': 'D'}
    revision = quarter_to_revision.get(quarter, None)
```

---

### Fix 2: Add Missing OPPSCAP Layout Entries (Priority: HIGH)

**File:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Change:** Add OPPSCAP layouts for quarters A and B

```python
LAYOUT_REGISTRY = {
    # ... existing entries ...
    
    # OPPSCAP layouts (all quarters use same 2025D layout)
    ('oppscap', '2025', 'A'): OPPSCAP_2025D_LAYOUT,  # Q1/January
    ('oppscap', '2025', 'B'): OPPSCAP_2025D_LAYOUT,  # Q2/April
    ('oppscap', '2025', 'C'): OPPSCAP_2025D_LAYOUT,  # Q3/July
    ('oppscap', '2025', 'D'): OPPSCAP_2025D_LAYOUT,  # Q4/October
    ('oppscap', '2025', None): OPPSCAP_2025D_LAYOUT,  # Annual fallback
    
    # ... rest of registry ...
}
```

---

### Fix 3: Improve Error Message (Priority: LOW)

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`

**Change:** Better error message with debugging info

```python
# Line 269-271
layout = get_layout(product_year, quarter_vintage, 'oppscap')
if not layout:
    raise ParseError(
        f"No layout found for OPPSCAP {product_year} Q{quarter_vintage or 'None'}. "
        f"Available quarters: A, B, C, D. "
        f"Filename: {filename}, Metadata: {metadata.get('quarter_vintage', 'missing')}"
    )
```

---

### Fix 4: Handle XLSX Format (Priority: MEDIUM)

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`

**Issue:** The parser detects XLSX but might be calling `_parse_txt_fixed_width()` which requires layout.

**Check:** Ensure `_parse_xlsx()` method exists and handles metadata properly.

---

## Implementation Steps

1. **Step 1:** Add month name mapping to `vintage_extractor.py` (15 min)
   - Test with: `OPPSCAP_JAN.xlsx`, `OPPSCAP_APRIL.xlsx`, `OPPSCAP_JUL.xlsx`, `OPPSCAP_OCTOBER.xlsx`

2. **Step 2:** Add missing OPPSCAP layout entries (5 min)
   - Add A, B, and annual fallback to registry

3. **Step 3:** Improve error handling (5 min)
   - Add better error messages with debugging context

4. **Step 4:** Test with XLSX file (10 min)
   - Verify `OPPSCAP_JAN.xlsx` parses correctly
   - Check metadata extraction logs

5. **Step 5:** Verify layout lookup (5 min)
   - Test `get_layout('2025', 'A', 'oppscap')` returns layout
   - Test `get_layout('2025', 'Q1', 'oppscap')` normalizes correctly

---

## Testing Checklist

- [ ] `OPPSCAP_JAN.xlsx` → quarter_vintage = "2025Q1" or "A"
- [ ] `OPPSCAP_APR.xlsx` → quarter_vintage = "2025Q2" or "B"
- [ ] `OPPSCAP_JUL.xlsx` → quarter_vintage = "2025Q3" or "C"
- [ ] `OPPSCAP_OCT.xlsx` → quarter_vintage = "2025Q4" or "D"
- [ ] Layout lookup works for all quarters (A, B, C, D)
- [ ] XLSX files parse correctly (not just TXT)
- [ ] Error message includes helpful debugging info

---

## Rollback Plan

If issues arise:
1. Revert `vintage_extractor.py` changes
2. Revert layout registry additions
3. Manually set `quarter_vintage` in metadata before calling parser

---

## Estimated Time

**Total:** 40 minutes
- Fix 1: 15 min
- Fix 2: 5 min
- Fix 3: 5 min
- Fix 4: 10 min (investigation)
- Testing: 10 min

---

## Related Files

- `cms_pricing/ingestion/metadata/vintage_extractor.py` - Month name parsing
- `cms_pricing/ingestion/parsers/layout_registry.py` - Layout registry
- `cms_pricing/ingestion/parsers/oppscap_parser.py` - Parser implementation
- `cms_pricing/ingestion/datasets/rvu_spec.py` - Dataset routing

