# Quarter Notation Standard

**Status:** ✅ IMPLEMENTED  
**Version:** 1.0  
**Date:** 2025-10-20  
**Commit:** `f906623`

---

## Summary

Standardized quarter notation across the CMS Pricing API to use **CMS letter format** (A, B, C, D) with backward compatibility for Q-notation (Q1, Q2, Q3, Q4).

---

## Problem

**Before standardization:**
- **Mixed notation** across system:
  - GPCI used letters: `('gpci', '2025', 'A')`, `('gpci', '2025', 'D')`
  - OPPSCAP used Q-notation: `('oppscap', '2025', 'Q4')`
  - PPRRVU used Q-notation: `('pprrvu', '2025', 'Q4')`
- **`get_layout()` only handled Q-notation** (line 202: `if 'Q' in quarter_vintage`)
  - Passing `"Q4"` worked ✅
  - Passing `"D"` failed ❌ (fell back to annual layout)
- **Inconsistent with CMS naming**: Official files use `RVU25D`, `GPCI25C`, not `RVU25Q4`

---

## Solution

### 1. **Standardized LAYOUT_REGISTRY Keys**

All datasets now use **CMS letter notation** (A, B, C, D):

```python
LAYOUT_REGISTRY = {
    # Quarter notation: CMS letters (A=Q1, B=Q2, C=Q3, D=Q4)
    # Matches CMS file naming convention (e.g., RVU25D, GPCI25C)
    
    # PPRRVU layouts (D = October/Q4 release)
    ('pprrvu', '2025', 'D'): PPRRVU_2025D_LAYOUT,
    ('pprrvu', '2025', 'C'): PPRRVU_2025D_LAYOUT,
    ('pprrvu', '2025', 'B'): PPRRVU_2025D_LAYOUT,
    ('pprrvu', '2025', 'A'): PPRRVU_2025D_LAYOUT,
    
    # GPCI layouts
    ('gpci', '2025', 'D'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'C'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'B'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'A'): GPCI_2025D_LAYOUT,
    
    # OPPSCAP layouts
    ('oppscap', '2025', 'D'): OPPSCAP_2025D_LAYOUT,
    ('oppscap', '2025', 'C'): OPPSCAP_2025D_LAYOUT,
    
    # ... etc
}
```

### 2. **Enhanced `get_layout()` with Backward Compatibility**

Accepts **three** quarter formats:

```python
def get_layout(product_year: str, quarter_vintage: str, dataset: str):
    """
    Args:
        quarter_vintage: Quarter in CMS letter format (A/B/C/D) or Q-notation (Q1/Q2/Q3/Q4)
    
    Note:
        Accepts multiple quarter formats for backward compatibility:
        - CMS letters: "A", "B", "C", "D" (preferred, matches CMS file naming)
        - Q-notation: "Q1", "Q2", "Q3", "Q4"
        - Composite: "2025Q4", "2025_Q4"
        All formats are normalized to CMS letters for registry lookup.
    """
    # Normalize quarter_vintage to CMS letter format (A/B/C/D)
    if quarter_vintage in ['A', 'B', 'C', 'D']:
        quarter = quarter_vintage  # Direct CMS letter (preferred)
    elif quarter_vintage in ['Q1', 'Q2', 'Q3', 'Q4']:
        quarter_map = {'Q1': 'A', 'Q2': 'B', 'Q3': 'C', 'Q4': 'D'}
        quarter = quarter_map[quarter_vintage]  # Convert Q-notation
    elif 'Q' in quarter_vintage:
        # Handle composite: "2025Q4" → "D"
        q_part = quarter_vintage.split('Q')[-1].strip('_')
        quarter_map = {'1': 'A', '2': 'B', '3': 'C', '4': 'D'}
        quarter = quarter_map.get(q_part, None)
    else:
        quarter = None  # Annual fallback
```

---

## Quarter Mapping

| CMS Letter | Q-Notation | Fiscal Quarter | Typical Release Month | Example Filename |
|------------|------------|----------------|----------------------|------------------|
| **A** | Q1 | January | January | `RVU25A.zip` |
| **B** | Q2 | April | April | `RVU25B.zip` |
| **C** | Q3 | July | July | `RVU25C.zip` |
| **D** | Q4 | October | October | `RVU25D.zip` |

---

## Rationale

### Why CMS Letters (A, B, C, D)?

1. **Matches CMS file naming**: Official releases use `RVU25D`, `GPCI25C`, not `RVU25Q4`
2. **Simpler**: Single letter vs two characters (`D` vs `Q4`)
3. **Less translation overhead**: No need to map letters → Q-notation → letters
4. **Existing precedent**: GPCI already used this convention
5. **More intuitive**: `D` clearly represents the "D" release, not a generic "Q4"

---

## Impact

**All parsers now use consistent notation:**
- ✅ PPRRVU Parser
- ✅ GPCI Parser
- ✅ OPPSCAP Parser
- ✅ ANES Parser
- ✅ Locality Parser

**Backward compatibility maintained:**
- Existing code passing `"Q4"` continues to work (auto-converted to `"D"`)
- No breaking changes to parser APIs
- Ingestors can use either format

---

## Testing

Verified both conventions produce identical results:

```python
# Test 1: CMS letter notation (preferred)
metadata_d = {'quarter_vintage': 'D', ...}
result_d = parse_oppscap(file, filename, metadata_d)
# ✅ 16,100 rows parsed

# Test 2: Q-notation (backward compatible)
metadata_q4 = {'quarter_vintage': 'Q4', ...}
result_q4 = parse_oppscap(file, filename, metadata_q4)
# ✅ 16,100 rows parsed

assert result_d.metrics == result_q4.metrics  # ✅ Identical
```

---

## Usage Guidelines

### For New Code (Preferred)

```python
# Ingestor metadata
metadata = {
    'product_year': 2025,
    'quarter_vintage': 'D',  # ✅ Use CMS letter
    ...
}

# Layout lookup
layout = get_layout('2025', 'D', 'oppscap')  # ✅ Direct letter
```

### For Existing Code (Supported)

```python
# Still works (backward compatible)
metadata = {
    'product_year': 2025,
    'quarter_vintage': 'Q4',  # ✅ Auto-converted to 'D'
    ...
}

layout = get_layout('2025', 'Q4', 'oppscap')  # ✅ Converted internally
```

---

## Files Changed

- `cms_pricing/ingestion/parsers/layout_registry.py`
  - Updated `LAYOUT_REGISTRY` to use CMS letters
  - Enhanced `get_layout()` with conversion logic
  - Added comprehensive docstring
- `CHANGELOG.md` (documented change)

---

## Related PRDs to Update

See: `QUARTER_NOTATION_PRD_UPDATES.md` for specific PRD section updates.

---

## Future Considerations

1. **Ingestor updates**: Update RVU ingestor to extract letter from release ID (`RVU25D` → `'D'`)
2. **Documentation**: Update all parser documentation to use CMS letter examples
3. **Migration**: Gradually migrate existing Q-notation usage to letters in new code
4. **Validation**: Consider adding a helper to validate quarter format before passing to `get_layout()`

---

## References

- **CMS File Naming**: https://www.cms.gov/medicare/physician-fee-schedule/search
- **Implementation Commit**: `f906623` - fix(layout-registry): Standardize quarter notation to CMS letters
- **Testing Commit**: `84d6704` - feat(parser): Complete OPPSCAP Parser v1.0.0

