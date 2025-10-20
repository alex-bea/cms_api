# Quarter Notation Standard - PRD Update Recommendations

**Version:** 1.0  
**Date:** 2025-10-20  
**Status:** READY FOR REVIEW  
**Related:** `QUARTER_NOTATION_STANDARD.md`

---

## Overview

This document provides specific recommendations for updating PRDs to codify the **CMS Letter Quarter Notation Standard** (A, B, C, D).

---

## PRD Updates Needed

### **1. REF-parser-routing-detection-v1.0.md**

**Section:** §3 "Layout Registry"

**Current Status:** Mentions layout registry but doesn't specify quarter notation convention.

**Add New Subsection:** §3.4 "Quarter Notation Convention"

```markdown
### §3.4 Quarter Notation Convention

**Standard:** CMS Letter Format (A, B, C, D)

Quarter notation in `LAYOUT_REGISTRY` keys uses CMS letter format to match official file naming:

| CMS Letter | Q-Notation | Fiscal Quarter | Example Filename |
|------------|------------|----------------|------------------|
| **A** | Q1 | January | `RVU25A.zip` |
| **B** | Q2 | April | `RVU25B.zip` |
| **C** | Q3 | July | `RVU25C.zip` |
| **D** | Q4 | October | `RVU25D.zip` |

**Registry Key Format:**
```python
LAYOUT_REGISTRY = {
    # Format: (dataset, year, quarter) -> layout
    # Quarter: CMS letter (A/B/C/D)
    ('pprrvu', '2025', 'D'): PPRRVU_2025D_LAYOUT,
    ('gpci', '2025', 'C'): GPCI_2025D_LAYOUT,
    ...
}
```

**Backward Compatibility:**

`get_layout()` accepts multiple formats for backward compatibility:
- **CMS letters** (preferred): `"A"`, `"B"`, `"C"`, `"D"`
- **Q-notation** (supported): `"Q1"`, `"Q2"`, `"Q3"`, `"Q4"` (auto-converted)
- **Composite** (supported): `"2025Q4"`, `"2025_Q4"` (parsed and converted)

All formats are normalized to CMS letters internally for registry lookup.

**Rationale:**
- Matches CMS official file naming convention
- Simpler (1 char vs 2 chars)
- Less translation overhead
- Already used by GPCI dataset

**Rule:** `R-LAYOUT-001` - All `LAYOUT_REGISTRY` keys MUST use CMS letter notation (A/B/C/D)  
**Rule:** `R-LAYOUT-002` - Layout lookup functions MUST support backward-compatible conversion from Q-notation
```

---

### **2. STD-parser-contracts-impl-v2.0.md**

**Section:** §2 "Parser Metadata Requirements"

**Current Status:** Mentions `quarter_vintage` but doesn't specify format.

**Update Subsection:** §2.1 "Required Metadata Fields"

**Add Detail for `quarter_vintage` field:**

```markdown
#### `quarter_vintage` (string)

Quarter release identifier in CMS letter format.

**Preferred Format:** CMS letter (`"A"`, `"B"`, `"C"`, `"D"`)  
**Supported Formats:**
- CMS letters: `"A"`, `"B"`, `"C"`, `"D"` (preferred)
- Q-notation: `"Q1"`, `"Q2"`, `"Q3"`, `"Q4"` (backward compatible)
- Composite: `"2025Q4"`, `"2025_Q4"` (parsed)

**Mapping:**
- `A` / `Q1` = January release
- `B` / `Q2` = April release
- `C` / `Q3` = July release
- `D` / `Q4` = October release

**Example:**
```python
metadata = {
    'release_id': 'OPPSCAP_2025D',
    'product_year': 2025,
    'quarter_vintage': 'D',  # ✅ Preferred: CMS letter
    ...
}

# Also supported (backward compatible):
metadata = {
    'quarter_vintage': 'Q4',  # ✅ Auto-converted to 'D'
}
```

**Rationale:** Aligns with CMS file naming convention (e.g., `RVU25D.zip`).

**Rule:** `R-META-003` - Ingestors SHOULD populate `quarter_vintage` with CMS letter format  
**Rule:** `R-META-004` - Parsers MUST accept both CMS letter and Q-notation formats via `get_layout()`
```

---

### **3. REF-parser-reference-appendix-v1.0.md**

**Section:** Appendix A "CMS File Naming Conventions"

**Current Status:** May reference RVU file names but doesn't explain quarter notation.

**Add New Section:** §A.2 "Quarter Release Notation"

```markdown
### §A.2 Quarter Release Notation

CMS Medicare fee schedule files use **letter notation** (A, B, C, D) to indicate quarterly releases:

**Format:** `<Dataset><Year><Quarter>.<ext>`

**Examples:**
- `RVU25A.zip` - January 2025 release (Q1)
- `RVU25B.zip` - April 2025 release (Q2)
- `RVU25C.zip` - July 2025 release (Q3)
- `RVU25D.zip` - October 2025 release (Q4)

**Quarter Letter Mapping:**

| Letter | Quarter | Month | Typical Release Date |
|--------|---------|-------|---------------------|
| **A** | Q1 | January | First week of January |
| **B** | Q2 | April | First week of April |
| **C** | Q3 | July | First week of July |
| **D** | Q4 | October | First week of October |

**Internal Representation:**

The CMS Pricing API uses this letter notation throughout the system:
- Layout registry keys: `('pprrvu', '2025', 'D')`
- Metadata fields: `quarter_vintage: 'D'`
- Release IDs: `OPPSCAP_2025D`

**Alternative Notations:**

Q-notation (`Q1`, `Q2`, `Q3`, `Q4`) is supported for backward compatibility but CMS letter format is preferred.

**See Also:** `planning/standards/QUARTER_NOTATION_STANDARD.md`
```

---

### **4. RUN-parser-qa-runbook-prd-v1.0.md**

**Section:** §2 "Pre-Implementation Checklist"

**Current Status:** Has Step 1 "Inventory All Formats" but doesn't mention quarter notation.

**Update Step:** Step 1b "Extract metadata from filenames"

**Add Guidance:**

```markdown
#### Step 1b: Extract Metadata from Filenames

When inventorying CMS files, extract:
- Dataset name (e.g., `RVU`, `GPCI`, `OPPSCAP`)
- Product year (e.g., `2025`)
- **Quarter release** (e.g., `D` for October/Q4)
- File format (e.g., `.txt`, `.csv`, `.zip`)

**Quarter Extraction:**

CMS uses **letter notation** (A, B, C, D):

```python
import re

filename = "RVU25D.zip"
match = re.match(r'^([A-Z]+)(\d{2})([A-D])\.(zip|txt|csv|xlsx)$', filename)
if match:
    dataset = match.group(1)    # "RVU"
    year = f"20{match.group(2)}"  # "2025"
    quarter = match.group(3)    # "D"  ← CMS letter format
    format = match.group(4)     # "zip"
```

**Mapping to Layout Registry:**

```python
# Layout lookup uses CMS letters directly
layout = get_layout(
    product_year=year,        # "2025"
    quarter_vintage=quarter,  # "D" (not "Q4")
    dataset=dataset.lower()   # "rvu"
)
```

**Common Mistake:** ❌ Don't convert `D` → `Q4` before calling `get_layout()`. The function handles conversion internally.

✅ **Correct:**
```python
quarter_vintage = 'D'  # Extract from filename directly
```

❌ **Incorrect:**
```python
quarter_map = {'A': 'Q1', 'B': 'Q2', 'C': 'Q3', 'D': 'Q4'}
quarter_vintage = quarter_map[quarter]  # Unnecessary conversion
```
```

---

### **5. STD-data-architecture-impl-v1.0.md**

**Section:** §1.2 "Metadata Standards"

**Current Status:** Defines metadata fields but doesn't specify quarter format.

**Add Detail:** §1.2.3 "Quarter Vintage Encoding"

```markdown
### §1.2.3 Quarter Vintage Encoding

**Standard:** CMS Letter Format (A, B, C, D)

Quarter releases are encoded using CMS letter notation to match official file naming:

```python
# Preferred encoding
quarter_vintage = 'D'  # October/Q4 release

# Mapping table
QUARTER_MAPPING = {
    'A': {'q_notation': 'Q1', 'month': 'January', 'fiscal_quarter': 1},
    'B': {'q_notation': 'Q2', 'month': 'April', 'fiscal_quarter': 2},
    'C': {'q_notation': 'Q3', 'month': 'July', 'fiscal_quarter': 3},
    'D': {'q_notation': 'Q4', 'month': 'October', 'fiscal_quarter': 4},
}
```

**Database Storage:**

Store `quarter_vintage` as CMS letter (1 char) in all metadata tables:

```sql
CREATE TABLE cms_release (
    release_id VARCHAR(50) PRIMARY KEY,
    product_year INTEGER NOT NULL,
    quarter_vintage CHAR(1) CHECK (quarter_vintage IN ('A', 'B', 'C', 'D')),
    ...
);
```

**API Responses:**

Return CMS letter in API responses for consistency:

```json
{
  "release_id": "OPPSCAP_2025D",
  "product_year": 2025,
  "quarter_vintage": "D",
  "quarter_label": "Q4 (October)",
  ...
}
```

**Rule:** `R-DATA-007` - All database schemas MUST store `quarter_vintage` as CMS letter (CHAR(1))  
**Rule:** `R-DATA-008` - APIs SHOULD return both CMS letter and human-readable quarter label
```

---

### **6. STD-observability-monitoring-prd-v1.0.md**

**Section:** §3 "Structured Logging"

**Current Status:** Defines logging standards but doesn't specify quarter format in logs.

**Add Best Practice:** "Quarter Vintage in Logs"

```markdown
### §3.5 Quarter Vintage Logging

When logging release or parsing events, include `quarter_vintage` in CMS letter format:

✅ **Correct:**
```python
logger.info(
    "parse_start",
    dataset="oppscap",
    product_year=2025,
    quarter_vintage="D",  # CMS letter
    filename="OPPSCAP_Oct.txt"
)
```

❌ **Incorrect:**
```python
# Don't use Q-notation in logs
logger.info(
    "parse_start",
    quarter="Q4",  # Avoid Q-notation
    ...
)
```

**Rationale:** Consistent with CMS file naming and internal encoding. Easier to correlate logs with source files.

**Metrics Tags:**

Use CMS letters in metric tags:

```python
metrics.increment(
    "parser.rows_parsed",
    tags={
        "dataset": "oppscap",
        "quarter": "D",  # ✅ CMS letter
        "year": "2025",
    }
)
```
```

---

## Implementation Checklist

**PRD Updates Needed:**
1. Update `REF-parser-routing-detection-v1.0.md` §3.4 (Quarter Notation Convention)
2. Update `STD-parser-contracts-impl-v2.0.md` §2.1 (`quarter_vintage` field detail)
3. Update `REF-parser-reference-appendix-v1.0.md` §A.2 (Quarter Release Notation)
4. Update `RUN-parser-qa-runbook-prd-v1.0.md` Step 1b (Quarter Extraction)
5. Update `STD-data-architecture-impl-v1.0.md` §1.2.3 (Quarter Vintage Encoding)
6. Update `STD-observability-monitoring-prd-v1.0.md` §3.5 (Quarter Logging)

**Additional Tasks:**
7. Add cross-references to `QUARTER_NOTATION_STANDARD.md` in all updated PRDs
8. Update example code snippets across PRDs to use `'D'` instead of `'Q4'`
9. Add migration notes for teams using Q-notation

---

## Migration Notes

**For Teams Using Q-Notation:**

1. **No immediate action required** - Backward compatibility maintained
2. **New code** - Use CMS letters (`'D'` instead of `'Q4'`)
3. **Existing code** - No need to update, but consider migrating gradually
4. **Database schemas** - Use `CHAR(1)` for `quarter_vintage` column
5. **API contracts** - Document that both formats are accepted, CMS letters preferred

---

## Example PRD Update Template

For each PRD update, use this template:

```markdown
---

**Change Log:**
- **2025-10-20:** Added §X.X "Quarter Notation" to standardize CMS letter format (A/B/C/D)
  - **Rationale:** Aligns with CMS file naming convention and internal encoding
  - **Impact:** No breaking changes, backward compatible with Q-notation
  - **See Also:** `planning/standards/QUARTER_NOTATION_STANDARD.md`

---
```

---

## Summary

**6 PRDs to Update:**
1. ✅ `REF-parser-routing-detection-v1.0.md` - Add §3.4 quarter convention
2. ✅ `STD-parser-contracts-impl-v2.0.md` - Detail `quarter_vintage` field
3. ✅ `REF-parser-reference-appendix-v1.0.md` - Add §A.2 CMS notation guide
4. ✅ `RUN-parser-qa-runbook-prd-v1.0.md` - Update filename extraction step
5. ✅ `STD-data-architecture-impl-v1.0.md` - Add §1.2.3 encoding standard
6. ✅ `STD-observability-monitoring-prd-v1.0.md` - Add §3.5 logging guidance

**Benefits:**
- ✅ Consistent with CMS file naming
- ✅ Simpler (1 char vs 2)
- ✅ Less translation overhead
- ✅ Backward compatible
- ✅ Clear documentation for future developers

**Time Estimate:** ~2 hours total (20 min per PRD)

