# GPCI Parser - Pre-Implementation Plan

**Date:** 2025-10-16  
**Duration:** 25 minutes  
**Purpose:** Prepare layout registry and verify sample data before coding

**Follow-up:** `IMPLEMENTATION.md` (main parser implementation)

---

## 🎯 **Goals**

1. ✅ Measure actual line length from sample data
2. ✅ Update `GPCI_2025D_LAYOUT` to v2025.4.1 (CMS-native names)
3. ✅ Verify layout against sample data
4. ✅ Document measurements for CI reference

---

## 📋 **Step-by-Step Checklist**

### Step 1: Measure Line Length (5 min)

**Purpose:** Determine correct `min_line_length` for fixed-width parsing

**Commands:**
```bash
cd /Users/alexanderbea/Cursor/cms-api

# Examine line lengths in a window (quick look)
echo "=== TXT File Line Lengths (window sample) ==="
head -40 sample_data/rvu25d_0/GPCI2025.txt | tail -20 | awk '{print length($0)}'

# Full-file min/max across all *data* lines (5-digit MAC at start)
echo ""
echo "=== Full-File Line Lengths (data lines) ==="
awk '/^[0-9]{5}/{print length($0)}' sample_data/rvu25d_0/GPCI2025.txt | sort -n | \
awk 'NR==1{min=$1} {max=$1} END{print "MIN:",min; print "MAX:",max}'

# Sample first few data lines
echo ""
echo "=== First 3 Data Lines ==="
awk '/^[0-9]{5}/ {print} NR>=3{exit}' sample_data/rvu25d_0/GPCI2025.txt

# Provenance: SHA-256 of the sample file
echo ""
echo "=== SHA-256 (sample file) ==="
shasum -a 256 sample_data/rvu25d_0/GPCI2025.txt | awk '{print $1}'
```

**Expected Output:**
- Line lengths: ~130-145 characters (varies by locality_name length)
- Shortest line: ~130 chars (short locality names)
- Longest line: ~145 chars (long locality names like "BAKERSFIELD")

**Decision:** Set `min_line_length: 100` (conservative, allows margin)

**Document results:**
```bash
# Record findings
echo "# GPCI Line Length Analysis (2025-10-16)
Sample file: sample_data/rvu25d_0/GPCI2025.txt
Sample file SHA-256: <PASTE_SHA256_HERE>
Window (head-40) min length: XXX
Window (head-40) max length: XXX
Full-file MIN length (data lines): XXX
Full-file MAX length (data lines): XXX
Recommended min_line_length: 100 (conservative with margin)
" > planning/parsers/gpci/LINE_LENGTH_ANALYSIS.md
```

---

### Step 2: Verify Column Positions (10 min)

**Purpose:** Confirm layout column positions match actual data

**Commands:**
```bash
cd /Users/alexanderbea/Cursor/cms-api

# Extract one data line for manual inspection
echo "=== Sample Data Line with Positions ==="
head -10 sample_data/rvu25d_0/GPCI2025.txt | tail -1 > /tmp/gpci_sample_line.txt

# Show with character positions (manual check)
python3 << 'EOF'
with open('/tmp/gpci_sample_line.txt', 'r') as f:
    line = f.read().rstrip('\r\n')

# Colspec width asserts (end is EXCLUSIVE)
assert 26 - 24 == 2   # locality_code width
assert 125 - 120 == 5 # gpci_work width
assert 138 - 133 == 5 # gpci_pe width
assert 145 - 140 == 5 # gpci_mp width

print("=== Character Position Map ===")
print("Pos  0-5   (MAC):          ", repr(line[0:5]))
print("Pos 15-17  (State):        ", repr(line[15:17]))
print("Pos 24-26  (Locality Code):", repr(line[24:26]))
print("Pos 28-80  (Locality Name):", repr(line[28:80]))
print("Pos 120-125 (GPCI Work):   ", repr(line[120:125]))
print("Pos 133-138 (GPCI PE):     ", repr(line[133:138]))
print("Pos 140-145 (GPCI MP):     ", repr(line[140:145]))
print()
print(f"Total line length: {len(line)}")
EOF
```

**Expected verification:**
- MAC (0:5): "01112" or similar (5 digits)
- State (15:17): "CA", "AL", "AK" (2 letters)
- Locality (24:26): "54", "00", "01" (2 digits)
- GPCI values: Decimal values like "1.017", "0.869"

**Action:** If positions don't match, adjust layout accordingly

---

### Step 3: Update Layout Registry (10 min)

**File:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Current location:** Lines 75-88 (GPCI_2025D_LAYOUT)

**Changes to make:**

```python
# Replace GPCI_2025D_LAYOUT (v2025.4.0 → v2025.4.1)

GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Patch bump for CMS-native column alignment
    'source_version': '2025D',
    'min_line_length': 100,  # Set from Step 1 measurement
    'data_start_pattern': r'^\d{5}',  # MAC code (5 digits) at line start
    'columns': {
        # Core schema columns (CMS-native names) - REQUIRED for hash
        'locality_code': {'start': 24, 'end': 26, 'type': 'string', 'nullable': False},
        'gpci_work':     {'start': 120, 'end': 125, 'type': 'decimal', 'nullable': False},
        'gpci_pe':       {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': False},
        'gpci_mp':       {'start': 140, 'end': 145, 'type': 'decimal', 'nullable': False},
        
        # Optional enrichment columns - excluded from hash
        'mac':           {'start': 0, 'end': 5, 'type': 'string', 'nullable': True},
        'state':         {'start': 15, 'end': 17, 'type': 'string', 'nullable': True},
        'locality_name': {'start': 28, 'end': 80, 'type': 'string', 'nullable': True},
    }
}

# Update registry lookup (around line 153)
# Ensure these entries exist:
LAYOUT_REGISTRY = {
    # ... existing entries ...
    ('gpci', '2025', 'A'): GPCI_2025D_LAYOUT,  # Validate quarters before reuse
    ('gpci', '2025', 'B'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'C'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'D'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', None): GPCI_2025D_LAYOUT,  # Year fallback (use cautiously)
}
```

**Layout lookup contract:** `get_layout(dataset: str, product_year: str|int, quarter_vintage: 'A'|'B'|'C'|'D'|None)` → layout dict or `None`. Prefer explicit per‑quarter entries you have validated; use the year fallback only if you have verified format stability across quarters.

**Key changes:**
1. ✅ `locality_id` → `locality_code`
2. ✅ `work_gpci` → `gpci_work`
3. ✅ `pe_gpci` → `gpci_pe`
4. ✅ `mp_gpci` → `gpci_mp`
5. ✅ Added `data_start_pattern`
6. ✅ Set `min_line_length` from measurement
7. ✅ Marked enrichment columns
- ✅ `state` enrichment may be blank; keep it **nullable** and `.str.strip()` it in the parser

**Save and commit:**
```bash
git add cms_pricing/ingestion/parsers/layout_registry.py
# Don't commit yet - wait for full parser implementation
```

---

### Step 4: Verify Layout with Sample Data (5 min)

**Purpose:** Quick smoke test that layout parses correctly

**Test script:**
```bash
cd /Users/alexanderbea/Cursor/cms-api

python3 << 'EOF'
import pandas as pd
import re
from io import StringIO
from cms_pricing.ingestion.parsers.layout_registry import get_layout

# Load layout using A/B/C/D quarter key
layout = get_layout(
    product_year="2025",
    quarter_vintage="D",  # Use A/B/C/D to match RVU25A-D
    dataset="gpci"
)

print("=== Layout Retrieved ===")
print(f"Version: {layout['version']}")
print(f"Min line length: {layout['min_line_length']}")
print(f"Columns: {list(layout['columns'].keys())}")
print()

# Read sample data
with open('sample_data/rvu25d_0/GPCI2025.txt', 'r') as f:
    lines = f.readlines()

# Find data start robustly
data_start = 0
for i, raw in enumerate(lines):
    line = raw.rstrip('\r\n')
    if len(line) >= layout['min_line_length'] and re.match(layout['data_start_pattern'], line):
        data_start = i
        print(f"Data starts at line: {i}")
        break

# Build colspecs
colspecs = [(c['start'], c['end']) for c in layout['columns'].values()]
names = list(layout['columns'].keys())

# Parse first 5 data rows
df = pd.read_fwf(
    StringIO('\n'.join([l.rstrip('\r\n') for l in lines[data_start:data_start+5]])),
    colspecs=colspecs,
    names=names,
    dtype=str
)

# Sanity asserts on widths and numeric domains
assert layout['columns']['gpci_work']['end'] - layout['columns']['gpci_work']['start'] == 5
assert layout['columns']['gpci_pe']['end'] - layout['columns']['gpci_pe']['start'] == 5
assert layout['columns']['gpci_mp']['end'] - layout['columns']['gpci_mp']['start'] == 5
assert layout['columns']['locality_code']['end'] - layout['columns']['locality_code']['start'] == 2

print("\n=== Parsed Sample (First 5 Rows) ===")
print(df[['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp']])

print("\n=== Column Names Verification ===")
print("✓ locality_code present:", 'locality_code' in df.columns)
print("✓ gpci_work present:", 'gpci_work' in df.columns)
print("✓ gpci_pe present:", 'gpci_pe' in df.columns)
print("✓ gpci_mp present:", 'gpci_mp' in df.columns)

# Domain checks (convert to float)
vals = pd.concat([df['gpci_work'], df['gpci_pe'], df['gpci_mp']]).astype(float)
assert 0 < vals.min() and vals.max() < 3.0
print("\nParsed sample row count:", len(df))

print("\n=== Sample Values ===")
if len(df) > 0:
    row = df.iloc[0]
    print(f"Locality: {row['locality_code']}")
    print(f"Work GPCI: {row['gpci_work']}")
    print(f"PE GPCI: {row['gpci_pe']}")
    print(f"MP GPCI: {row['gpci_mp']}")
    if 'locality_name' in df.columns:
        print(f"Name: {row['locality_name']}")
    if 'state' in df.columns:
        print(f"State: {str(row['state']).strip()}")

print("\n✅ Layout verification complete!")
EOF
```

**Expected output:**
```
Version: v2025.4.1
Columns: ['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 'mac', 'state', 'locality_name']
✓ locality_code present: True
✓ gpci_work present: True
✓ gpci_pe present: True
✓ gpci_mp present: True
Locality: 00
Work GPCI: 1.000
PE GPCI: 0.869
MP GPCI: 0.575
Name: ALABAMA
```

**If verification fails:**
- Check column start/end positions
- Verify `min_line_length` setting
- Adjust layout and re-test

---

## ✅ **Pre-Implementation Checklist**

**Completed:**
-  Line lengths measured from sample data
-  `min_line_length` determined (recommended: 100)
-  Column positions verified against actual data
-  `GPCI_2025D_LAYOUT` updated to v2025.4.1
-  Column names match schema v1.2 exactly:
  -  `locality_code` (not `locality_id`)
  -  `gpci_work` (not `work_gpci`)
  -  `gpci_pe` (not `pe_gpci`)
  -  `gpci_mp` (not `mp_gpci`)
-  Registry lookup entries added for 2025 Q1-Q4
-  Layout verified with sample data (smoke test passed)
-  Results documented in `LINE_LENGTH_ANALYSIS.md`

**Ready for:**
- ✅ Parser implementation (`IMPLEMENTATION.md`)
- ✅ Golden fixture extraction
- ✅ Test writing

---

## 📊 **Expected Results**

### Layout Registry
**File:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Changes:**
- Lines 75-88: `GPCI_2025D_LAYOUT` updated
- Lines 153-157: Registry lookup entries added
- Version: v2025.4.0 → v2025.4.1

### Documentation
**File:** `planning/parsers/gpci/LINE_LENGTH_ANALYSIS.md`

**Contents:**
- Minimum observed line length
- Maximum observed line length
- Recommended `min_line_length` setting
- Column position verification results

---

## 🚨 **Common Issues & Solutions**

### Issue 1: Line lengths vary significantly
**Cause:** Locality names have different lengths  
**Solution:** Set conservative `min_line_length` (e.g., 100) to allow shortest lines

### Issue 2: Column positions don't align
**Cause:** Layout may be from different CMS release  
**Solution:** Manually inspect sample file, adjust start/end positions

### Issue 3: Data start detection fails
**Cause:** Header format changed or `data_start_pattern` incorrect  
**Solution:** Verify pattern matches MAC code format (`^\d{5}`)

### Issue 4: Columns parse as empty
**Cause:** Start/end positions off by one (inclusive/exclusive issue)  
**Solution:** Remember `end` is EXCLUSIVE for `read_fwf()` and slicing

---

## ⏭️ **Next Steps After Pre-Implementation**

Once checklist complete:
1. ✅ Layout registry updated and verified
2. ➡️ **Extract golden fixtures** (15 min)
   - File: `planning/parsers/gpci/IMPLEMENTATION.md` - Step "Extract Golden Fixture"
3. ➡️ **Write golden test** (10 min)
4. ➡️ **Implement parser** (50 min)

---

## 📁 **Files to Modify**

| File | Lines | What to Change |
|------|-------|----------------|
| `cms_pricing/ingestion/parsers/layout_registry.py` | 75-88 | `GPCI_2025D_LAYOUT` definition |
| `cms_pricing/ingestion/parsers/layout_registry.py` | 153-157 | Registry lookup entries |
| `planning/parsers/gpci/LINE_LENGTH_ANALYSIS.md` | NEW | Document measurements |

---

## ✅ **Completion Criteria**

**Pre-implementation is complete when:**
- ✅ Line lengths measured and documented
- ✅ Layout v2025.4.1 updated with CMS-native names
- ✅ Smoke test passes (parses 5 sample rows correctly)
- ✅ All column names match schema v1.2
- ✅ Ready to start parser implementation

---

**Time Budget:** 25 minutes  
**Next Phase:** Golden-First Implementation (75 minutes)

**Ready to execute!** Follow this plan step-by-step before coding the parser.
