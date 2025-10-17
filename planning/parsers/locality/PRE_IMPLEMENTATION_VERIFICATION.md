# Locality Parser - Pre-Implementation Format Verification

**Following:** STD-parser-contracts v1.9 §21.4  
**Start Time:** 2025-10-17 10:23:05  
**Parser:** Locality-County Crosswalk  
**Schema:** cms_localitycounty_v1.0.json (v1.1)

---

## Step 1: Inventory All Formats ✅

**Expected Formats:**
- ✅ TXT: `sample_data/rvu25d_0/25LOCCO.txt` (169 rows)
- ❓ CSV: Not found in sample_data
- ❓ XLSX: Not found in sample_data  
- ❓ ZIP: May be in RVU bundle

**Source Location:**
- CMS RVU Bundle: `rvu25d_0.zip` → Contains `25LOCCO.txt`
- Product Year: 2025
- Quarter: D (Annual)

**Status:** ⚠️ Only TXT format available - need to check if CSV/XLSX exist

---

## Step 2: Inspect Headers & Structure

### TXT Format (Fixed-Width)

**Header Inspection:**
```
Row 1: "COUNTIES INCLUDED IN 2025 LOCALITIES. (ALPHABETICALLY BY STATE...)"
Row 2: (blank line)
Row 3: "Medicare AdmiLocality         State   ..."
Row 4: (blank line)
Row 5: "    10112       00    ALABAMA                STATEWIDE ..."
```

**Findings:**
- ✅ 3 header rows to skip
- ⚠️ **Line lengths VARY:** 148-188 chars (NOT consistent!)
  - Blank lines: 1 char
  - Header row: 156 chars
  - Data rows: 148-188 chars (counties list varies)
- ✅ Data start pattern: `^\s*\d{5}` (Medicare Admin code)

**Column Structure (Visual Inspection):**
```
Position  Field                   Example
--------  ----------------------  ---------------------
0-11      medicare_admin_code     "    10112  " (right-aligned, spaces)
12-17     locality_code           "    00 " (spaces)
18-42     state_name              "ALABAMA                " (left-aligned, padded)
43-173    locality_description    "STATEWIDE                          ..."
174-end   counties_list          "ALL COUNTIES" (variable length!)
```

**⚠️ CRITICAL FINDING:** Variable line length due to counties list!
- Shortest: 148 chars (statewide)
- Longest: 188 chars (multi-county areas)
- **Implication:** `min_line_length` must be conservative (~140)

---

## Step 3: Schema vs File Structure Analysis

### Schema Expectations (cms_localitycounty_v1.0.json)

**Natural Keys:**
```json
["locality_code", "state_fips", "county_fips"]
```

**Columns Expected:**
- `locality_code` (String, 2-digit)
- `state_fips` (String, 2-digit) 
- `county_fips` (String, 3-digit)
- `locality_name`
- `effective_from` / `effective_to`

### File Reality (25LOCCO.txt)

**Columns Present:**
- `medicare_admin_code` (5-digit)
- `locality_code` (2-digit)
- `state_name` (Text, not FIPS!)
- `locality_description`
- `counties_list` (Text, variable)

### ⚠️ **CRITICAL GAP: Schema Mismatch!**

**Missing in File:**
- ❌ `state_fips` - File has state NAME not FIPS
- ❌ `county_fips` - File has county NAMES not FIPS

**Present in File:**
- ✅ `medicare_admin_code` - Not in schema
- ✅ `locality_code` - Matches schema
- ✅ State/locality names - But not FIPS codes

**Parser Implication:**
- **Either:** Schema is wrong (should use names not FIPS)
- **Or:** Parser must DERIVE FIPS from names (lookup table needed)
- **Or:** This is wrong file (need different source)

**⚠️ BLOCKER:** Need to resolve schema-file mismatch before implementation!

---

## Step 4: Layout Registry Check

**Checking:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Expected Key:** `('localitycounty', '2025', 'Q4')` or `('locality', '2025', 'Q4')`

**Status:** ⏳ Need to check if layout exists

**If No Layout:** Need to create layout with column positions from visual inspection above

---

## Step 5: Format-Specific Quirks Documented

| Format | Quirk | Impact | Solution |
|--------|-------|--------|----------|
| TXT | Variable line length (148-188) | Cannot use strict min_line_length | Use ~140, rely on data pattern |
| TXT | Counties list is variable width | No fixed end position | Parse as "rest of line" |
| TXT | State names not FIPS codes | Schema expects FIPS | Need lookup table |
| TXT | 3 header rows + 1 blank | Wrong skiprows breaks parsing | Dynamic detection or skip rows=4 |

---

## Step 6: Test Matrix (Preliminary)

| Format | Test Type | Fixture | Expected Rows | Expected Rejects |
|--------|-----------|---------|---------------|------------------|
| TXT | golden | LOCCO2025_sample.txt | ~20 | 0 |
| TXT | full | 25LOCCO.txt | 169 | ? |

**Note:** Cannot plan CSV/XLSX tests until we find those files

---

## Step 7: Signoff Checklist

**Before Coding:**
- ✅ TXT format inspected (variable line length identified)
- ✅ Resolve schema-file mismatch (FIPS vs names) → Two-stage architecture
- Check if CSV/XLSX formats exist
- ✅ Create or verify layout in layout_registry.py (LOCCO_2025D_LAYOUT exists)
- ✅ Confirm natural keys (decided: raw uses names, enrich derives FIPS)
- ✅ Determine if FIPS lookup is parser responsibility (NO - enrich stage)

---

## 🚨 **BLOCKER IDENTIFIED**

**Issue:** Schema expects FIPS codes, file contains names

**Options:**
1. **Update schema** to use state_name + county_names (simpler)
2. **Add FIPS lookup** in parser (state/county name → FIPS code)
3. **Use different source** file that has FIPS codes

**Next Action:** Check if there's a different locality file with FIPS codes, OR decide to update schema

---

**Time Spent:** ~15 minutes (format inspection)  
**Time Saved:** Unknown (but prevented implementing wrong thing!)  
**Next:** Resolve schema mismatch before proceeding

---

*This is exactly what §21.4 is designed to catch!* 🎯

