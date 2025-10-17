# PPRRVU Parser - Next Session Execution Plan

**Time Required:** 20-25 minutes  
**Confidence:** High (root cause identified)  
**Fallback:** CSV-first if > 30 min  

---

## 🎯 Root Cause (CONFIRMED)

**Schema-Layout Column Name Mismatch:**

| What | Layout Has | Schema Expects | Status |
|------|------------|----------------|--------|
| Work RVU | `work_rvu` | `rvu_work` | ❌ WRONG |
| PE Non-Fac | `pe_rvu_nonfac` | `rvu_pe_nonfac` | ❌ WRONG |
| PE Fac | `pe_rvu_fac` | `rvu_pe_fac` | ❌ WRONG |
| Malpractice | `mp_rvu` | `rvu_malp` | ❌ WRONG |
| Modifier | ❌ MISSING | `modifier` | 🚨 CRITICAL (natural key!) |
| Effective From | ❌ MISSING | `effective_from` | 🚨 CRITICAL (natural key!) |
| OPPS Cap | ❌ MISSING | `opps_cap_applicable` | ⚠️ NEEDED |

**Impact:** Parser creates DataFrame with wrong column names → `KeyError` when schema validation tries to access `hcpcs` (which exists but other natural keys are missing)

---

## ✅ Recommended Fix Strategy

### Approach: Align Layout with Schema Contract

**Why this approach:**
- Schema is authoritative (defines contract)
- Layout should serve schema
- Clean, no workarounds
- Fixes root cause permanently

**Time:** 20-25 minutes

---

## 📋 Step-by-Step Execution

### STEP 1: Inspect Source File for Column Positions (5 min)

```bash
cd /Users/alexanderbea/Cursor/cms-api

# Look at actual data to find modifier, effective_from, opps_cap positions
head -20 tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt | tail -10

# Or check source file
head -20 sample_data/rvu25d_0/PPRRVU2025_Oct.txt | tail -10

# Expected format (CMS fixed-width):
# Position 0-4: HCPCS
# Position 5-6: Modifier (2 chars, often blank)
# Position 57-58: Status
# Position 61-65: Work RVU
# ... find effective_from and opps_cap positions
```

**Output:** Document exact column positions for modifier, effective_from, opps_cap

---

### STEP 2: Update Layout Registry (10 min)

Edit `cms_pricing/ingestion/parsers/layout_registry.py`:

```python
PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Bump patch (column names changed)
    'min_line_length': 200,
    'source_version': '2025D',
    'columns': {
        # Core identifiers (ALIGNED WITH SCHEMA)
        'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': True},  # ← ADD (natural key!)
        'description': {'start': 7, 'end': 57, 'type': 'string', 'nullable': True},
        'status_code': {'start': 57, 'end': 58, 'type': 'string', 'nullable': False},
        
        # RVU columns (RENAMED TO MATCH SCHEMA)
        'rvu_work': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},  # was work_rvu
        'rvu_pe_nonfac': {'start': 68, 'end': 72, 'type': 'decimal', 'nullable': True},  # was pe_rvu_nonfac
        'rvu_pe_fac': {'start': 77, 'end': 81, 'type': 'decimal', 'nullable': True},  # was pe_rvu_fac
        'rvu_malp': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},  # was mp_rvu
        
        # Other fields
        'na_indicator': {'start': 92, 'end': 93, 'type': 'string', 'nullable': True},
        
        # Keep useful columns (not in schema but helpful)
        'bilateral_ind': {'start': 103, 'end': 104, 'type': 'string', 'nullable': True},
        'multiple_proc_ind': {'start': 104, 'end': 105, 'type': 'string', 'nullable': True},
        'assistant_surg_ind': {'start': 105, 'end': 106, 'type': 'string', 'nullable': True},
        'co_surg_ind': {'start': 106, 'end': 107, 'type': 'string', 'nullable': True},
        'team_surg_ind': {'start': 107, 'end': 108, 'type': 'string', 'nullable': True},
        'endoscopic_base': {'start': 108, 'end': 109, 'type': 'string', 'nullable': True},
        'conversion_factor': {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': True},
        'global_days': {'start': 140, 'end': 143, 'type': 'string', 'nullable': True},
        'physician_supervision': {'start': 144, 'end': 146, 'type': 'string', 'nullable': True},
        'diag_imaging_family': {'start': 147, 'end': 148, 'type': 'string', 'nullable': True},
        'total_nonfac': {'start': 153, 'end': 157, 'type': 'decimal', 'nullable': True},
        'total_fac': {'start': 160, 'end': 164, 'type': 'decimal', 'nullable': True},
        
        # Add missing schema fields (find positions in Step 1)
        'opps_cap_applicable': {'start': ???, 'end': ???, 'type': 'string', 'nullable': True},  # ← FIND
        'effective_from': {'start': ???, 'end': ???, 'type': 'string', 'nullable': True},  # ← FIND OR inject from metadata
    }
}
```

**Note:** If `effective_from` isn't in the file, handle in `_cast_dtypes()`:
```python
# In _cast_dtypes():
if 'effective_from' not in df.columns:
    df['effective_from'] = metadata.get('vintage_date', pd.Timestamp('2025-01-01'))
```

---

### STEP 3: Update Parser Column References (3 min)

Edit `cms_pricing/ingestion/parsers/pprrvu_parser.py`:

**In `_normalize_column_names()`:**
```python
def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names.
    
    Fixed-width layout already uses schema names - skip normalization.
    CSV/XLSX may use different names - apply aliases.
    """
    # Check if columns already canonical (from layout)
    schema_cols = {'hcpcs', 'modifier', 'status_code', 'rvu_work', 'rvu_pe_nonfac'}
    
    if schema_cols.issubset(set(df.columns)):
        # Layout already canonical - no normalization needed
        logger.debug("Columns canonical from layout, skipping normalization")
        return df
    
    # Apply aliases for CSV/XLSX formats
    COLUMN_ALIASES = {
        'HCPCS': 'hcpcs',
        'HCPCS_CODE': 'hcpcs',
        'MOD': 'modifier',
        'MODIFIER': 'modifier',
        'STATUS': 'status_code',
        'STATUS_CODE': 'status_code',
        
        # CSV may use old or new names - support both
        'WORK_RVU': 'rvu_work',
        'RVU_WORK': 'rvu_work',
        'PE_NONFAC_RVU': 'rvu_pe_nonfac',
        'RVU_PE_NONFAC': 'rvu_pe_nonfac',
        'PE_FAC_RVU': 'rvu_pe_fac',
        'RVU_PE_FAC': 'rvu_pe_fac',
        'MALP_RVU': 'rvu_malp',
        'MP_RVU': 'rvu_malp',
        'RVU_MALP': 'rvu_malp',
        
        'NA_IND': 'na_indicator',
        'NA_INDICATOR': 'na_indicator',
        'OPPS_CAP': 'opps_cap_applicable',
        'GLOBAL_DAYS': 'global_days',
        'EFFECTIVE_DATE': 'effective_from',
        'EFFECTIVE_FROM': 'effective_from',
    }
    
    df = df.copy()
    df.columns = [
        COLUMN_ALIASES.get(c.strip().upper(), c.lower().strip().replace(' ', '_'))
        for c in df.columns
    ]
    return df
```

**In `_cast_dtypes()`:**
```python
def _cast_dtypes(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """Cast using schema column names."""
    df = df.copy()
    
    # Codes
    if 'hcpcs' in df.columns:
        df['hcpcs'] = df['hcpcs'].astype(str).str.strip().str.upper()
    
    if 'modifier' in df.columns:
        df['modifier'] = df['modifier'].fillna('').astype(str).str.strip().str.upper()
        df.loc[df['modifier'] == '', 'modifier'] = None
    
    if 'status_code' in df.columns:
        df['status_code'] = df['status_code'].astype(str).str.strip().str.upper()
    
    # RVUs (USING SCHEMA NAMES)
    rvu_cols = ['rvu_work', 'rvu_pe_nonfac', 'rvu_pe_fac', 'rvu_malp']
    for col in rvu_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = canonicalize_numeric_col(df[col], precision=2, rounding='HALF_UP')
    
    # Global days
    if 'global_days' in df.columns:
        df['global_days'] = pd.to_numeric(df['global_days'], errors='coerce').fillna(0).astype('Int64')
    
    # Effective from (inject from metadata if not in file)
    if 'effective_from' not in df.columns:
        df['effective_from'] = metadata.get('vintage_date', pd.Timestamp('2025-01-01'))
    elif 'effective_from' in df.columns:
        df['effective_from'] = pd.to_datetime(df['effective_from'], errors='coerce')
        df['effective_from'] = df['effective_from'].fillna(metadata.get('vintage_date'))
    
    return df
```

---

### STEP 4: Run & Verify (5 min)

```bash
# Run golden test
pytest tests/ingestion/test_pprrvu_parser.py::test_pprrvu_golden_fixture -xvs

# Should see:
# ✅ Golden test passed. Parsed 94 rows, 0 rejects

# Run all tests
pytest tests/ingestion/test_pprrvu_parser.py -v

# Expected: 7/7 passing
```

---

### STEP 5: Commit & Push (2 min)

```bash
git add -A
git commit -m "fix(parser): Align PPRRVU layout with schema contract

Root cause: Column name mismatch between layout and schema
- Layout had: work_rvu, pe_rvu_fac, mp_rvu  
- Schema expects: rvu_work, rvu_pe_fac, rvu_malp

Fix:
- Renamed RVU columns in PPRRVU_2025D_LAYOUT to match schema
- Added missing modifier column (natural key!)
- Added missing effective_from (natural key - injected from metadata)
- Simplified _normalize_column_names (layout already canonical)
- Updated _cast_dtypes to use schema column names
- Layout version: v2025.4.0 → v2025.4.1

Tests: 7/7 passing
Total coverage: 41 tests (34 Phase 0 + 7 PPRRVU)

Per STD-parser-contracts v1.2 §21"

git push origin main
```

---

## 🔄 Fallback: CSV-First (If Issues Persist)

**Time:** 10 minutes

See `PPRRVU_HANDOFF.md` section "Fallback Plan" for CSV-first approach.

**When to use:**
- If Step 1 takes > 10 min to find column positions
- If layout updates break other tests
- If you want working parser faster

---

## 📊 Files to Update

1. **`cms_pricing/ingestion/parsers/layout_registry.py`**
   - Rename: `work_rvu` → `rvu_work`, etc.
   - Add: `modifier` column
   - Add or document: `effective_from`, `opps_cap_applicable`
   - Bump: `version: v2025.4.1`

2. **`cms_pricing/ingestion/parsers/pprrvu_parser.py`**
   - Update: `_normalize_column_names()` - skip for fixed-width
   - Update: `_cast_dtypes()` - use schema names
   - Update: `COLUMN_ALIASES` - support both old/new names

3. **`tests/ingestion/test_pprrvu_parser.py`**
   - Update: Assertions to use schema column names (if needed)
   - Add: Regression test for column alignment

---

## 🧪 Validation Checklist

After fix:
-  Import works: `from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu`
-  Layout lookup works: `get_layout("2025", "2025Q4", "pprrvu")` returns dict
-  Schema loading works: Opens `cms_pprrvu_v1.0.json`
-  Golden test passes: 94 rows parsed
-  All 7 tests pass
-  Performance < 2s
-  Natural keys present: hcpcs, modifier, status_code, effective_from
-  Deterministic hashes: Repeat parse = same hash
-  No silent failures: No NaN coercion

---

## 💡 Key Insights

1. **Schema is source of truth** - Layout must match schema, not vice versa
2. **Natural keys are critical** - Missing modifier/effective_from breaks uniqueness
3. **Column naming conventions** - Use schema names everywhere (rvu_work not work_rvu)
4. **Layout versioning** - Bump version when column names change
5. **Normalization is conditional** - Skip for fixed-width (already canonical), apply for CSV

---

## 🎯 Success Criteria

Parser is complete when:
- ✅ All 7 tests passing
- ✅ Natural keys validated: `['hcpcs', 'modifier', 'status_code', 'effective_from']`
- ✅ Column names match schema exactly
- ✅ Deterministic output (hash stability)
- ✅ Performance < 2s for 10K rows
- ✅ No KeyError exceptions
- ✅ Categorical validation working
- ✅ Duplicate detection working (BLOCK severity)

Then:
- Update `STD-parser-contracts v1.2 → v1.3` (30 min)
- Document schema-layout alignment requirement
- Move to next parser (Conversion Factor)

---

## 📚 Reference Files

**Read before starting:**
- `PPRRVU_HANDOFF.md` - Full context
- `PPRRVU_FIX_PLAN.md` - Detailed fix strategy
- `/tmp/PPRRVU_SESSION_SUMMARY.md` - What we accomplished
- `/tmp/PPRRVU_DEBUG_PLAN.md` - Original debug plan

**Schema:**
- `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json`
- Natural keys: `['hcpcs', 'modifier', 'effective_from']`
- Version: 1.1 (precision=2, HALF_UP)

**Layout:**
- `cms_pricing/ingestion/parsers/layout_registry.py`
- Current: PPRRVU_2025D_LAYOUT (v2025.4.0)
- Update to: v2025.4.1

**Parser:**
- `cms_pricing/ingestion/parsers/pprrvu_parser.py` (420 lines)
- Tests: `tests/ingestion/test_pprrvu_parser.py` (7 tests)

---

## 🚀 After PPRRVU Complete

**Phase 1 Remaining:**
1. Conversion Factor parser (2 hours)
2. GPCI parser (1.25 hours)
3. ANES parser (1 hour)
4. OPPSCAP parser (1.25 hours)
5. Locality parser (1 hour)

**Total:** ~7.5 hours

**Documentation:**
- Update STD-parser-contracts v1.3 (30 min)
- Update DOC-test-patterns (45 min)

---

**Ready to execute! Clear path forward, root cause identified, fix is straightforward.** 🎯

