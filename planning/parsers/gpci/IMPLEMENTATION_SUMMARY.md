# GPCI Parser - High-Level Implementation Summary

**Date:** 2025-10-17  
**Full Plan:** `IMPLEMENTATION.md` (27K, v2.1)  
**Standards:** STD-parser-contracts v1.7 §21.1 (11-step template)

---

## 🎯 **TL;DR - What We're Building**

**A pure function parser** that converts CMS GPCI files (TXT/CSV/XLSX/ZIP) into validated, deterministic pandas DataFrames.

```python
def parse_gpci(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Pure function: No file writes, no global state.
    Returns: ParseResult(data, rejects, metrics)
    """
```

**Time:** 2-3 hours (full implementation + tests)

---

## 📊 **High-Level Architecture**

```
┌─────────────────────────────────────────────────────────────┐
│                    CMS RVU Bundle (ZIP)                      │
│  ┌────────────┬────────────┬─────────────┬──────────────┐   │
│  │ PPRRVU.txt │ GPCI2025.txt│ ANES2025.txt│ OPPSCAP.txt │   │
│  └────────────┴────────────┴─────────────┴──────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
         ┌──────────────────────────────────────┐
         │  Parser Router (content sniffing)     │
         │  cms_pricing/ingestion/parsers/       │
         │  __init__.py::route_to_parser()      │
         └──────────────────────────────────────┘
                            │
                            ▼
         ┌──────────────────────────────────────┐
         │      GPCI Parser (THIS MODULE)        │
         │  cms_pricing/ingestion/parsers/       │
         │  gpci_parser.py::parse_gpci()        │
         │                                       │
         │  Follows STD-parser-contracts v1.7   │
         │  §21.1 (11-step template)            │
         └──────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
                │                       │
                ▼                       ▼
    ┌─────────────────────┐   ┌──────────────────┐
    │  ParseResult.data    │   │ ParseResult.     │
    │  (valid rows)        │   │ rejects          │
    │                      │   │ (quarantine)     │
    │  • 100-120 rows      │   │                  │
    │  • Hash computed     │   │ • Error codes    │
    │  • Sorted by key     │   │ • Context        │
    └─────────────────────┘   └──────────────────┘
                │
                ▼
    ┌─────────────────────────────────┐
    │  Ingestor (DIS Normalize Stage)  │
    │  Writes artifacts:                │
    │  • parsed.parquet                 │
    │  • rejects.parquet (if any)       │
    │  • metrics.json                   │
    │  • provenance.json                │
    └─────────────────────────────────┘
                │
                ▼
    ┌─────────────────────────────────┐
    │  Database / Data Warehouse       │
    │  • cms_gpci table                 │
    │  • Natural keys enforced          │
    │  • Joins with PPRRVU for pricing  │
    └─────────────────────────────────┘
```

---

## 📋 **11-Step Parser Structure** (STD-parser-contracts §21.1)

**From:** `IMPLEMENTATION.md` lines 200-500

### **Phase 1: Input Processing** (Steps 1-4)

| Step | Action | Uses | Output |
|------|--------|------|--------|
| **1** | Encoding detection | `_parser_kit.detect_encoding()` | encoding, BOM-stripped content |
| **2** | Format routing | Layout check / file extension | DataFrame (raw) |
| **3** | Column normalization | `_normalize_column_names()` | DataFrame (canonical names) |
| **4** | String cleanup | `normalize_string_columns()` | DataFrame (whitespace stripped) |

**Format-Specific Parsing:**
- **Fixed-width TXT:** `get_layout()` → `pd.read_fwf()` with colspecs
- **CSV:** `pd.read_csv()` with dialect detection
- **XLSX:** `pd.read_excel(dtype=str)` → avoid Excel coercion
- **ZIP:** Extract → recurse → concat

### **Phase 2: Validation** (Steps 5-7)

| Step | Action | Uses | Output |
|------|--------|------|--------|
| **5** | Type casting | `canonicalize_numeric_col()` (3dp for GPCI) | DataFrame (typed) |
| **6** | Categorical validation | `enforce_categorical_dtypes()` | valid_df + rejects_df |
| **7** | Row count validation | Custom validator | Warning if outside 100-120 |

**Validation Tiers:**
- **BLOCK:** < 90 rows (fail), invalid locality format
- **WARN:** 90-100 or 120+ rows, GPCI out of [0.20, 2.50]
- **INFO:** 100-120 rows (expected)

### **Phase 3: Finalization** (Steps 8-11)

| Step | Action | Uses | Output |
|------|--------|------|--------|
| **8** | Metadata injection | metadata dict → DataFrame columns | DataFrame (with provenance) |
| **9** | Hash & sort | `finalize_parser_output()` | DataFrame (deterministic) |
| **10** | Metrics collection | Aggregate all phases | metrics dict |
| **11** | Return result | `ParseResult(data, rejects, metrics)` | Structured output |

---

## 🔗 **Integration Points**

### **1. Parser Router** (`parsers/__init__.py`)

**How parser is discovered:**
```python
from cms_pricing.ingestion.parsers import route_to_parser

# Router uses filename pattern + content sniffing
dataset, schema_id, parser_func = route_to_parser(
    filename="GPCI2025.txt",
    file_head=first_8kb  # For magic bytes
)

# Returns: ("gpci", "cms_gpci_v1.2", parse_gpci)
```

**Router pattern:**
```python
PARSER_ROUTING = {
    r"GPCI.*\.(txt|csv|xlsx)$": ("gpci", "cms_gpci_v1.2", parse_gpci),
    # ...
}
```

### **2. Layout Registry** (`parsers/layout_registry.py`)

**How layout is loaded:**
```python
from cms_pricing.ingestion.parsers.layout_registry import get_layout

layout = get_layout(
    product_year="2025",
    quarter_vintage="D",  # Or "2025Q4"
    dataset="gpci"
)

# Returns: GPCI_2025D_LAYOUT (v2025.4.1)
# - Column positions (start, end)
# - min_line_length: 100
# - data_start_pattern: r'^\d{5}'
```

**Layout structure:**
```python
GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',
    'columns': {
        'locality_code': {'start': 24, 'end': 26, ...},
        'gpci_work': {'start': 121, 'end': 126, ...},
        'gpci_pe': {'start': 133, 'end': 138, ...},
        'gpci_mp': {'start': 145, 'end': 150, ...},
    }
}
```

### **3. Schema Contract** (`contracts/cms_gpci_v1.2.json`)

**How schema is loaded:**
```python
from cms_pricing.ingestion.contracts.schema_registry import load_schema

schema = load_schema("cms_gpci_v1.2")

# Returns:
# - columns: {name: {type, nullable, constraints}}
# - natural_keys: ['locality_code', 'effective_from']
# - column_order: [...] (for deterministic hashing)
# - quality_thresholds: {min_rows: 90, expected_rows: [100, 120]}
```

### **4. Parser Kit** (`parsers/_parser_kit.py`)

**Shared utilities used:**
- `detect_encoding()` - BOM + encoding cascade
- `canonicalize_numeric_col()` - Decimal with 3dp precision
- `enforce_categorical_dtypes()` - Domain validation
- `finalize_parser_output()` - Hash + sort by natural keys
- `normalize_string_columns()` - Strip whitespace/NBSP

### **5. DIS Ingestor** (calls parser)

**Integration with normalize stage:**
```python
class MPFSIngestor(BaseDISIngestor):
    async def normalize_stage(self, raw_batch: RawBatch):
        metadata = {
            'release_id': self.current_release_id,
            'schema_id': 'cms_gpci_v1.2',
            'product_year': '2025',
            'quarter_vintage': '2025Q4',
            'vintage_date': datetime.now(),
            'source_release': 'RVU25D',
            # ...
        }
        
        # Call parser (returns ParseResult)
        result = parse_gpci(file_obj, filename, metadata)
        
        # Ingestor writes artifacts
        self._write_parquet(result.data, "gpci/parsed.parquet")
        if len(result.rejects) > 0:
            self._write_parquet(result.rejects, "gpci/rejects.parquet")
        self._write_json(result.metrics, "gpci/metrics.json")
```

---

## 🧱 **Data Flow Example**

### **Input: GPCI2025.txt (Fixed-Width)**

```
ADDENDUM E. FINAL CY 2025 GEOGRAPHIC PRACTICE COST INDICES...
Medicare Admi State  Locality...
10112           AL      00   ALABAMA                              1.000       0.869       0.575
```

### **Step-by-Step Transformation:**

1. **Encoding Detection:** UTF-8 detected, no BOM
2. **Format Detection:** Fixed-width (via layout pattern)
3. **Layout Load:** `GPCI_2025D_LAYOUT` v2025.4.1
4. **Fixed-Width Parse:** `pd.read_fwf()` with colspecs
   ```python
   colspecs = [(24, 26), (121, 126), (133, 138), (145, 150)]
   names = ['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp']
   ```
5. **Type Cast:** Strings → Decimal (3dp)
   ```python
   df['gpci_work'] = canonicalize_numeric_col(df['gpci_work'], precision=3)
   # Result: "1.000" (string for hashing)
   ```
6. **Validation:** 115 rows (✓ in range 100-120)
7. **Metadata Injection:**
   ```python
   df['release_id'] = 'mpfs_2025_q4_20251015'
   df['source_release'] = 'RVU25D'
   df['parsed_at'] = '2025-10-17T10:30:00Z'
   ```
8. **Hash & Sort:**
   ```python
   df = finalize_parser_output(df, ['locality_code', 'effective_from'], schema)
   # Adds: row_content_hash (64-char SHA-256)
   # Sorts by: locality_code, effective_from
   ```

### **Output: ParseResult**

```python
ParseResult(
    data=<DataFrame: 115 rows × 12 columns>,
    rejects=<DataFrame: 0 rows>,
    metrics={
        'total_rows': 115,
        'valid_rows': 115,
        'reject_rows': 0,
        'parse_duration_sec': 0.42,
        'encoding_detected': 'utf-8',
        'schema_id': 'cms_gpci_v1.2',
        'parser_version': 'v1.0.0'
    }
)
```

---

## 📝 **Schema Contract (cms_gpci_v1.2)**

### **Core Columns** (in hash)
- `locality_code` (string, 2-digit)
- `gpci_work` (decimal, 3dp)
- `gpci_pe` (decimal, 3dp)
- `gpci_mp` (decimal, 3dp)
- `effective_from` (date)
- `effective_to` (date, nullable)

### **Enrichment Columns** (excluded from hash)
- `mac` (string, 5-digit)
- `state` (string, 2-letter)
- `locality_name` (string)

### **Provenance Columns** (excluded from hash)
- `source_release` (string, e.g., "RVU25D")
- `source_inner_file` (string)
- `source_file_sha256` (string)
- `release_id` (string)
- `parsed_at` (timestamp)
- `row_content_hash` (string, 64-char SHA-256)

---

## 🧪 **Testing Strategy**

### **Golden Tests** (`tests/ingestion/test_gpci_parser_golden.py`)
- Parse known fixture → verify hash unchanged
- Test all formats: TXT, CSV, XLSX, ZIP
- Test determinism: same input → same hash

### **Negative Tests** (`tests/ingestion/test_gpci_parser_negatives.py`)
- Empty file
- Malformed layout
- Invalid GPCI values
- Duplicate localities
- Missing columns
- Encoding errors

### **Integration Test** (`tests/integration/test_gpci_payment_spotcheck.py`)
- Join GPCI + PPRRVU + Conversion Factor
- Calculate payment for CPT 99213 in 2 localities
- Verify against CMS PFS Lookup Tool fixtures

---

## ⏱️ **Implementation Time Breakdown**

| Phase | Activity | Time | Cumulative |
|-------|----------|------|------------|
| **1** | Create parser file (template) | 10 min | 10 min |
| **2** | Implement 11-step structure | 50 min | 1h |
| **3** | Write golden tests | 15 min | 1h 15m |
| **4** | Write negative tests | 20 min | 1h 35m |
| **5** | Integration test (payment) | 10 min | 1h 45m |
| **6** | Documentation & fixtures | 15 min | 2h |

**Total:** 2 hours (focused implementation)

**If including pre-implementation:**
- Pre-implementation: 30 min (already complete ✓)
- Implementation: 2 hours
- **Grand Total:** 2.5 hours

---

## ✅ **Readiness Checklist**

**Pre-Implementation:** (✅ All Complete)
- [x] Layout registry updated (v2025.4.1)
- [x] Column positions verified
- [x] Line lengths measured (150 chars)
- [x] Schema contract exists (cms_gpci_v1.2.json)
- [x] Sample data verified (DATA_PROVENANCE.md)

**Ready to Code:**
- [x] Environment tested (Step 4 passed)
- [x] Parser kit utilities available
- [x] Reference parsers reviewed (PPRRVU, CF)
- [x] Standards document read (STD-parser-contracts v1.7)
- [x] Implementation plan complete (IMPLEMENTATION.md 27K)

---

## 🎯 **Next Step**

**You are here:** Pre-implementation complete, ready to code! 🚀

**Start coding:**
```bash
# Activate environment
source .venv_gpci/bin/activate

# Create parser file
touch cms_pricing/ingestion/parsers/gpci_parser.py

# Follow: planning/parsers/gpci/IMPLEMENTATION.md
# Reference: STD-parser-contracts v1.7 §21.1
```

**Estimated time to working parser:** 2 hours ⏱️

---

## 📚 **Key References**

| Document | Purpose | Location |
|----------|---------|----------|
| **STD-parser-contracts v1.7** | Parser standards (§21.1 = 11-step template) | `prds/STD-parser-contracts-prd-v1.0.md` |
| **IMPLEMENTATION.md** | Full implementation guide (27K, v2.1) | `planning/parsers/gpci/IMPLEMENTATION.md` |
| **cms_gpci_v1.2.json** | Schema contract | `cms_pricing/ingestion/contracts/` |
| **layout_registry.py** | GPCI_2025D_LAYOUT v2025.4.1 | `cms_pricing/ingestion/parsers/` |
| **_parser_kit.py** | Shared utilities | `cms_pricing/ingestion/parsers/` |
| **conversion_factor_parser.py** | Reference (CSV/XLSX/ZIP) | `cms_pricing/ingestion/parsers/` |
| **pprrvu_parser.py** | Reference (fixed-width) | `cms_pricing/ingestion/parsers/` |

---

**Everything is ready! Time to code the parser!** 🎉

