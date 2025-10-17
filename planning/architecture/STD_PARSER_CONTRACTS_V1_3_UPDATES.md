# STD-parser-contracts v1.2 → v1.3 Update Plan

**Purpose:** Document PPRRVU implementation learnings to prevent issues in remaining parsers  
**Time:** 45 minutes  
**Priority:** CRITICAL - Blocker for next 5 parsers  

---

## Sections to Add

### §5.3 Parser Error Taxonomy (NEW)

Add after existing §5.2:

````markdown
### 5.3 Parser Error Taxonomy

All parsers use a common exception hierarchy for consistent error handling:

**Base Exception:**
```python
class ParseError(Exception):
    """Base exception for all parser errors."""
    pass
```

**Specific Exceptions:**

1. **DuplicateKeyError** - Natural key violations
```python
class DuplicateKeyError(ParseError):
    def __init__(self, message: str, duplicates: List[Dict] = None):
        super().__init__(message)
        self.duplicates = duplicates  # List of duplicate key combinations
```

Usage: Raised when `severity=BLOCK` and duplicate natural keys detected.

2. **CategoryValidationError** - Invalid categorical values
```python
class CategoryValidationError(ParseError):
    def __init__(self, field: str, invalid_values: List[Any]):
        self.field = field
        self.invalid_values = invalid_values
```

Usage: Raised when unknown categorical values found before domain casting.

3. **LayoutMismatchError** - Fixed-width parsing failures
```python
class LayoutMismatchError(ParseError):
    pass
```

Usage: Raised when layout doesn't match file structure (wrong widths, missing columns).

4. **SchemaRegressionError** - Unexpected schema fields
```python
class SchemaRegressionError(ParseError):
    def __init__(self, message: str, unexpected_fields: List[str] = None):
        self.unexpected_fields = unexpected_fields
```

Usage: Raised when DataFrame has fields not in schema contract (banned columns).

**When to Raise vs Return in Rejects:**
- `DuplicateKeyError`: Raise if `severity=BLOCK`, return in rejects if `severity=WARN`
- `CategoryValidationError`: Return in rejects (soft failure)
- `LayoutMismatchError`: Always raise (critical parsing failure)
- `SchemaRegressionError`: Always raise (contract violation)

**Location:** `cms_pricing/ingestion/parsers/_parser_kit.py`
````

---

### §6.5 Schema vs API Naming Convention (NEW)

Add after existing §6.4:

````markdown
### 6.5 Schema vs API Naming Convention

**Problem:** CMS datasets have columns that appear in both internal storage and external API responses, requiring different naming for different audiences.

**Solution:** Parsers output **schema format** (DB canonical). API layer transforms to **presentation format**.

**Schema Format (DB Canonical):**
- Pattern: `{component}_{type}` prefix grouping
- Example: `rvu_work`, `rvu_pe_nonfac`, `rvu_pe_fac`, `rvu_malp`
- Used by: Parsers, database tables, schema contracts, ingestors
- Rationale: Logical grouping, clear data ownership

**API Format (Presentation):**
- Pattern: `{type}_{component}` suffix grouping  
- Example: `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`
- Used by: API responses, Pydantic schemas, external docs
- Rationale: More intuitive for API consumers

**Transformation Boundary:**

Transform at **API serialization**, NOT in parser:

```python
# Parser outputs schema format:
result = parse_pprrvu(...)  # DataFrame has rvu_work

# API router transforms for response:
from cms_pricing.mappers import schema_to_api
api_df = schema_to_api(result.data)  # Now has work_rvu
```

**Column Mapper Location:** `cms_pricing/mappers/__init__.py`

**Per-Dataset Mappings:**
- PPRRVU: `rvu_work` ↔ `work_rvu`
- GPCI: (add as needed)
- Others: (add as implemented)

**Benefits:**
- Single source of truth (schema)
- Clean layer separation
- Reversible transformations
- No silent drift
````

---

### §8.5 Natural Key Uniqueness Severity (NEW)

Add after existing §8.4:

````markdown
### 8.5 Natural Key Uniqueness Severity

**Configurable Severity:**

The `check_natural_key_uniqueness()` function supports configurable severity:

```python
def check_natural_key_uniqueness(
    df, 
    natural_keys, 
    severity=ValidationSeverity.WARN,
    schema_id=None,
    release_id=None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Returns (unique_df, duplicates_df)
    # OR raises DuplicateKeyError if severity=BLOCK
```

**Per-Dataset Severity Policies:**

| Dataset | Severity | Rationale |
|---------|----------|-----------|
| PPRRVU | BLOCK | Critical reference data, duplicates indicate data quality issue |
| GPCI | WARN | May have intentional duplicates across effective dates |
| Locality | WARN | County-locality mappings may overlap |
| Conversion Factor | BLOCK | Single value per type/date, duplicates are errors |
| ANES CF | BLOCK | Single value per locality/date |
| OPPSCAP | WARN | May have multiple modifiers for same HCPCS |

**Configuration:**

Set in parser constant:

```python
# In pprrvu_parser.py:
UNIQUENESS_SEVERITY = ValidationSeverity.BLOCK

# In parser:
unique_df, dupes_df = check_natural_key_uniqueness(
    df, 
    natural_keys=NATURAL_KEYS,
    severity=UNIQUENESS_SEVERITY,  # BLOCK for PPRRVU
    ...
)
```

**When BLOCK:**
- Raises `DuplicateKeyError` immediately
- Stops processing (no partial data)
- Error contains list of duplicate key combinations

**When WARN:**
- Returns duplicates in rejects DataFrame
- Continues processing with unique rows
- Duplicates quarantined for review
````

---

### §14.3 Schema File Naming & Loading (NEW)

Add after existing §14.2:

````markdown
### 14.3 Schema File Naming & Loading

**Filename Convention:**
- Format: `cms_{dataset}_v{major}.0.json`
- Example: `cms_pprrvu_v1.0.json`

**Internal Version:**
- Schema JSON contains: `"version": "{major}.{minor}"`
- Example: `"version": "1.1"`

**Version Mismatch Pattern:**
- Metadata uses: `schema_id: "cms_pprrvu_v1.1"`
- File is named: `cms_pprrvu_v1.0.json`
- **Parser must strip minor version to find file**

**Loading Logic:**

```python
# In parser:
schema_id = metadata.get('schema_id', 'cms_pprrvu_v1.1')

# Strip minor version: cms_pprrvu_v1.1 → cms_pprrvu_v1.0
schema_base = schema_id.rsplit('.', 1)[0]  # Remove .1
schema_file = Path(__file__).parent.parent / "contracts" / f"{schema_base}.0.json"

with open(schema_file) as f:
    schema = json.load(f)
```

**Rationale:**
- Major version = breaking changes (new file)
- Minor version = additive changes (update existing file)
- Filename stability for imports

**Example:**
- v1.0 → v1.1: Add precision field (update cms_pprrvu_v1.0.json)
- v1.1 → v2.0: Remove vintage_year (create cms_pprrvu_v2.0.json)
````

---

### §15.5 Layout-Schema Alignment (CRITICAL - NEW)

Add after existing §15.4:

````markdown
### 15.5 Layout-Schema Alignment

**CRITICAL REQUIREMENT:** Layout column names MUST exactly match schema contract column names.

**Why:**
- Parser uses layout to create DataFrame columns
- Schema validation expects specific column names
- Mismatch causes `KeyError` in categorical validation
- Missing natural keys break uniqueness checks

**Alignment Checklist:**

Before implementing parser:
1. ✅ Load schema contract: `cms_{dataset}_v1.0.json`
2. ✅ List required columns from schema
3. ✅ Verify layout has ALL required columns
4. ✅ Verify column names EXACTLY match (case-sensitive)
5. ✅ Verify natural key columns present

**Common Misalignments:**

| Schema Has | Layout Had | Fix |
|------------|------------|-----|
| `rvu_work` | `work_rvu` | Rename layout column |
| `modifier` | ❌ MISSING | Add to layout at correct position |
| `effective_from` | ❌ MISSING | Add or inject from metadata |
| `rvu_malp` | `mp_rvu` | Rename layout column |

**Example: PPRRVU Layout Update**

Before (v2025.4.0 - WRONG):
```python
'columns': {
    'work_rvu': {'start': 61, 'end': 65},  # ❌ Wrong name
    # modifier missing!  # ❌ Missing natural key
}
```

After (v2025.4.1 - CORRECT):
```python
'columns': {
    'rvu_work': {'start': 61, 'end': 65},  # ✅ Matches schema
    'modifier': {'start': 5, 'end': 7},  # ✅ Added natural key
}
```

**Validation:**

After creating layout, verify alignment:

```python
# Load both
layout = get_layout(year, quarter, dataset)
schema = json.load(open(f'cms_{dataset}_v1.0.json'))

# Check all schema columns in layout
schema_cols = set(schema['columns'].keys())
layout_cols = set(layout['columns'].keys())

missing = schema_cols - layout_cols
if missing:
    raise LayoutMismatchError(f"Layout missing schema columns: {missing}")

# Check names match exactly
for col in schema_cols:
    if col not in layout_cols:
        print(f"❌ Schema has '{col}' but layout doesn't")
```

**Bump layout version when:**
- Column names change
- Columns added/removed
- Column positions change

````

---

### §21.2 Common Pitfalls (NEW)

Add before existing §21 Implementation Template:

````markdown
## 21. Common Pitfalls & Solutions

### Pitfall 1: min_line_length Too Strict

**Problem:**
```python
LAYOUT = {
    'min_line_length': 200,  # Assumption, not measurement
}
```

All data rows are 173 characters → All rows filtered out → Empty DataFrame → KeyError.

**Solution:**
Measure actual data first:
```bash
head -20 sample.txt | tail -10 | awk '{print length}'
# Output: 173, 173, 173, ...
```

Set min_line_length conservatively:
```python
'min_line_length': 165,  # Allows for 173-char lines with margin
```

---

### Pitfall 2: Layout-Schema Column Name Mismatch

**Problem:**
```python
# Layout has:
'work_rvu': {'start': 61, 'end': 65}

# Schema expects:
"rvu_work": {"type": "float64"}

# Result: KeyError: 'rvu_work'
```

**Solution:**
Align layout with schema BEFORE implementing parser:
1. Load schema contract
2. List required columns
3. Update layout column names to match exactly
4. Bump layout version

---

### Pitfall 3: Missing Natural Key Columns

**Problem:**
```python
# Schema natural_keys:
["hcpcs", "modifier", "effective_from"]

# Layout has:
'hcpcs': {...}  # ✓
# modifier missing!  # ❌
# effective_from missing!  # ❌
```

Uniqueness check fails because natural key columns don't exist.

**Solution:**
- Add all natural key columns to layout
- OR inject from metadata in parser (effective_from often from vintage_date)

---

### Pitfall 4: Schema File Version Stripping

**Problem:**
```python
# Metadata has:
schema_id = "cms_pprrvu_v1.1"

# Try to open:
f'cms_pprrvu_v1.1.json'  # ❌ FileNotFoundError

# Actual file:
'cms_pprrvu_v1.0.json'  # Contains version 1.1 internally
```

**Solution:**
Strip minor version before loading:
```python
schema_base = schema_id.rsplit('.', 1)[0]  # cms_pprrvu_v1.1 → cms_pprrvu_v1
schema_file = f"{schema_base}.0.json"  # cms_pprrvu_v1.0.json
```

---

### Pitfall 5: Wrong Parameter Names

**Problem:**
```python
canonicalize_numeric_col(series, precision=2, rounding='HALF_UP')  # ❌
# TypeError: got unexpected keyword argument 'rounding'
```

**Solution:**
Check kit function signature:
```python
canonicalize_numeric_col(series, precision=2, rounding_mode='HALF_UP')  # ✅
```

---

### Pitfall 6: API Names in Parser

**Problem:**
```python
# Parser outputs:
rvu_cols = ['work_rvu', 'pe_rvu_fac']  # ❌ API format

# Schema expects:
['rvu_work', 'rvu_pe_fac']  # Schema format
```

**Solution:**
Always use schema format in parser. Transform at API layer:
```python
# Parser:
rvu_cols = ['rvu_work', 'rvu_pe_fac']  # ✅ Schema format

# API router:
from cms_pricing.mappers import schema_to_api
response_df = schema_to_api(db_df)  # rvu_work → work_rvu
```

````

---

## Updates to Existing Sections

### §15.4 Layout Registry API (ENHANCE)

Add API documentation:

````markdown
**Function Signature:**
```python
def get_layout(
    product_year: str,
    quarter_vintage: str,
    dataset: str
) -> Optional[Dict[str, Any]]
```

**Example Usage:**
```python
layout = get_layout("2025", "2025Q4", "pprrvu")
# Returns: {'version': 'v2025.4.1', 'columns': {...}, 'min_line_length': 165}
```

**Lookup Logic:**
1. Extract quarter: "2025Q4" → "Q4"
2. Try specific: `(dataset, year, quarter)` = `("pprrvu", "2025", "Q4")`
3. Fallback annual: `("pprrvu", "2025", None)`
4. Return None if not found

**Column Names:**
Layout column names MUST match schema contract exactly.
See §15.5 for alignment requirements.
````

---

### §21.1 Implementation Template (ENHANCE)

Add validation guard to Step 5:

````markdown
**Step 5:** Load schema & validate alignment

```python
# Load schema
schema_file = Path(__file__).parent.parent / "contracts" / f"{schema_base}.0.json"
with open(schema_file) as f:
    schema = json.load(f)

# Validation guard: Check all required columns present
required_cols = set(schema['columns'].keys())
actual_cols = set(df.columns)
missing = required_cols - actual_cols

if missing:
    raise SchemaRegressionError(
        f"DataFrame missing required schema columns: {missing}",
        unexpected_fields=list(missing)
    )
```

**Why:** Catches layout-schema misalignment immediately after normalization.
````

---

## Version & Changelog

Update header:
```markdown
**Status:** Draft v1.3
```

Add to changelog at end:

````markdown
## Changelog

### v1.3 (2025-10-16)

**Added:**
- §5.3 Parser Error Taxonomy - Custom exceptions for clearer error handling
- §6.5 Schema vs API Naming Convention - DB canonical vs presentation format
- §8.5 Natural Key Uniqueness Severity - Per-dataset BLOCK/WARN policies
- §14.3 Schema File Naming & Loading - Version stripping logic
- §15.5 Layout-Schema Alignment - Critical requirement and checklist
- §21.2 Common Pitfalls - 6 common issues with solutions

**Enhanced:**
- §15.4 Layout Registry API - Added signature, lookup logic, examples
- §21.1 Implementation Template - Added validation guard

**Motivation:** PPRRVU parser implementation revealed critical gaps causing 2+ hours debugging. These additions prevent same issues in remaining 5 parsers.

**Impact:** Prevents schema-layout mismatches, establishes column transformation pattern, documents error handling best practices.
````

---

**Total Additions:** ~300 lines  
**Files to Update:** 1 (STD-parser-contracts-prd-v1.0.md)  
**Cross-References:** Update DOC-master-catalog after v1.3 publish  

---

**Next Steps:**
1. Apply these additions to STD-parser-contracts-prd-v1.0.md
2. Update version to v1.3
3. Commit with message referencing PPRRVU learnings
4. Update cross-references in other PRDs

