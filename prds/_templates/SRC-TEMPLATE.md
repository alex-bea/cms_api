# {Dataset Name} Data Source

**Status:** Draft v{X}.{Y}  
**Owners:** Data Engineering  
**Consumers:** {List consumers - e.g., MPFS Ingester, Pricing API, QA}  
**Change control:** PR review  
**Review cadence:** Quarterly (or as CMS releases change)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **PRD-{related}-prd-v1.0.md:** Related product PRDs
- **STD-parser-contracts-prd-v1.0.md:** Parser implementation standards
- **STD-qa-testing-prd-v1.0.md:** QA testing standards

**Last Updated:** {YYYY-MM-DD}  
**Verified Against CMS Release:** {Release ID, e.g., RVU25D}

---

## 1. Overview

**Official CMS Name:** {Full official name}  
**Dataset Code:** {Short code, e.g., GPCI, PPRRVU, OPPS}  
**Source URL:** [CMS Link]({URL})  
**Release Cadence:** {Quarterly / Annual / Ad-hoc}  
**Business Purpose:** {What this data is used for in 1-2 sentences}

**Typical Characteristics:**
- **File Size:** {e.g., < 1MB, ~10MB, etc.}
- **Row Count:** {e.g., 100-120 rows, 10K-20K rows}
- **Update Frequency:** {e.g., Quarterly with letters A/B/C/D}
- **Effective Dates:** {When data goes into effect}

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Notes |
|--------|-----------|--------------|----------------|-------|
| **TXT** | `.txt` | ✅ Always | ✅ Implemented | Fixed-width, requires layout |
| **CSV** | `.csv` | ✅ Always | ✅ Implemented | Header variations |
| **XLSX** | `.xlsx` | ⚠️ Sometimes | ✅ Implemented | Excel workbook |
| **ZIP** | `.zip` | ✅ Always | ✅ Implemented | Archives above formats |

### 2.2 Format-Specific Details

**TXT (Fixed-Width):**
- **Line Length:** {Measured chars} (e.g., 173 chars)
- **Header Rows:** {Number} rows (e.g., 3 rows to skip)
- **Data Start Pattern:** {Regex} (e.g., `^\d{5}` for HCPCS codes)
- **Layout Version:** `{dataset}_{year}_{quarter}` (e.g., `gpci_2025_q4`)
- **Encoding:** UTF-8 (CP1252 fallback for older files)

**CSV:**
- **Delimiter:** {Comma / Tab} 
- **Header Structure:** {e.g., Row 1 = title, Row 2 = column names}
- **Skip Rows:** `skiprows={N}` in pandas
- **Quoting:** QUOTE_MINIMAL (or as needed)

**XLSX:**
- **Sheet Name:** {e.g., "Data", "Sheet1", or specific name}
- **Header Row:** Row {N}
- **Data Type:** {Sample vs Full dataset}
- **Known Issues:** {e.g., Excel date coercion, float precision}

**ZIP:**
- **Inner Files:** {List expected filenames}
- **Format Detection:** {Extension-based / Content-based}

---

## 3. Schema & Column Mapping

### 3.1 Natural Keys

```python
NATURAL_KEYS = ['{key1}', '{key2}', ...]  # e.g., ['locality_code', 'effective_from']
```

**Uniqueness:** {Expected behavior - always unique / duplicates allowed / known quirks}

### 3.2 Schema Contract

**Location:** `cms_pricing/ingestion/contracts/cms_{dataset}_v{X}.0.json`

**Core Columns:**
| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `{col1}` | {type} | {Y/N} | {rules} | {notes} |
| `{col2}` | {type} | {Y/N} | {rules} | {notes} |

### 3.3 Column Header Aliases

```python
# Map CMS header variations to schema canonical names
ALIAS_MAP = {
    # TXT format
    '{cms_header_txt}': '{schema_col}',
    
    # CSV format
    '{cms_header_csv}': '{schema_col}',
    
    # XLSX format  
    '{cms_header_xlsx}': '{schema_col}',
    
    # Historical variations
    '{old_header}': '{schema_col}',
}
```

---

## 4. Business Rules & Validations

### 4.1 Value Ranges

| Column | Min | Max | Typical | Notes |
|--------|-----|-----|---------|-------|
| `{col}` | {min} | {max} | {range} | {notes} |

### 4.2 Derived Fields

{List any fields calculated by parser, not in CMS source}

```python
# Example
df['effective_from'] = metadata['vintage_date']
df['{derived_col}'] = {calculation}
```

### 4.3 Floor Values & Exceptions

{Document any statutory floors, caps, or business rule adjustments}

**Example:**
- **GPCI Work Floor:** Statutory floor of 1.0 for work component

---

## 5. Known Data Quality Issues

### 5.1 Duplicate Keys

**Issue:** {Description}  
**Frequency:** {Always / Sometimes / Rare / Release-specific}  
**Affected Releases:** {Which quarters/years}  
**Parser Handling:** {BLOCK / WARN / Quarantine with details}  
**Test Coverage:** {Test name that validates behavior}

### 5.2 Missing Values

| Column | Expected Nulls | Reason | Handling |
|--------|----------------|--------|----------|
| `{col}` | {%} | {reason} | {BLOCK / ALLOW} |

### 5.3 Outliers & Anomalies

{Document known valid outliers that should NOT be quarantined}

---

## 6. CMS-Specific Context

### 6.1 MAC vs Locality Codes

{If applicable, explain relationship between MAC and Locality codes for this dataset}

### 6.2 Quarter-to-Date Mapping

**CMS Release Naming:**
- A → Q1 (Effective: {date})
- B → Q2 (Effective: {date})
- C → Q3 (Effective: {date})
- D → Q4 / Annual (Effective: {date})

---

## 7. Testing Strategy

### 7.1 Golden Fixtures

| Format | Fixture File | Rows | Rejects | Purpose |
|--------|--------------|------|---------|---------|
| TXT | `{file}` | {N} | 0 | Clean happy path |
| CSV | `{file}` | {N} | 0 | Format parity |
| XLSX | `{file}` | {N} | {N} | {purpose} |
| ZIP | `{file}` | {N} | 0 | Format parity |

**Location:** `tests/fixtures/{dataset}/golden/`

### 7.2 Edge Case Fixtures

| Fixture | Issue Tested | Expected Outcome |
|---------|--------------|------------------|
| `{file}` | {issue} | {outcome} |

**Location:** `tests/fixtures/{dataset}/edge_cases/`

### 7.3 Negative Fixtures

{List fixtures for invalid/malformed input testing}

**Location:** `tests/fixtures/{dataset}/negative/`

---

## 8. Historical Format Changes

| Release | Change | Impact | Migration |
|---------|--------|--------|-----------|
| {release} | {change} | {impact} | {what was done} |

---

## 9. Implementation References

**Parser:** `cms_pricing/ingestion/parsers/{dataset}_parser.py`  
**Schema:** `cms_pricing/ingestion/contracts/cms_{dataset}_v{X}.0.json`  
**Layout:** `cms_pricing/ingestion/parsers/layout_registry.py` (if fixed-width)  
**Tests:** `tests/ingestion/test_{dataset}_parser_golden.py`  
**Fixtures:** `tests/fixtures/{dataset}/`

**Related PRDs:**
- `PRD-{dataset}-prd-v1.0.md` - Product requirements
- `STD-parser-contracts-prd-v1.0.md` - Parser standards
- `STD-qa-testing-prd-v1.0.md` - Testing standards

---

## 10. Maintenance

**Next CMS Release Expected:** {Date or Quarter}  
**Last Verified:** {YYYY-MM-DD}  
**Verification Checklist:**
- [ ] File formats unchanged
- [ ] Column headers stable
- [ ] Value ranges within expected bounds
- [ ] No new duplicate key scenarios
- [ ] Layout still accurate (if fixed-width)

**Known Issues:**
- {List any ongoing issues or limitations}

---

*SRC Document Template Version: 1.0 (2025-10-17)*  
*Based on: GPCI parser implementation experience*

