# GPCI Missing Data - Pattern Mismatch Analysis

## 🔍 **Root Cause Analysis**

### **Configuration Check Results**

✅ **DatasetSpec Configuration** (`cms_pricing/ingestion/datasets/rvu_spec.py`):
- **Key**: `"gpci"` (line 146)
- **Pattern**: `r".*gpci.*\.(txt|csv|xlsx|xls)$"` (line 155)
- **Status**: ✅ Case-insensitive pattern (should match `GPCI2025.txt`)

✅ **Loader Dispatch** (`cms_pricing/ingestion/datasets/rvu_loaders.py`):
- **Key**: `"gpci"` (line in RVU_DATASET_LOADERS dict)
- **Function**: `load_gpci_data`
- **Status**: ✅ Matches DatasetSpec key

✅ **RVUIngestor Patterns** (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`):
- **Pattern**: `r".*gpci.*\.(txt|csv|xlsx|xls)$"` (line 120)
- **Status**: ✅ Case-insensitive pattern

⚠️ **Parser Routing** (`cms_pricing/ingestion/parsers/__init__.py`):
- **Pattern**: `r"GPCI.*\.(txt|csv|xlsx)$"` (uppercase-only!)
- **Status**: ⚠️ **POTENTIAL ISSUE** - Only matches uppercase `GPCI`, not lowercase

## 🐛 **The Problem**

### **Likely Root Cause: File Discovery/Routing Issue**

The patterns themselves look correct (case-insensitive), but there are **two different routing systems**:

1. **RVUIngestor.EXPECTED_PATTERNS** - Used for file discovery in the ingestor
2. **Parser Routing (PARSER_ROUTING)** - Used for parser dispatch (uppercase-only!)
3. **DatasetSpec.route_file()** - Used for dataset routing

### **Potential Issues:**

1. **Case Sensitivity Mismatch**: 
   - Parser routing pattern is `r"GPCI.*"` (uppercase only)
   - But RVUIngestor pattern is `r".*gpci.*"` (case-insensitive)
   - CMS files are typically `GPCI2025.txt` (uppercase)

2. **File Discovery Not Using DatasetSpec Patterns**:
   - The scraper might be using `RVUIngestor.EXPECTED_PATTERNS` 
   - But dataset routing uses `DatasetSpec.route_file()`
   - These might not be synchronized

3. **Missing GPCI Files in Release**:
   - The source version `3cb855d0-0` might not include GPCI files
   - Need to check if CMS actually published GPCI for this release

## 📊 **Next Steps to Diagnose**

1. **Check if GPCI files exist in scraped directory**
2. **Check manifest for GPCI file mentions**
3. **Verify file routing logs during ingestion**
4. **Check if DatasetSpec.route_file() is being called correctly**

## 🔧 **Recommended Fixes**

### **Fix 1: Synchronize Patterns (If Needed)**

If parser routing pattern is the issue, update `cms_pricing/ingestion/parsers/__init__.py`:

```python
# Current (uppercase only):
r"GPCI.*\.(txt|csv|xlsx)$"

# Should be (case-insensitive):
r".*[Gg][Pp][Cc][Ii].*\.(txt|csv|xlsx)$"
# OR better:
r"(?i).*gpci.*\.(txt|csv|xlsx)$"
```

### **Fix 2: Ensure DatasetSpec Routing is Used**

Verify that `DatasetSpec.route_file()` is called during file discovery, not just `RVUIngestor.EXPECTED_PATTERNS`.

### **Fix 3: Add GPCI to Fallback Discovery**

Ensure the catchall pattern includes GPCI and is used as fallback.
