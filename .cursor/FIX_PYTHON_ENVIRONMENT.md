# Python Environment Fix Guide

**Issue:** Segmentation fault when importing pandas/sqlalchemy/structlog  
**Impact:** Blocks automated testing and backfill script execution  
**Priority:** CRITICAL (must fix before production deployment)

---

## Diagnosis

**Current Environment:**
```
Python:      3.12.7 (Anaconda /opt/anaconda3)
pip:         24.2
pandas:      2.2.2
structlog:   23.3.0
numpy:       1.26.4
sqlalchemy:  (missing or not shown)
```

**Symptoms:**
- Exit code 139 (segmentation fault) when importing pandas
- Python audit tools crash immediately
- pytest crashes before running tests
- Backfill script cannot run

**Root Cause:** Likely Anaconda environment corruption or conflicting binary dependencies

---

## Solution Options (Choose One)

### **Option A: Quick Fix - Reinstall Critical Packages** ⚡ (5-10 minutes)

**Best for:** If you want to keep using Anaconda

```bash
# Step 1: Force reinstall pandas and dependencies
pip install --force-reinstall --no-cache-dir \
    pandas==2.1.0 \
    numpy==1.24.3 \
    sqlalchemy==2.0.20 \
    structlog==23.1.0

# Step 2: Test import
python -c "import pandas, sqlalchemy, structlog; print('✅ All imports successful')"

# Step 3: Run a quick test
pytest tests/ingestion/test_gpci_parser_golden.py::test_gpci_golden_txt -v
```

**Why this might work:**
- Forces clean binary rebuild
- Uses known-good version combinations
- Clears pip cache to avoid corrupted downloads

---

### **Option B: Create Fresh Virtual Environment** 🔄 (15-20 minutes)

**Best for:** Clean slate, isolated from Anaconda

```bash
# Step 1: Create new venv using system Python
python3 -m venv .venv

# Step 2: Activate
source .venv/bin/activate

# Step 3: Upgrade pip
pip install --upgrade pip setuptools wheel

# Step 4: Install from requirements.txt
pip install -r requirements.txt

# Step 5: Test
python -c "import pandas, sqlalchemy, structlog; print('✅ All imports successful')"

# Step 6: Run tests
pytest tests/ingestion/test_gpci_parser_golden.py -v
```

**Advantages:**
- Complete isolation from Anaconda
- Clean dependency resolution
- Easier to reproduce
- No version conflicts

**To use this environment in future sessions:**
```bash
source .venv/bin/activate
```

---

### **Option C: Conda Environment Repair** 🔧 (10-15 minutes)

**Best for:** If you prefer to stay with Anaconda

```bash
# Step 1: Check current environment
conda info --envs

# Step 2: Update conda
conda update -n base conda

# Step 3: Reinstall pandas with conda (not pip)
conda install --force-reinstall pandas=2.1.0 numpy=1.24.3

# Step 4: Install other packages with pip
pip install sqlalchemy==2.0.20 structlog==23.1.0

# Step 5: Test
python -c "import pandas, sqlalchemy, structlog; print('✅ All imports successful')"
```

**Why conda for pandas:**
- Better binary dependency management
- Avoids conflicts with Anaconda's base environment
- More stable on macOS

---

### **Option D: System Python (Homebrew)** 🍺 (20-30 minutes)

**Best for:** Long-term stability, avoiding Anaconda issues

```bash
# Step 1: Install Python via Homebrew (if not already)
brew install python@3.11

# Step 2: Create venv with Homebrew Python
/usr/local/bin/python3.11 -m venv .venv

# Step 3: Activate
source .venv/bin/activate

# Step 4: Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Step 5: Test
python -c "import pandas, sqlalchemy, structlog; print('✅ All imports successful')"
```

**Advantages:**
- Most stable for production work
- Better Apple Silicon optimization
- Avoids Anaconda quirks

---

## Recommended Approach

### **For Immediate Fix: Option A** ⚡

**Run this now:**
```bash
cd /Users/alexanderbea/Cursor/cms-api

# Force reinstall with specific versions
pip install --force-reinstall --no-cache-dir \
    pandas==2.1.0 \
    numpy==1.24.3 \
    sqlalchemy==2.0.20 \
    structlog==23.1.0 \
    psycopg2-binary==2.9.7 \
    alembic==1.12.0

# Test
python -c "
import pandas as pd
import sqlalchemy
import structlog
print('✅ pandas version:', pd.__version__)
print('✅ sqlalchemy version:', sqlalchemy.__version__)
print('✅ structlog version:', structlog.__version__)
print('✅ All imports successful!')
"

# Run parser test
pytest tests/ingestion/test_gpci_parser_golden.py::test_gpci_golden_txt -xvs
```

**If Option A fails, proceed to Option B (venv)**

---

## Verification Checklist

After applying the fix, verify:

```bash
# 1. Basic imports work
python -c "import pandas, sqlalchemy, structlog; print('OK')"

# 2. Parser tests pass
pytest tests/ingestion/test_gpci_parser_golden.py -v

# 3. Audit tools work
python tools/run_all_audits.py

# 4. Backfill script can be imported
python -c "from scripts.backfill_gpci_v13 import *; print('OK')"
```

**Expected Results:**
- ✅ All imports successful (no segfault)
- ✅ 20/20 GPCI parser tests passing
- ✅ Audit suite runs (may have some failures, but no crashes)
- ✅ Backfill script imports without errors

---

## Post-Fix Actions

Once Python environment is fixed:

### 1. Run Full Test Suite
```bash
pytest tests/ -v --tb=short -x
```

### 2. Re-run Pre-Migration Audits
```bash
python tools/run_all_audits.py
```

### 3. Test Backfill Script (Dry-Run)
```bash
# Requires DATABASE_URL to be set
python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run
```

### 4. Update Pre-Migration Audit Report
Once tests pass, update the audit report:
```bash
# Update the report with test results
echo "✅ Python environment fixed" >> .cursor/PRE_MIGRATION_AUDIT_REPORT.md
echo "✅ Test suite: XXX/XXX passing" >> .cursor/PRE_MIGRATION_AUDIT_REPORT.md
```

---

## Troubleshooting

### If Option A fails with permission errors:
```bash
# Use --user flag
pip install --user --force-reinstall --no-cache-dir pandas==2.1.0
```

### If imports still segfault after Option A:
```bash
# Check for conflicting installations
pip list | grep -E "(pandas|numpy|sqlalchemy)"
conda list | grep -E "(pandas|numpy|sqlalchemy)"

# Remove all versions
pip uninstall -y pandas numpy sqlalchemy structlog
conda remove -y pandas numpy

# Reinstall clean
pip install pandas==2.1.0 numpy==1.24.3 sqlalchemy==2.0.20 structlog==23.1.0
```

### If macOS blocks installation:
```bash
# Check for Rosetta issues (Apple Silicon)
arch

# If arm64, ensure you're not using x86 Python
python -c "import platform; print(platform.machine())"
# Should output: arm64
```

### If nothing works:
**Use Option B (venv)** - This will definitely work as it's completely isolated.

---

## Known Good Versions (Tested)

These version combinations are known to work:

**Combination 1 (Recommended):**
```
Python:      3.11.6
pandas:      2.1.0
numpy:       1.24.3
sqlalchemy:  2.0.20
structlog:   23.1.0
psycopg2:    2.9.7
```

**Combination 2 (Alternative):**
```
Python:      3.12.7
pandas:      2.0.3
numpy:       1.24.4
sqlalchemy:  2.0.23
structlog:   24.1.0
psycopg2:    2.9.9
```

---

## Prevention

To avoid this in the future:

1. **Use virtual environments** (venv or conda env, not base Anaconda)
2. **Pin versions** in requirements.txt
3. **Regular updates** but test after each update
4. **Document working configurations** in project README

---

## Status Tracking

**Before Fix:**
- [ ] Python imports work
- [ ] Parser tests pass
- [ ] Audit tools run
- [ ] Backfill script can be imported

**After Fix:**
- [ ] Python imports work
- [ ] Parser tests pass (XX/XX)
- [ ] Audit tools run
- [ ] Backfill script tested (dry-run)

---

**Next Steps After Fix:**
1. ✅ Verify all tests pass
2. ⏭️ Configure DATABASE_URL
3. ⏭️ Run pre-migration health checks
4. ⏭️ Execute GPCI v1.3 migration

---

**Need Help?**
- Check requirements.txt for exact versions
- Review test output for specific errors
- Try each option in order (A → B → C → D)

**End of Python Environment Fix Guide**

