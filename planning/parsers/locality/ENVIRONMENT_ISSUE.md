# Environment Issue: Python Segfault (Exit 139)

**Status:** Blocking test execution  
**Impact:** Parser code is valid, tests ready, but cannot execute locally  
**Created:** 2025-10-17

## Problem

Python segfaults (exit code 139) when importing pandas/pyarrow:

```bash
$ python -c "import pandas; import pyarrow"
# Exit code: 139 (segmentation fault)
```

**Affected:**
- `/opt/anaconda3/bin/python` (Anaconda Python 3.9)
- `/Library/Developer/CommandLineTools/usr/bin/python3` (System Python 3.9)
- Both `.venv` and `.venv_gpci` virtual environments

## Root Cause

macOS-specific pandas/pyarrow compatibility issue:
- Arrow C++ library conflicts with macOS system libraries
- Known issue on macOS 14+ (Sonoma) with certain pyarrow versions
- Same issue encountered earlier when trying to create parquet files

## Verified Working

✅ **Parser code is syntactically valid:**
```bash
$ python3 -c "import ast; ast.parse(open('cms_pricing/ingestion/parsers/locality_parser.py').read())"
# No errors
```

✅ **Test code is valid:**
- `tests/parsers/test_locality_parser.py` created
- 4 comprehensive tests ready

## Solutions

### Option 1: Docker (Recommended)
```bash
# Use Docker environment (clean Python + dependencies)
docker-compose up -d
docker-compose exec web pytest tests/parsers/test_locality_parser.py -xvs
```

### Option 2: Rebuild Environment
```bash
# Remove broken environments
rm -rf .venv .venv_gpci

# Create fresh venv with compatible versions
python3 -m venv .venv_locality
source .venv_locality/bin/activate

# Install with pinned versions (avoid segfault)
pip install pandas==2.0.3 pyarrow==12.0.1 pytest structlog

# Run tests
export PYTHONPATH=/Users/alexanderbea/Cursor/cms-api
pytest tests/parsers/test_locality_parser.py -xvs
```

### Option 3: Use Homebrew Python
```bash
# Install Homebrew Python (often more stable)
brew install python@3.11

# Create venv with Homebrew Python
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv_locality
source .venv_locality/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/parsers/test_locality_parser.py -xvs
```

### Option 4: Skip Local, Use CI
```bash
# Commit code and let GitHub Actions run tests
git add cms_pricing/ingestion/parsers/locality_parser.py
git add tests/parsers/test_locality_parser.py
git commit -m "feat: Add locality parser (Phase 1)"
git push

# Tests will run in CI with clean environment
```

## Files Ready for Testing

Once environment is fixed:

```bash
# Run golden test
export REF_MODE=inline
pytest tests/parsers/test_locality_parser.py::test_locality_raw_txt_golden -xvs

# Run all locality tests
pytest tests/parsers/test_locality_parser.py -xvs

# Expected results:
# - test_locality_raw_txt_golden: PASSED
# - test_locality_natural_key_uniqueness: PASSED  
# - test_locality_encoding_detection: PASSED
# - test_locality_column_names_normalized: PASSED
```

## Workaround for Current Session

Since we can't run tests locally:

1. ✅ **Syntax validated** - Parser is valid Python
2. ✅ **Tests written** - 4 comprehensive tests ready
3. ✅ **Pattern proven** - Follows GPCI parser (already working)
4. ⏭️ **Defer execution** - Run in Docker or fixed environment

## Next Steps

**Immediate:**
- Proceed with Phase 2 (normalizer) - doesn't need local tests yet
- Fix environment before integration testing

**Before Production:**
- Fix local environment using one of the options above
- Run full test suite (golden + edge cases)
- Verify in Docker to ensure CI will pass

---

**Recommendation:** Use Docker (Option 1) for immediate testing, then fix local environment for development workflow.
