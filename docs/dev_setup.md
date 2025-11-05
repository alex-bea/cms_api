# CMS Pricing API – Local Environment Bootstrap

This guide captures the reproducible host setup we use for ingestion and PDF/OCR work. Follow the steps in order; every command assumes the repository root (`/Users/alexanderbea/Cursor/cms-api`) unless a different directory is mentioned.

## 1. Host Prerequisites (macOS)
- Install Homebrew if it is not already available.
- Keep system packages fresh: `brew update`.
- Install the native libraries that back Parquet + OCR tooling:
  ```bash
  brew install python@3.11 apache-arrow snappy tesseract libomp
  brew ls --versions python@3.11 apache-arrow snappy tesseract libomp
  ```
  Capture the version output for your log or pull request.

## 2. Python Virtual Environment
1. Start from a clean project interpreter:
   ```bash
   cd /Users/alexanderbea/Cursor/cms-api
   rm -rf .venv
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```
2. Upgrade packaging helpers:
   ```bash
   python -m pip install --upgrade pip setuptools wheel
   ```

## 3. Dependency Installation
Install the pinned runtime and developer requirements:
```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```
The core libraries now cover:
- `pyarrow==16.1.0`, `fastparquet==2024.11.0`, `pandas==2.2.3`
- PDF/OCR stack: `pdfplumber`, `pdfminer.six`, `pypdf`, `pypdfium2`, `pytesseract`, `Pillow`

Finally, snapshot the environment so others (and CI) can reproduce it:
```bash
python -m pip freeze > requirements.lock
```

## 4. Sanity Checks
Run the import guards to make sure the Parquet/PDF stack is healthy:
```bash
python -c "import pandas, pyarrow, fastparquet"
python -c "import pdfplumber, pdfminer"
python -c "import pytesseract; print(pytesseract.get_tesseract_version())"
```
Record the outputs (success plus the Tesseract version) in the stabilization issue.

## 5. Next Steps
- Execute `python -m pytest tests/ingestors/test_mpfs_ingestor_e2e.py` to confirm the segfault is gone.
- When onboarding a new machine or CI worker, run `scripts/bootstrap_env.sh` (added in this plan) instead of calling the commands manually—the script mirrors the steps above.

