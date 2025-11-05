# Environment Stabilization Plan (v1.0)

**Context:** Recent MPFS ingestion tests (`tests/ingestors/test_mpfs_ingestor_e2e.py`) failed with a Python segmentation fault caused by missing or mismatched native dependencies (Parquet/PDF stack). This plan standardizes the local development environment so ingestion and PDF tooling run reliably across machines and in CI.

---

## 1. Goals
- Provide a reproducible Python toolchain for ingestion + PDF extraction work.
- Ensure all native libraries required by Parquet (`pyarrow`/`fastparquet`) and PDF/OCR tooling are installed once and validated.
- Document bootstrap steps so new engineers (and CI) can stand up the environment quickly.
- Unblock MPFS ingestion tests and upcoming `pdf_reader` implementation.

---

## 2. Target Stack

| Layer | Requirement |
|-------|-------------|
| Python Runtime | CPython 3.11.x (aligned with repo tooling) managed via `pyenv` or system install. |
| Virtual Environment | Project-scoped venv created under `.venv/` (or use Poetry). |
| Core Python Packages | `pandas`, `numpy`, `pyarrow`, `fastparquet`, `pytest`, `pytest-asyncio`, `structlog`, `pdfplumber`, `pdfminer.six`, `pytesseract`, `Pillow`. |
| Native Libraries | Arrow runtime (`libarrow`), compression libs (`zlib`, `liblzma`, `libsnappy`), Tesseract OCR (for future PDF reader), Poppler utilities (optional for PDF page counts). |
| Tooling | `pre-commit`, `ruff`, `black` (optional but aligns with repo tooling). |

---

## 3. Step-by-Step Stabilization

### 3.1 Assess Current Environment
1. Capture Python version: `python3 --version`.
2. Check for existing virtual environment: `python3 -m venv --help` and inspect `.venv/`.
3. Verify native prerequisites:  
   - macOS: `brew list pyarrow tesseract snappy`  
   - Debian/Ubuntu: `dpkg -l | grep libarrow`
4. Log the findings in the issue/PR to document the starting state.

### 3.2 Install / Upgrade Native Dependencies
| Platform | Commands |
|----------|----------|
| **macOS (Homebrew)** | `brew install pyenv` (optional), `brew install python@3.11`, `brew install pyarrow snappy tesseract libomp` |
| **Ubuntu/Debian** | `sudo apt update && sudo apt install python3.11 python3.11-venv libarrow-dev libparquet-dev libtesseract-dev tesseract-ocr libsnappy-dev` |
| **Fallback** | If native packages unavailable, rely on manylinux wheels (`pip install pyarrow==<ver>`), but ensure `libstdc++`/`libgcc` up to date. |

### 3.3 Create and Bootstrap Virtual Environment
1. `python3.11 -m venv .venv`
2. `source .venv/bin/activate`
3. Upgrade tooling: `pip install --upgrade pip setuptools wheel`
4. Install project dependencies:  
   - Generate `requirements-dev.txt` (or use existing `pyproject.toml`).  
   - `pip install -r requirements-dev.txt` (include pytest, pyarrow, pdf stack).
5. Freeze versions (`pip freeze > requirements.lock`) to aid reproducibility.

### 3.4 Validate Parquet & PDF Stack
1. `python -c "import pyarrow; import fastparquet; import pandas"`  
2. `python -c "import pdfplumber; import pdfminer.six"`  
3. `python -c "import pytesseract; print(pytesseract.get_tesseract_version())"` (optional).
4. Document outputs and confirm no ImportErrors/segfaults.

### 3.5 Update Documentation & Automation
1. Add “Environment Setup” section to `README.md` or a new `docs/dev_setup.md`.  
2. Update `artifacts/pdf_reader.md` with PDF/OCR dependency notes.  
3. Add bootstrap script `scripts/bootstrap_env.sh` to automate Steps 3.2–3.3.  
4. Register script in `Makefile`/`taskfile` (if used) for quick invocation.

### 3.6 Integrate with CI
1. Add GitHub Actions job (or update existing workflow) to:  
   - Install native deps (using `apt` or `brew` equivalents).  
   - Set up Python 3.11, create venv, install dependencies.  
   - Run `python -m pytest tests/ingestors/test_mpfs_ingestor_e2e.py`.
2. Ensure CI caches virtualenv/wheels for faster runs (`actions/cache`).
3. Fail fast if the environment diverges (missing deps).

---

## 4. Validation Checklist
- [ ] `python -m pytest tests/ingestors/test_mpfs_ingestor_e2e.py` completes without segfault.  
- [ ] Sample PDF extraction scripts (placeholder until `pdf_reader`) run without native errors.  
- [ ] Developers can recreate the environment following documented steps.  
- [ ] CI job mirrors local setup and runs ingestion tests successfully.  
- [ ] Requirements lockfile/pyproject updated with explicit versions.  

---

## 5. Timeline & Ownership
| Task | Owner | Target |
|------|-------|--------|
| Native deps install + venv bootstrap | Data Eng (local machine owners) | Day 0 |
| Documentation updates (`README`, `pdf_reader.md`) | Data Eng / Docs lead | Day 1 |
| CI workflow update | Platform Eng | Day 2–3 |
| Validation & sign-off (attach pytest logs) | Data Eng lead | Day 3 |

---

## 6. Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Dependency drift across machines | Freeze dependencies (`requirements.lock`), document reinstall steps. |
| CI env lacks native packages | Add explicit install step; consider Docker image with prebuilt libs. |
| Tesseract/Poppler increase setup time | Make OCR optional behind feature flag until PDF reader lands. |
| Future Python upgrades | Pin Python version in docs/CI; revisit annually or when breaking changes occur. |

---

## 7. Follow-Up
1. Run MPFS ingestion tests after environment stabilized (tracks next session task).  
2. Proceed with `pdf_reader` implementation using the standardized stack.  
3. Monitor CI for stability; adjust lockfiles when library updates are required.  

_Stored at `artifacts/env_stabilization_plan.md`._
