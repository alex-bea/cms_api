## RVU E2E Harness Modernization Plan

### Executive summary
- ANES parser fix is green; safe to push and redeploy.
- RVU E2E local failures stem from mixed sync/async usage and dict vs object drift after introducing `asyncio.run()` and `RawBatch`. Test harness also writes to read-only paths on Render and scans unrelated datasets, creating noise.
- This document outlines the staged plan to modernize the RVU E2E harness and eliminate the failures/warnings locally without blocking ANES deploys.

### Root causes
- **Metadata errors**: legacy helpers expect dicts; pipeline now passes `RawBatch`-like objects → `AttributeError: 'dict' has no attribute 'metadata'`.
- **Async crash**: `asyncio.run()` called within a running loop; un-awaited coroutine `_discover_source_files_async`.
- **Read-only writes**: Harness writes under `tests/fixtures/rvu/test_data` which is read-only on Render.
- **Discovery scope**: Discovery walks entire RVU ZIP sets, triggering OPPSCAP/PPRRVU logs and parser errors unrelated to ANES.
- **Warnings**: parser decimal formatting noise; pandas and pydantic deprecations.

### Plan of record

1) Fix test I/O to always use tmp directory
- Write to `tmp_path`/`tmp_path_factory` rather than repo paths.
- Add `RVU_TEST_DATA_DIR` env support; default to `/tmp/cms_rvu_tests/<uuid>`.
- On Render, prefer `/tmp`; ensure cleanup.

2) Async hygiene and event loop safety
- Remove `asyncio.run()` from library code and helpers.
- Provide public async APIs; use `pytest.mark.asyncio` and `pytest-asyncio` with `asyncio_mode = auto`.
- Ensure `_discover_source_files_async` is awaited by callers; consider `maybe_await(value_or_coro)` utility.

3) Reconcile data model: dict vs object
- Standardize stage inputs/outputs on a `RawBatch`-like object with `.metadata` and `.df`.
- Add adapters in tests to convert legacy dict fixtures to `RawBatch`.
- Audit `_normalize_stage`, `_enrich_stage`, `_publish_stage` to accept standardized object; add type hints.

4) Scope dataset discovery during tests
- Add allowlist (e.g., `datasets={"anes"}`) so E2E tests only touch the target dataset.
- Disable deep crawl for unrelated RVU directories in tests.

5) Tame logging noise and warn only on relevant issues
- In tests, set `caplog` to reduce parser log verbosity except when asserted.
- Aggregate/limit “Failed to format decimal …” warnings.
- Filter third-party deprecation warnings in `pytest.ini`.

6) Render read-only compatibility
- Route all test outputs to `RVU_TEST_DATA_DIR`/`tempfile.gettempdir()`.
- Replace any `Path(__file__)` writes under `tests/**` with tmp locations.

7) Isolate or xfail non-ANES RVU parsers during modernization
- Temporarily `xfail`/skip OPPSCAP/PPRRVU E2E cases; split dataset-specific E2E files.
- Run ANES-only in PR CI; full RVU nightly.

8) Clean pytest-asyncio warnings and loop lifecycle
- Avoid lingering tasks; cancel/await at teardown.
- Close custom loops in fixtures; avoid nested loops.

9) Deps deprecation backlog (non-blocking)
- Track Pydantic v2 migration and SQLAlchemy `declarative_base` import updates.

### Concrete deliverables
- `pytest.ini`: `asyncio_mode = auto`, filtered warnings.
- `RVU_TEST_DATA_DIR` support in tests and ingestors.
- `rvu_ingestor.py`: remove `asyncio.run`, await discovery, dataset allowlist, tmp dir usage.
- `_parser_kit.py`: warning aggregation.
- Stage helpers: standardized `RawBatch` usage; adapters.
- Tests: use `tmp_path`; pass dataset allowlist; temporary skips/xfails.
- Docs: update `INGESTION_GUIDE.md`, `HOW_TO_RUN_LOCALLY.md`.

### Validation criteria
- ANES-only RVU E2E locally: 0 failures; warnings reduced ≥90%.
- No `asyncio.run()` RuntimeError; no un-awaited coroutine warnings.
- No writes under `tests/**`; artifacts in tmp dir.
- No OPPSCAP/PPRRVU logs during ANES-only tests.

### Suggested sequence
1) Merge/push ANES parser fix; redeploy.
2) Add discovery allowlist; switch tests to tmp output env.
3) Remove `asyncio.run()`; configure pytest asyncio.
4) Standardize on `RawBatch`; update stages and adapters.
5) Reduce logging/warnings.
6) Split CI jobs; add xfails.

### Runbook snippets
- ANES-only locally: `pytest tests/ingestors/test_rvu_ingestor_e2e.py -k anes -q`
- Full RVU (post-modernization): `pytest tests/ingestors/test_rvu_ingestor_e2e.py -q`
- Temp dir: `export RVU_TEST_DATA_DIR=$(mktemp -d /tmp/rvu_tests.XXXXXX)`


