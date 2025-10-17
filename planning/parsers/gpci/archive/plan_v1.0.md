```
# GPCI Parser Implementation Plan — Authoritative (v2.1)

**Status:** ✅ ACTIVE — This is the right document to work off of.
**Supersedes:** v1.0, v2.0
**Last updated:** 2025-10-16

➡️ Jump directly to: [v2.1 — Authoritative Plan](#gpci-parser-implementation-plan-v21)

---
# GPCI Parser Implementation Plan v2.1

**Generated:** 2025-10-16  
**Status:** ✅ Ready for implementation (CMS‑native schema v1.2)

**What changed vs v2.0 (review fixes applied):**
- Split **Core Schema** (hash-participating) vs **Optional Enrichment** vs **Provenance** columns.
- Parser template updated to match **actual parser_kit + CF/PPRRVU helpers** (function names & flow).
- Expanded **metadata preflight**: `release_id, schema_id, product_year, quarter_vintage, vintage_date, file_sha256, source_uri`.
- Added a dedicated **row‑count validator** helper (warn/fail tiers).
- Clarified that enrichment & provenance columns are **excluded from the row hash** (core‑only hashing for determinism).

---

## 📋 Overview
GPCI (Geographic Practice Cost Indices) provide locality‑based modifiers for the three MPFS RVU components: **work**, **practice expense (PE)**, and **malpractice (MP)**. One row per **Medicare PFS locality** per effective period. **This parser normalizes indices only**; it **does not compute payment** and **does not apply floors** (policy is applied in pricing).

- **Expected universe:** ~109 localities (guardrail: **100–120 rows**; **fail** if `<90`).  
- **Natural keys:** `['locality_code', 'effective_from']`.  
- **Formats:** TXT (fixed‑width), CSV, XLSX; also appears within quarterly `RVUYY[A-D].zip` bundles.  
- **Release cadence:** Quarterly (A=Jan 1, B=Apr 1, C=Jul 1, D=Oct 1).  
- **Discovery:** CMS PFS Relative Value Files page.  

> **Floors** (e.g., AK 1.50 work GPCI; periodically extended 1.00 work floor elsewhere) are **not hard‑coded here**. Ingest values **as published**; apply any floors in the **pricing service**.

---

## 🧱 Canonical Schema (cms_gpci_v1.2)

### Core Schema Columns — participate in row hash ✅

| Column            | Type    | Constraints / Notes                                      |
|-------------------|---------|----------------------------------------------------------|
| `locality_code`   | string  | **2‑digit, zero‑padded** (`^\d{2}$`); e.g., "01", "99" |
| `gpci_work`       | decimal | 3 dp; soft domain **[0.30, 2.00]** (warn outside)        |
| `gpci_pe`         | decimal | 3 dp; same domain                                        |
| `gpci_mp`         | decimal | 3 dp; same domain                                        |
| `effective_from`  | date    | From release cadence (A/B/C/D)                           |
| `effective_to`    | date    | Null for current; set by interval closure                |

**Hash note:** The **row hash** includes only the **Core Schema** columns in a stable column order.

### Optional Enrichment Columns — excluded from row hash

| Column            | Type   | Source            | Notes                             |
|-------------------|--------|-------------------|-----------------------------------|
| `locality_name`   | string | CMS file / join   | e.g., "ALABAMA", "MANHATTAN"      |
| `state`           | string | CMS file / join   | 2‑letter USPS (not FIPS)          |
| `mac`             | string | CMS file          | 5‑digit Medicare Admin Contractor |

### Provenance Columns — required, excluded from row hash

| Column                | Type   | Notes                                         |
|-----------------------|--------|-----------------------------------------------|
| `source_release`      | string | e.g., `RVU25A`, `RVU25D`                      |
| `source_inner_file`   | string | e.g., `GPCI2025.csv`, `GPCI2025.txt`          |
| `source_sha256`       | string | SHA‑256 of the **inner** file                 |

---

## 🔎 Source of Truth & Provenance
- **Ingest from:** CMS PFS Relative Value Files (quarterly `RVUYY[A-D].zip`).
- **Provenance fields:** persist `source_release`, `source_inner_file`, `source_sha256`.
- **Determinism:** compute a **row hash** over **Core Schema** only; enforce idempotent loads.

---

## 🧭 Layout Registry (TXT fixed‑width)
Keep the layout minimal and CMS‑native. Names must match canonical schema.

```python
GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Patch bump for column name alignment
    'source_version': '2025D',
    'min_line_length': 100,   # Measure against sample
    'columns': {
        'mac':            {'start': 0,   'end': 5,   'type': 'string',  'nullable': False},
        'locality_code':  {'start': 24,  'end': 26,  'type': 'string',  'nullable': False},
        'locality_name':  {'start': 28,  'end': 80,  'type': 'string',  'nullable': True},
        'gpci_work':      {'start': 120, 'end': 125, 'type': 'decimal', 'nullable': False},
        'gpci_pe':        {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': False},
        'gpci_mp':        {'start': 140, 'end': 145, 'type': 'decimal', 'nullable': False},
    },
    'data_start_pattern': r'^\d{5}',  # MAC code at start
}
```

> Do **not** introduce `state_fips` in parsing; maintain geography as enrichment in the warehouse.

---

## 🧰 CSV/XLSX Header Aliases

```python
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code', 'locality': 'locality_code', 'loc': 'locality_code',
    'locality name':   'locality_name',
    'pw gpci':         'gpci_work', 'work gpci': 'gpci_work', '2025 pw gpci (with 1.0 floor)': 'gpci_work',
    'pe gpci':         'gpci_pe',   'practice expense gpci': 'gpci_pe',
    'mp gpci':         'gpci_mp',   'malpractice gpci': 'gpci_mp', 'malp gpci': 'gpci_mp',
}
```

Parser always ingests **published** values; it **never** applies floors or policy transforms.

---

## ✅ Validation & Quality Gates

**Row count**  
- Expect **100–120** rows; **warn** outside; **fail** if `< 90`.

**Domains & guardrails**  
- Soft domain for each of `gpci_work/gpci_pe/gpci_mp`: **[0.30, 2.00]** → **warn** if outside.  
- **Hard fail** if `< 0.20` or `> 2.50`.

**Shape & keys**  
- `locality_code` must match `^\d{2}$` (zero‑pad).  
- No duplicate `(locality_code, 'effective_from')` in final output (duplicates quarantined if encountered upstream).  
- Deterministic sort + **Core‑only** row hashing.

**Integration smoke (post‑load)**  
- Join one CPT (e.g., 99213) with PPRRVU + GPCI + CF and **spot‑check 1–2 localities** against the CMS PFS Lookup tool (±$0.01). This is a **test harness** step, not parser logic.

---

## 🧪 Golden‑First Workflow
1. **Golden fixture**: snapshot latest GPCI inner file (TXT/CSV/XLSX) + `sha256`.  
2. **Unit tests**: schema/dtypes; bounds; key uniqueness; zero‑padding; row‑count.  
3. **Integration tests**: locality count; optional left‑join to locality crosswalk (≥98% match when available).  
4. **Payment spot‑check**: single‑code compute vs CMS tool for sanity.

---

## 🧩 Interfaces (Warehouse & API)
- **Table:** `ref_gpci_indices` (historical; PK `(locality_code, effective_from)`).
- **View:** `vw_gpci_current(as_of_date)` returns one row per locality on a date.  
- **API:** `GET /gpci?locality_code=05&date=2025-01-01` → `{ gpci_work, gpci_pe, gpci_mp, effective_from }`.

---

## 🛰️ Observability & Provenance
Emit in metrics: `release_id`, `source_release`, `source_inner_file`, `source_sha256`, row count, min/max of each `gpci_*`, and a histogram. Store raw artifacts (zip + inner file) for replayability.

---

## ⚠️ Edge Cases
- **Locality reconfigs** (e.g., CA consolidation) → expect row deltas across years; don’t assert 1:1 continuity.  
- **Re‑publication without data change** → identical **Core‑only** row hashes with new `effective_from`.  
- **Third‑party reprints** are non‑authoritative; ingest only CMS artifacts.

---

## 🧩 Parser Structure (Template — matches parser_kit & CF/PPRRVU patterns)

```python
def parse_gpci(file_obj: IO[bytes], filename: str, metadata: Dict[str, Any]) -> ParseResult:
    """Parse GPCI file to canonical schema (cms_gpci_v1.2)."""
    start_time = time.perf_counter()

    # 0) Metadata preflight (Task B)
    validate_required_metadata(metadata, [
        'release_id', 'schema_id', 'product_year', 'quarter_vintage',
        'vintage_date', 'file_sha256', 'source_uri'
    ])

    # 1) Detect encoding (head‑sniff)
    head = file_obj.read(8192)
    encoding, _ = detect_encoding(head)
    file_obj.seek(0)

    # 2) Parse by format (ZIP/TXT/CSV/XLSX)
    content = file_obj.read()
    if filename.lower().endswith('.zip'):
        df, inner_name = _parse_zip(content, encoding)
    elif _is_fixed_width(content):
        layout = get_layout(
            product_year=metadata['product_year'],
            quarter_vintage=metadata['quarter_vintage'],
            dataset='gpci'
        )
        df, inner_name = _parse_fixed_width(content, encoding, layout), filename
    elif filename.lower().endswith(('.xlsx', '.xls')):
        df, inner_name = _parse_xlsx(io.BytesIO(content)), filename
    else:
        df, inner_name = _parse_csv(content, encoding), filename

    # 3) Normalize header names & 3.5) strings
    df = _normalize_column_names(df, alias_map=ALIAS_MAP)  # helper in parser
    df = normalize_string_columns(df)

    # Prepare rejects aggregation
    rejects_df = pd.DataFrame()

    # 4) Cast dtypes with precision (3 dp for gpci_*)
    df = _cast_dtypes(df, metadata)

    # 5) Range validation (WARN + FAIL tiers)
    range_rejects = _validate_gpci_ranges(df)  # returns subset to reject
    if len(range_rejects) > 0:
        rejects_df = pd.concat([rejects_df, range_rejects], ignore_index=True)
        df = df[~df.index.isin(range_rejects.index)].copy()

    # 5.5) Row count validation (warn/fail)
    rowcount_warn = _validate_row_count(df)  # may return warning message
    if rowcount_warn:
        logger.warning(rowcount_warn)

    # 6) Load schema + categorical validation (kept for parity)
    schema = _load_schema(metadata['schema_id'])
    cat_result = enforce_categorical_dtypes(
        df, schema,
        natural_keys=['locality_code', 'effective_from'],
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )

    # 7) Natural key uniqueness (WARN severity)
    unique_df, dupes_df = check_natural_key_uniqueness(
        cat_result.valid_df,
        natural_keys=['locality_code', 'effective_from'],
        severity=ValidationSeverity.WARN,
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id']
    )
    if len(dupes_df) > 0:
        rejects_df = pd.concat([rejects_df, dupes_df], ignore_index=True)

    # 8) Inject metadata + provenance
    for col in ['release_id', 'vintage_date', 'product_year', 'quarter_vintage']:
        unique_df[col] = metadata[col]
    unique_df['source_filename']   = filename
    unique_df['source_file_sha256'] = metadata['file_sha256']
    unique_df['source_uri']        = metadata.get('source_uri', '')
    unique_df['source_release']    = metadata.get('source_release', '')
    unique_df['source_inner_file'] = inner_name
    unique_df['parsed_at']         = pd.Timestamp.utcnow()

    # 9) Finalize (Core‑only hash + sort)
    final_df = finalize_parser_output(
        unique_df,
        natural_keys=['locality_code', 'effective_from'],
        core_columns=['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 'effective_from', 'effective_to']
    )

    # 10) Build metrics (incl. value stats)
    all_rejects = pd.concat([rejects_df, cat_result.rejects_df], ignore_index=True)
    metrics = build_parser_metrics(
        total_rows=len(df) + len(range_rejects),
        valid_rows=len(final_df),
        reject_rows=len(all_rejects),
        encoding_detected=encoding,
        parse_duration_sec=time.perf_counter() - start_time,
        parser_version=PARSER_VERSION,
        schema_id=metadata['schema_id'],
        locality_count=len(final_df),
        gpci_value_stats={
            'work_min': float(final_df['gpci_work'].min()) if len(final_df) else None,
            'work_max': float(final_df['gpci_work'].max()) if len(final_df) else None,
            'pe_min':   float(final_df['gpci_pe'].min())   if len(final_df) else None,
            'pe_max':   float(final_df['gpci_pe'].max())   if len(final_df) else None,
            'mp_min':   float(final_df['gpci_mp'].min())   if len(final_df) else None,
            'mp_max':   float(final_df['gpci_mp'].max())   if len(final_df) else None,
        }
    )

    assert metrics['total_rows'] == len(final_df) + len(all_rejects)
    return ParseResult(data=final_df, rejects=all_rejects, metrics=metrics)
```

---

## 🔧 Helpers (Range & Row‑Count)

```python
def _validate_gpci_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """Return subset of rows that violate hard range thresholds."""
    hard_low, hard_high = 0.20, 2.50
    mask = (
        (df['gpci_work'] < hard_low) | (df['gpci_work'] > hard_high) |
        (df['gpci_pe']   < hard_low) | (df['gpci_pe']   > hard_high) |
        (df['gpci_mp']   < hard_low) | (df['gpci_mp']   > hard_high)
    )
    return df[mask]


def _validate_row_count(df: pd.DataFrame) -> Optional[str]:
    """Warn/fail on unexpected GPCI row counts."""
    count = len(df)
    if count < 90:
        raise ParseError(
            f"CRITICAL: GPCI row count {count} < 90 (minimum threshold). Potential parsing failure."
        )
    if count < 100 or count > 120:
        return f"Row count {count} outside expected range [100, 120]. Possible CMS locality reconfig."
    return None
```

---

## 🧪 Test Plan
**Golden tests**: TXT, CSV, XLSX happy paths, determinism/hash stability.  
**Negative tests**: out‑of‑range values; negative values; invalid locality code; missing `gpci_work`; malformed CSV; duplicate keys; empty file; row‑count under 90.  
**Total:** 12 tests (4 golden + 8 negatives) with golden SHA‑256 pinned.

---

## ✅ Acceptance Criteria
- Output matches **cms_gpci_v1.2** with Core/Enrichment/Provenance separation.
- Locality count within **100–120**; guardrails enforced; **fail** if `<90`.
- No duplicate `(locality_code, effective_from)` in final output.
- Provenance populated (`source_release`, `source_inner_file`, `source_sha256`).
- Row hash computed over **Core Schema only**; golden fixtures reproduce identical hashes.
- Payment spot‑check matches CMS tool within ±$0.01 for sampled localities.

---

## ⏱️ Time Breakdown (≈ 2 hours)
| Task | Time |
|---|---|
| Layout patch + verify | 15m |
| Parser skeleton + helpers | 50m |
| Validation + guards | 15m |
| Golden fixtures + tests | 30m |
| Negative tests | 25m |
| Docs + CHANGELOG | 10m |

---

## 📚 References
- CMS PFS Relative Value Files (RVU bundles)
- Internal standards: STD‑parser‑contracts v1.7 (§21.1 template; §7.3 alignment)
- Companion docs: REF‑cms‑pricing‑source‑map, PRD‑rvu‑gpci, REF‑geography‑source‑map

**Ready to implement.** Keep floors and geography out of the parser; enforce Core‑only hashing for determinism.
# GPCI Parser Implementation Plan — Authoritative (v2.1)

**Status:** ✅ ACTIVE — This is the right document to work off of.
**Supersedes:** v1.0, v2.0
**Last updated:** 2025-10-16

➡️ Jump directly to: [v2.1 — Authoritative Plan](#gpci-parser-implementation-plan-v21)

---
# GPCI Parser Implementation Plan v2.1

**Generated:** 2025-10-16  
**Status:** ✅ Ready for implementation (CMS‑native schema v1.2)

**What changed vs v2.0 (review fixes applied):**
- Split **Core Schema** (hash-participating) vs **Optional Enrichment** vs **Provenance** columns.
- Parser template updated to match **actual parser_kit + CF/PPRRVU helpers** (function names & flow).
- Expanded **metadata preflight**: `release_id, schema_id, product_year, quarter_vintage, vintage_date, file_sha256, source_uri, source_release` (now required).
- Added a dedicated **row‑count validator** helper (warn/fail tiers).
- Clarified that enrichment & provenance columns are **excluded from the row hash** (core‑only hashing for determinism).

---

## 📋 Overview
GPCI (Geographic Practice Cost Indices) provide locality‑based modifiers for the three MPFS RVU components: **work**, **practice expense (PE)**, and **malpractice (MP)**. One row per **Medicare PFS locality** per effective period. **This parser normalizes indices only**; it **does not compute payment** and **does not apply floors** (policy is applied in pricing).

- **Expected universe:** ~109 localities (guardrail: **100–120 rows**; **fail** if `<90`).  
- **Natural keys:** `['locality_code', 'effective_from']`.  
- **Formats:** TXT (fixed‑width), CSV, XLSX; also appears within quarterly `RVUYY[A-D].zip` bundles.  
- **Release cadence:** Quarterly (A=Jan 1, B=Apr 1, C=Jul 1, D=Oct 1).  
- **Discovery:** CMS PFS Relative Value Files page.  

> **Floors** (e.g., AK 1.50 work GPCI; periodically extended 1.00 work floor elsewhere) are **not hard‑coded here**. Ingest values **as published**; apply any floors in the **pricing service**.

---

## 🧱 Canonical Schema (cms_gpci_v1.2)

### Core Schema Columns — participate in row hash ✅

| Column            | Type    | Constraints / Notes                                      |
|-------------------|---------|----------------------------------------------------------|
| `locality_code`   | string  | **2‑digit, zero‑padded** (`^\d{2}$`); e.g., "01", "99" |
| `gpci_work`       | decimal | 3 dp; soft domain **[0.30, 2.00]** (warn outside)        |
| `gpci_pe`         | decimal | 3 dp; same domain                                        |
| `gpci_mp`         | decimal | 3 dp; same domain                                        |
| `effective_from`  | date    | From release cadence (A/B/C/D)                           |
| `effective_to`    | date    | Null for current; set by interval closure                |

**Hash note:** The **row hash** includes only the **Core Schema** columns in a stable column order.

### Optional Enrichment Columns — excluded from row hash

| Column            | Type   | Source            | Notes                             |
|-------------------|--------|-------------------|-----------------------------------|
| `locality_name`   | string | CMS file / join   | e.g., "ALABAMA", "MANHATTAN"      |
| `state`           | string | CMS file / join   | 2‑letter USPS (not FIPS)          |
| `mac`             | string | CMS file          | 5‑digit Medicare Admin Contractor |

### Provenance Columns — required, excluded from row hash

| Column                | Type   | Notes                                         |
|-----------------------|--------|-----------------------------------------------|
| `source_release`      | string | e.g., `RVU25A`, `RVU25D`                      |
| `source_inner_file`   | string | e.g., `GPCI2025.csv`, `GPCI2025.txt`          |
| `source_sha256`       | string | SHA‑256 of the **inner** file                 |

---

## 🔎 Source of Truth & Provenance
- **Ingest from:** CMS PFS Relative Value Files (quarterly `RVUYY[A-D].zip`).
- **Provenance fields:** persist `source_release`, `source_inner_file`, `source_sha256`.
- **Determinism:** compute a **row hash** over **Core Schema** only; enforce idempotent loads.

---

## 🧭 Layout Registry (TXT fixed‑width)
Keep the layout minimal and CMS‑native. Names must match canonical schema.

```python
GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Patch bump for column name alignment
    'source_version': '2025D',
    'min_line_length': 100,   # Set after measuring sample (see note below)
    'columns': {
        'mac':            {'start': 0,   'end': 5,   'type': 'string',  'nullable': False},
        'locality_code':  {'start': 24,  'end': 26,  'type': 'string',  'nullable': False},
        'locality_name':  {'start': 28,  'end': 80,  'type': 'string',  'nullable': True},
        'gpci_work':      {'start': 120, 'end': 125, 'type': 'decimal', 'nullable': False},
        'gpci_pe':        {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': False},
        'gpci_mp':        {'start': 140, 'end': 145, 'type': 'decimal', 'nullable': False},
    },
    'data_start_pattern': r'^\d{5}',  # MAC code at start
}
```

> Do **not** introduce `state_fips` in parsing; maintain geography as enrichment in the warehouse.

**Measure min line length (pre‑implementation):**
```bash
head -40 GPCI2025.txt | tail -20 | awk '{print length($0)}'
# Choose a conservative min_line_length (e.g., 100) based on output
```

---

## 🧰 CSV/XLSX Header Aliases

```python
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code', 'locality': 'locality_code', 'loc': 'locality_code',
    'locality name':   'locality_name',
    'pw gpci':         'gpci_work', 'work gpci': 'gpci_work', '2025 pw gpci (with 1.0 floor)': 'gpci_work',
    'pe gpci':         'gpci_pe',   'practice expense gpci': 'gpci_pe',
    'mp gpci':         'gpci_mp',   'malpractice gpci': 'gpci_mp', 'malp gpci': 'gpci_mp',
}
```

Parser always ingests **published** values; it **never** applies floors or policy transforms.

---

## ✅ Validation & Quality Gates

**Row count**  
- Expect **100–120** rows; **warn** outside; **fail** if `< 90`.

**Domains & guardrails**  
- Soft domain for each of `gpci_work/gpci_pe/gpci_mp`: **[0.30, 2.00]** → **warn** if outside.  
- **Hard fail** if `< 0.20` or `> 2.50`.

**Shape & keys**  
- `locality_code` must match `^\d{2}$` (zero‑pad).  
- No duplicate `(locality_code, 'effective_from')` in final output (duplicates quarantined if encountered upstream).  
- Deterministic sort + **Core‑only** row hashing.

**Integration smoke (post‑load)**  
- Join one CPT (e.g., 99213) with PPRRVU + GPCI + CF and **spot‑check 1–2 localities** against the CMS PFS Lookup tool (±$0.01). This is a **test harness** step, not parser logic.

---

## 🧪 Golden‑First Workflow
1. **Golden fixture**: snapshot latest GPCI inner file (TXT/CSV/XLSX) + `sha256`.  
2. **Unit tests**: schema/dtypes; bounds; key uniqueness; zero‑padding; row‑count.  
3. **Integration tests**: locality count; optional left‑join to locality crosswalk (≥98% match when available).  
4. **Payment spot‑check**: single‑code compute vs CMS tool for sanity.

---

## 🧩 Interfaces (Warehouse & API)
- **Table:** `ref_gpci_indices` (historical; PK `(locality_code, effective_from)`).
- **View:** `vw_gpci_current(as_of_date)` returns one row per locality on a date.  
- **API:** `GET /gpci?locality_code=05&date=2025-01-01` → `{ gpci_work, gpci_pe, gpci_mp, effective_from }`.

---

## 🛰️ Observability & Provenance
Emit in metrics: `release_id`, `source_release`, `source_inner_file`, `source_sha256`, row count, min/max of each `gpci_*`, and a histogram. Store raw artifacts (zip + inner file) for replayability.

---

## ⚠️ Edge Cases
- **Locality reconfigs** (e.g., CA consolidation) → expect row deltas across years; don’t assert 1:1 continuity.  
- **Re‑publication without data change** → identical **Core‑only** row hashes with new `effective_from`.  
- **Third‑party reprints** are non‑authoritative; ingest only CMS artifacts.

---

## 🧩 Parser Structure (Template — matches parser_kit & CF/PPRRVU patterns)

```python
def parse_gpci(file_obj: IO[bytes], filename: str, metadata: Dict[str, Any]) -> ParseResult:
    """Parse GPCI file to canonical schema (cms_gpci_v1.2)."""
    start_time = time.perf_counter()

    # 0) Metadata preflight (Task B) + source_release validation
    validate_required_metadata(metadata, [
        'release_id', 'schema_id', 'product_year', 'quarter_vintage',
        'vintage_date', 'file_sha256', 'source_uri', 'source_release'
    ])
    
    # CI safety: Validate source_release format (RVU25A/B/C/D pattern)
    year = metadata['product_year']
    valid_releases = {f'RVU{year[-2:]}A', f'RVU{year[-2:]}B', 
                     f'RVU{year[-2:]}C', f'RVU{year[-2:]}D'}
    if metadata['source_release'] not in valid_releases:
        raise ParseError(
            f"Unknown source_release: {metadata['source_release']}. "
            f"Expected one of {valid_releases} for year {year}"
        )

    # 1) Detect encoding (head‑sniff)
    head = file_obj.read(8192)
    encoding, _ = detect_encoding(head)
    file_obj.seek(0)

    # 2) Parse by format (ZIP/TXT/CSV/XLSX) using layout‑existence pattern
    content = file_obj.read()
    layout = get_layout(
        product_year=metadata['product_year'],
        quarter_vintage=metadata['quarter_vintage'],
        dataset='gpci'
    )

    if filename.lower().endswith('.zip'):
        df, inner_name = _parse_zip(content, encoding)
    elif layout is not None:
        df, inner_name = _parse_fixed_width(content, encoding, layout), filename
    elif filename.lower().endswith(('.xlsx', '.xls')):
        df, inner_name = _parse_xlsx(io.BytesIO(content)), filename
    else:
        df, inner_name = _parse_csv(content, encoding), filename

    # 3) Normalize header names & 3.5) strings
    df = _normalize_column_names(df, alias_map=ALIAS_MAP)  # helper in parser
    df = normalize_string_columns(df)

    # Prepare rejects aggregation
    rejects_df = pd.DataFrame()

    # 4) Cast dtypes with precision (3 dp for gpci_*)
    df = _cast_dtypes(df, metadata)

    # 5) Range validation (WARN + FAIL tiers)
    range_rejects = _validate_gpci_ranges(df)  # returns subset to reject
    if len(range_rejects) > 0:
        rejects_df = pd.concat([rejects_df, range_rejects], ignore_index=True)
        df = df[~df.index.isin(range_rejects.index)].copy()

    # 5.5) Row count validation (warn/fail)
    rowcount_warn = _validate_row_count(df)  # may return warning message
    if rowcount_warn:
        logger.warning(rowcount_warn)

    # 6) Load schema + categorical validation (kept for parity)
    schema = _load_schema(metadata['schema_id'])
    cat_result = enforce_categorical_dtypes(
        df, schema,
        natural_keys=['locality_code', 'effective_from'],
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )

    # 7) Natural key uniqueness (WARN severity)
    unique_df, dupes_df = check_natural_key_uniqueness(
        cat_result.valid_df,
        natural_keys=['locality_code', 'effective_from'],
        severity=ValidationSeverity.WARN,
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id']
    )
    if len(dupes_df) > 0:
        rejects_df = pd.concat([rejects_df, dupes_df], ignore_index=True)

    # 8) Inject metadata + provenance
    for col in ['release_id', 'vintage_date', 'product_year', 'quarter_vintage']:
        unique_df[col] = metadata[col]
    unique_df['source_filename']    = filename
    unique_df['source_file_sha256'] = metadata['file_sha256']
    unique_df['source_uri']         = metadata.get('source_uri', '')
    unique_df['source_release']     = metadata['source_release']
    unique_df['source_inner_file']  = inner_name
    unique_df['parsed_at']          = pd.Timestamp.utcnow()

    # 9) Finalize (Core‑only hash + sort) — use schema, not core_columns param
    final_df = finalize_parser_output(
        unique_df,
        natural_key_cols=['locality_code', 'effective_from'],
        schema=schema
    )

    # 10) Build metrics (incl. value stats)
    all_rejects = pd.concat([rejects_df, cat_result.rejects_df], ignore_index=True)
    metrics = build_parser_metrics(
        total_rows=len(df) + len(range_rejects),
        valid_rows=len(final_df),
        reject_rows=len(all_rejects),
        encoding_detected=encoding,
        parse_duration_sec=time.perf_counter() - start_time,
        parser_version=PARSER_VERSION,
        schema_id=metadata['schema_id'],
        locality_count=len(final_df),
        gpci_value_stats={
            'work_min': float(final_df['gpci_work'].min()) if len(final_df) else None,
            'work_max': float(final_df['gpci_work'].max()) if len(final_df) else None,
            'pe_min':   float(final_df['gpci_pe'].min())   if len(final_df) else None,
            'pe_max':   float(final_df['gpci_pe'].max())   if len(final_df) else None,
            'mp_min':   float(final_df['gpci_mp'].min())   if len(final_df) else None,
            'mp_max':   float(final_df['gpci_mp'].max())   if len(final_df) else None,
        }
    )

    assert metrics['total_rows'] == len(final_df) + len(all_rejects)
    return ParseResult(data=final_df, rejects=all_rejects, metrics=metrics)
```

---

## 🔧 Helpers (Range & Row‑Count)

```python
def _validate_gpci_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """Return subset of rows that violate hard range thresholds."""
    hard_low, hard_high = 0.20, 2.50
    mask = (
        (df['gpci_work'] < hard_low) | (df['gpci_work'] > hard_high) |
        (df['gpci_pe']   < hard_low) | (df['gpci_pe']   > hard_high) |
        (df['gpci_mp']   < hard_low) | (df['gpci_mp']   > hard_high)
    )
    return df[mask]


def _validate_row_count(df: pd.DataFrame) -> Optional[str]:
    """Warn/fail on unexpected GPCI row counts."""
    count = len(df)
    if count < 90:
        raise ParseError(
            f"CRITICAL: GPCI row count {count} < 90 (minimum threshold). Potential parsing failure."
        )
    if count < 100 or count > 120:
        return f"Row count {count} outside expected range [100, 120]. Possible CMS locality reconfig."
    return None
```

---

## 🔧 Helper Function Implementations (GPCI)

> These mirror the CF/PPRRVU patterns with GPCI‑specific casts.

```python
from zipfile import ZipFile
import io, csv


def _parse_zip(content: bytes, encoding: str) -> Tuple[pd.DataFrame, str]:
    """Parse a CMS ZIP and return (df, inner_filename). Picks the first member whose
    name contains 'GPCI' (case‑insensitive). Falls back to the only member if single‑file ZIP."""
    with ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()
        if len(names) == 0:
            raise ParseError("Empty ZIP archive")
        # Prefer a member with GPCI in name
        inner = next((n for n in names if 'gpci' in n.lower()), names[0])
        with zf.open(inner) as fh:
            raw = fh.read()
        # Route by extension
        lower = inner.lower()
        if lower.endswith(('.xlsx', '.xls')):
            return _parse_xlsx(io.BytesIO(raw)), inner
        elif lower.endswith('.txt') and b',' not in raw[:1024] and b'\t' not in raw[:1024]:
            # Assume fixed‑width if no delimiters in the head
            # Layout must be resolved by caller (Parser Structure) — here we default to CSV
            return _parse_csv(raw, encoding), inner
        else:
            return _parse_csv(raw, encoding), inner


def _parse_fixed_width(content: bytes, encoding: str, layout: Dict[str, Any]) -> pd.DataFrame:
    """Read fixed‑width using the layout registry colspecs."""
    text = content.decode(encoding, errors='replace').splitlines()
    colspecs = [(c['start'], c['end']) for c in layout['columns'].values()]
    names = list(layout['columns'].keys())
    df = pd.read_fwf(io.StringIO('\n'.join(text)), colspecs=colspecs, names=names, dtype=str)
    return df


def _parse_xlsx(buff: io.BytesIO) -> pd.DataFrame:
    """Parse Excel as strings to avoid coercion; strip duplicate header rows."""
    df = pd.read_excel(buff, dtype=str)
    # Drop duplicate header rows if present
    if len(df) > 1 and (df.iloc[0] == df.columns).all():
        df = df.iloc[1:].reset_index(drop=True)
    return df


def _parse_csv(content: bytes, encoding: str) -> pd.DataFrame:
    """Parse CSV with dialect sniffing; treat all as strings initially."""
    sample = content[:2048].decode(encoding, errors='replace')
    try:
        dialect = csv.Sniffer().sniff(sample)
        sep = dialect.delimiter
    except Exception:
        sep = ','
    df = pd.read_csv(io.BytesIO(content), dtype=str, sep=sep)
    # Guard: if first row repeats headers, drop it
    if len(df) > 1 and set(map(str.lower, df.columns)) == set(map(str.lower, df.iloc[0].tolist())):
        df = df.iloc[1:].reset_index(drop=True)
    return df


def _normalize_column_names(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """Lowercase, strip BOM/NBSP, collapse spaces, alias to canonical names."""
    norm = {}
    for c in df.columns:
        cc = str(c).replace('\ufeff', '').replace('\xa0', ' ').strip().lower()
        cc = ' '.join(cc.split())  # collapse spaces
        norm[c] = alias_map.get(cc, cc).strip().replace(' ', '_')
    df = df.rename(columns=norm)
    return df


def _cast_dtypes(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
    """Cast with 3dp precision for GPCI numeric columns; ensure zero‑padded locality codes; dates."""
    # Ensure presence of expected columns before casting
    for col in ['gpci_work', 'gpci_pe', 'gpci_mp']:
        if col in df.columns:
            df[col] = canonicalize_numeric_col(df[col], precision=3, rounding_mode='HALF_UP')
    # Dates
    if 'effective_from' not in df.columns:
        df['effective_from'] = pd.to_datetime(f"{metadata['product_year']}-01-01")
    else:
        df['effective_from'] = pd.to_datetime(df['effective_from'], errors='coerce')
    if 'effective_to' in df.columns:
        df['effective_to'] = pd.to_datetime(df['effective_to'], errors='coerce')
    else:
        df['effective_to'] = pd.NaT
    # Locality code formatting
    if 'locality_code' in df.columns:
        df['locality_code'] = df['locality_code'].astype(str).str.strip().str.zfill(2)
    return df


def _load_schema(schema_id: str) -> Dict[str, Any]:
    """Load schema JSON by id from the registry (parser kit helper or local map)."""
    return load_schema_from_registry(schema_id)
```

---

## 🧪 Test Plan
**Golden tests**: TXT, CSV, XLSX happy paths, determinism/hash stability.  
**Negative tests**: out‑of‑range values; negative values; invalid locality code; missing `gpci_work`; malformed CSV; duplicate keys; empty file; row‑count under 90.  
**Total:** 12 tests (4 golden + 8 negatives) with golden SHA‑256 pinned.

---

## ✅ Acceptance Criteria
- Output matches **cms_gpci_v1.2** with Core/Enrichment/Provenance separation.
- Locality count within **100–120**; guardrails enforced; **fail** if `<90`.
- No duplicate `(locality_code, effective_from)` in final output.
- Provenance populated (`source_release`, `source_inner_file`, `source_sha256`).
- Row hash computed over **Core Schema only**; golden fixtures reproduce identical hashes.
- Payment spot‑check matches CMS tool within ±$0.01 for sampled localities.

---

## ⏱️ Time Breakdown (≈ 2 hours)
| Task | Time |
|---|---|
| Layout patch + verify | 15m |
| Parser skeleton + helpers | 50m |
| Validation + guards | 15m |
| Golden fixtures + tests | 30m |
| Negative tests | 25m |
| Docs + CHANGELOG | 10m |

---

## 📚 References
- CMS PFS Relative Value Files (RVU bundles)
- Internal standards: STD‑parser‑contracts v1.7 (§21.1 template; §7.3 alignment)
- Companion docs: REF‑cms‑pricing‑source‑map, PRD‑rvu‑gpci, REF‑geography‑source‑map

**Ready to implement.** Keep floors and geography out of the parser; enforce Core‑only hashing for determinism.