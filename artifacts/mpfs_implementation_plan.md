# MPFS Ingestor Implementation Plan *(v2)*

**Date:** 2025-01-15  
**Status:** Draft v2.0 – ready for execution  
**Priority:** 🔴 Critical for ClearBill launch  
**Estimated Effort:** 10–15 engineering days (2 devs pairing across stages)

---

## 0. Goal & Success Criteria

Deliver a production-ready `MPFSIngestor` that:
- Reuses the authoritative RVU + GPCI datasets already ingested by `RVUIngestor`.
- Lands and versions CMS conversion-factor (CF) artifacts, enriching them alongside RVU/GPCI.
- Materializes curated MPFS tables (`mpfs_rvu`, `mpfs_indicators_all`, `mpfs_locality`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_payment_curated`, `mpfs_link_keys`) with full provenance metadata.
- Powers `/v1/mpfs` responses from curated storage, not ad-hoc joins, and records `datasets_used` consistently.
- Ships with deterministic tests (golden comparisons vs CMS PFREV amounts) and updated documentation/PRDs.

Success checkpoint = MPFS ingestion run on latest vintage with:  
`release_id`, `batch_id`, row counts, validation metrics, dataset digests logged + `/v1/mpfs` contract tests green.

---

## 0A. Quick Reference

| Metric | Value |
|--------|-------|
| **Total Effort** | 10-15 engineering days (2 devs pairing) |
| **Phases** | 8 phases (0-8) |
| **New Files** | 3 (`conversion_factor_fetcher.py`, `mpfs_builder.py`, test files) |
| **Files to Modify** | 5 (`mpfs_ingestor.py`, `dataset_snapshot_service.py`, test files, docs) |
| **Files to Delete** | 1 (`cms_mpfs_scraper.py`) |
| **Key Dependencies** | RVU ingestor must be complete (snapshots available) |
| **Critical Path** | Phase 1 (services) → Phase 2 (discovery) → Phases 3-5 (pipeline) |
| **Blocking Risk** | Missing RVU/GPCI snapshots (verify before Phase 1) |

**Quick Start:** Jump to Phase 1.3-1.4 to create services, then Phase 2 to refactor discovery.

**Pre-Flight Checklist (Before Starting):**
```bash
# Verify RVU snapshots exist
psql $DATABASE_URL -c "SELECT dataset_id, release_id, effective_from FROM dataset_snapshots WHERE dataset_id IN ('rvu_items', 'gpci_indices') ORDER BY created_at DESC LIMIT 5;"

# Verify database schema
psql $DATABASE_URL -c "\d fee_mpfs" | grep -E "(release_id|batch_id)"

# Check test data available
ls -la sample_data/rvu25d_0/PPRRVU2025_Oct.txt
ls -la sample_data/rvu25d_0/GPCI2025.txt
ls -la sample_data/rvu25d_0/RVU25D.pdf

# Verify RVU ingestor works (reference pattern)
python -c "from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor; print('✅ RVU ingestor importable')"
```

---

## 1. Current State Snapshot

| Area | Observation |
|------|-------------|
| Code | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` is scaffolded: validation + curated view builders are placeholders; publication emits empty dicts. |
| Discovery | MPFS currently relies on dedicated scraper (to be removed); ingestion will reuse RVU/GPCI snapshots and add a conversion-factor fetcher. |
| Tests | `tests/ingestors/test_mpfs_ingestor_e2e.py` covers stubs only; no golden-payment assertions. |
| Docs | `prds/REF-cms-pricing-source-map-prd-v1.0.md` mentions MPFS but lacks curated-view details; readiness plan requires MPFS to be operational. |
| Data paths | RVU + GPCI already in `DatasetSnapshot`; CF artifacts not yet tracked. |

---

## 2. Implementation Phases

> **💡 Tip:** Each phase builds on the previous. Start with Phase 1.3-1.4 (create services) to unblock all downstream work. Use RVU ingestor (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`) as reference pattern throughout.

### Phase 0 – Prerequisites & Alignment (0.5 day)
1. Confirm vintage targets (e.g., CY2025 RVU D release, GPCI annual, CF 2025).  
2. Document input digests in runbook (`artifacts/mpfs_opps_ingestion_runbook.md`).  
3. Align with API team on required output schema (facility vs non-facility amounts, locality joins, provenance fields).

### Phase 1 – Retire Old Scraper Dependency (1 day)
**Objective:** Remove deprecated `CMSMPFSScraper` and refactor MPFS ingestor to use snapshot-based discovery.

**1.1 Delete Scraper & Clean Up References**
- Delete `cms_pricing/ingestion/scrapers/cms_mpfs_scraper.py` (and its tests).
- Remove scraper imports from:
  - `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`
  - `tests/ingestors/test_mpfs_ingestor_e2e.py`
  - `artifacts/mpfs_opps_ingestion_runbook.md`
  - Any other documentation referencing MPFS scraper

**1.2 Update MPFSIngestor Constructor**
- Modify `MPFSIngestor.__init__` to:
  - Accept injected `DatasetSnapshotService` (required)
  - Accept injected `ConversionFactorFetcher` (required)
  - Remove `CMSMPFSScraper` initialization
  - Remove scraper-related instance variables

**1.3 Extend DatasetSnapshotService**
Add helper methods to `cms_pricing/services/dataset_snapshot_service.py`:

```python
def get_latest_snapshot(self, dataset_id: str) -> Optional[Dict[str, Any]]:
    """
    Get latest snapshot metadata for dataset.
    
    Returns:
        Dict with keys: path, checksum, release_id, effective_from, effective_to, digest
        Returns None if no snapshot found
    """
    snapshot = self.select_snapshot(dataset_id, valuation_date=date.today())
    if not snapshot:
        return None
    
    # Resolve curated parquet path from snapshot metadata
    # Path format: data/curated/{dataset}/{release_id}/{table}.parquet
    curated_path = self._resolve_curated_path(snapshot)
    
    return {
        "path": curated_path,
        "checksum": snapshot.digest,
        "release_id": snapshot.release_id,
        "effective_from": snapshot.effective_from,
        "effective_to": snapshot.effective_to,
        "digest": snapshot.digest,
        "manifest_url": snapshot.manifest_url
    }

def register_snapshot(
    self,
    dataset_id: str,
    release_id: str,
    digest: str,
    effective_from: date,
    effective_to: Optional[date] = None,
    manifest_url: Optional[str] = None,
    curated_path: Optional[str] = None
) -> DatasetSnapshot:
    """
    Register snapshot with enhanced metadata support for MPFS curated datasets.
    
    Supports dataset_id values: mpfs_rvu, mpfs_cf, mpfs_payment, mpfs_gpci, etc.
    """
    # Use existing register_snapshot implementation
    # Add curated_path to manifest if provided
    return self.register_snapshot(...)
```

**1.4 Create ConversionFactorFetcher**
Create new file `cms_pricing/ingestion/services/conversion_factor_fetcher.py`:

```python
class ConversionFactorFetcher:
    """
    Handles conversion factor download/cache for MPFS ingestion.
    
    Ensures CF artifacts are available locally, downloads if missing,
    caches under data/ingestion/mpfs/raw, and returns metadata.
    """
    
    async def ensure_conversion_factor(
        self, 
        year: int,
        manual_override_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Ensure conversion factor artifact is available for given year.
        
        Args:
            year: Calendar year for CF
            manual_override_path: Optional path to manually downloaded CF file
            
        Returns:
            Dict with keys: path, checksum, effective_from, effective_to, source_url
        """
        # If manual override provided, use it
        # Otherwise, check cache first
        # If not cached, download from CMS
        # Store in data/ingestion/mpfs/raw/{year}/conversion_factor.{ext}
        # Calculate checksum
        # Return metadata
```

**1.5 Update Discovery Manifest Generation**
- Adjust discovery to record:
  - **Reuse entries** for RVU/GPCI (source = snapshot, no download)
  - **Download entries** for CF (source = CMS URL, download required)
- Emit discovery metadata distinguishing reuse vs download for observability

### Phase 2 – Refactor Discovery & Land Stages (1.5 days)
**Objective:** Update discovery and land stages to use snapshot service and CF fetcher.

**2.1 Refactor `discover_source_files()`**
```python
async def discover_source_files(self) -> List[SourceFile]:
    """
    Discover MPFS source files using snapshot service and CF fetcher.
    
    Returns:
        List of SourceFile entries:
        - RVU/GPCI: snapshot-backed entries with dataset_id and release_id
        - CF: download entry with URL/path/checksum
    """
    source_files = []
    
    # Pull RVU snapshot via snapshot service
    rvu_meta = self.snapshot_service.get_latest_snapshot("rvu_items")
    if rvu_meta:
        source_files.append(SourceFile(
            url=None,  # Reuse, no download
            filename=f"rvu_items_{rvu_meta['release_id']}.parquet",
            content_type="application/parquet",
            dataset_id="rvu_items",  # New field
            release_id=rvu_meta["release_id"],
            path=rvu_meta["path"],
            checksum=rvu_meta["checksum"],
            is_reuse=True  # Flag for land stage
        ))
    
    # Pull GPCI snapshot
    gpci_meta = self.snapshot_service.get_latest_snapshot("gpci_indices")
    if gpci_meta:
        source_files.append(SourceFile(
            url=None,
            filename=f"gpci_indices_{gpci_meta['release_id']}.parquet",
            content_type="application/parquet",
            dataset_id="gpci_indices",
            release_id=gpci_meta["release_id"],
            path=gpci_meta["path"],
            checksum=gpci_meta["checksum"],
            is_reuse=True
        ))
    
    # Call CF fetcher
    cf_meta = await self.cf_fetcher.ensure_conversion_factor(year=2025)
    source_files.append(SourceFile(
        url=cf_meta.get("source_url"),
        filename=Path(cf_meta["path"]).name,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        dataset_id="mpfs_cf",
        path=cf_meta["path"],
        checksum=cf_meta["checksum"],
        is_reuse=False  # Download required
    ))
    
    # Emit discovery metadata (reuse vs download) for observability
    logger.info("MPFS discovery completed",
        reuse_count=sum(1 for f in source_files if f.is_reuse),
        download_count=sum(1 for f in source_files if not f.is_reuse)
    )
    
    return source_files
```

**2.2 Refactor `land_stage()`**
```python
async def land_stage(self, source_files: List[SourceFile]) -> RawBatch:
    """
    Land stage: Record metadata for snapshot-backed entries, download CF.
    """
    raw_batch = RawBatch(
        batch_id=str(uuid.uuid4()),
        source_files=source_files,
        raw_data={},
        metadata={
            "ingestion_timestamp": datetime.now().isoformat(),
            "datasets": []  # Track dataset list for provenance
        }
    )
    
    for source_file in source_files:
        if source_file.is_reuse:
            # Snapshot-backed entry: record metadata only (no download)
            raw_batch.raw_data[source_file.filename] = {
                "path": source_file.path,
                "dataset_id": source_file.dataset_id,
                "release_id": source_file.release_id,
                "checksum": source_file.checksum,
                "is_reuse": True
            }
            raw_batch.metadata["datasets"].append({
                "dataset_id": source_file.dataset_id,
                "release_id": source_file.release_id,
                "source": "snapshot_reuse"
            })
        else:
            # CF entry: download (if needed) to raw dir
            file_path = Path(source_file.path)
            if not file_path.exists():
                # Download from URL
                async with httpx.AsyncClient() as client:
                    response = await client.get(source_file.url)
                    response.raise_for_status()
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
            
            # Calculate checksum
            checksum = hashlib.sha256(file_path.read_bytes()).hexdigest()
            
            raw_batch.raw_data[source_file.filename] = {
                "path": str(file_path),
                "dataset_id": source_file.dataset_id,
                "checksum": checksum,
                "size_bytes": file_path.stat().st_size,
                "is_reuse": False
            }
            raw_batch.metadata["datasets"].append({
                "dataset_id": source_file.dataset_id,
                "source": "download"
            })
    
    return raw_batch
```

### Phase 3 – Implement Validation Pipeline (1.5 days)
**Objective:** Replace placeholder validators with real validation logic.

**3.1 Structural Validation**
```python
async def _validate_structural(self, raw_batch: RawBatch) -> List[Dict[str, Any]]:
    """Structural validation: confirm datasets resolved, files exist."""
    results = []
    
    # Confirm RVU/GPCI snapshots resolved
    rvu_entry = next((f for f in raw_batch.source_files if f.dataset_id == "rvu_items"), None)
    gpci_entry = next((f for f in raw_batch.source_files if f.dataset_id == "gpci_indices"), None)
    cf_entry = next((f for f in raw_batch.source_files if f.dataset_id == "mpfs_cf"), None)
    
    if not rvu_entry:
        results.append({"rule_id": "rvu_snapshot_missing", "severity": "CRITICAL", ...})
    if not gpci_entry:
        results.append({"rule_id": "gpci_snapshot_missing", "severity": "CRITICAL", ...})
    if not cf_entry:
        results.append({"rule_id": "cf_file_missing", "severity": "CRITICAL", ...})
    
    # CF file exists, non-zero size
    if cf_entry and not cf_entry.is_reuse:
        cf_path = Path(raw_batch.raw_data[cf_entry.filename]["path"])
        if not cf_path.exists():
            results.append({"rule_id": "cf_file_not_found", "severity": "CRITICAL", ...})
        elif cf_path.stat().st_size == 0:
            results.append({"rule_id": "cf_file_empty", "severity": "CRITICAL", ...})
    
    return results
```

**3.2 Domain Validation**
```python
async def _validate_domain(self, normalized_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """Domain validation: HCPCS format, locality coverage, CF > 0, date ranges."""
    results = []
    
    # HCPCS format (5 characters)
    if "rvu" in normalized_data:
        invalid_hcpcs = normalized_data["rvu"][~normalized_data["rvu"]["hcpcs"].str.match(r'^[A-Z0-9]{5}$')]
        if len(invalid_hcpcs) > 0:
            results.append({"rule_id": "invalid_hcpcs_format", "severity": "ERROR", ...})
    
    # Locality coverage across RVU/GPCI
    rvu_localities = set(normalized_data.get("rvu", pd.DataFrame())["locality_id"].unique())
    gpci_localities = set(normalized_data.get("gpci", pd.DataFrame())["locality_id"].unique())
    missing = rvu_localities - gpci_localities
    if missing:
        results.append({"rule_id": "locality_mismatch", "severity": "WARNING", ...})
    
    # CF > 0
    if "cf" in normalized_data:
        invalid_cf = normalized_data["cf"][normalized_data["cf"]["cf_value"] <= 0]
        if len(invalid_cf) > 0:
            results.append({"rule_id": "invalid_cf_value", "severity": "ERROR", ...})
    
    # Date ranges non-overlapping
    # Implementation: check effective_from/effective_to ranges
    
    return results
```

**3.3 Statistical Validation**
```python
async def _validate_statistics(self, normalized_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """Statistical validation: compare vs previous snapshot."""
    results = []
    
    # Compare RVU row count with prior snapshot (<=15% variance)
    current_rvu_count = len(normalized_data.get("rvu", pd.DataFrame()))
    previous_snapshot = self.snapshot_service.select_snapshot("rvu_items", ...)
    if previous_snapshot:
        # Load previous count from metadata or query
        previous_count = self._get_previous_rvu_count(previous_snapshot)
        variance = abs(current_rvu_count - previous_count) / previous_count
        if variance > 0.15:
            results.append({"rule_id": "rvu_count_drift", "severity": "WARNING", ...})
    
    # CF vs previous year (alert if >10% shift)
    current_cf = normalized_data.get("cf", pd.DataFrame())["cf_value"].iloc[0]
    previous_cf = self._get_previous_cf_value(year=2024)
    if previous_cf:
        shift = abs(current_cf - previous_cf) / previous_cf
        if shift > 0.10:
            results.append({"rule_id": "cf_value_shift", "severity": "WARNING", ...})
    
    return results
```

**3.4 Replace Placeholder Validators**
Update `_create_validation_rules()` to use real validator functions instead of `lambda x: True`.

### Phase 4 – Normalization Logic (2 days)
**Objective:** Parse inputs into canonical DataFrames.

**4.1 Implement Loader Methods**
```python
def _load_rvu_slice(self, release_id: str) -> pd.DataFrame:
    """Load curated RVU parquet via snapshot metadata."""
    snapshot = self.snapshot_service.get_snapshot_by_release("rvu_items", release_id)
    if not snapshot:
        raise ValueError(f"RVU snapshot not found: {release_id}")
    
    # Resolve curated parquet path
    curated_path = self._resolve_curated_path(snapshot)
    
    # Read parquet
    df = pd.read_parquet(curated_path)
    
    # Select required columns: hcpcs, work_rvu, pe_nf_rvu, pe_fac_rvu, mp_rvu, status_code, global_days
    required_cols = ["hcpcs", "work_rvu", "pe_nf_rvu", "pe_fac_rvu", "mp_rvu", "status_code", "global_days"]
    df = df[required_cols].copy()
    
    # Add metadata
    df["release_id"] = release_id
    df["dataset_digest"] = snapshot.digest
    
    return df

def _load_gpci_slice(self, release_id: str) -> pd.DataFrame:
    """Load curated GPCI parquet via snapshot metadata."""
    # Similar to _load_rvu_slice but for GPCI
    snapshot = self.snapshot_service.get_snapshot_by_release("gpci_indices", release_id)
    curated_path = self._resolve_curated_path(snapshot)
    df = pd.read_parquet(curated_path)
    # Select: locality_id, gpci_work, gpci_pe, gpci_mp
    df["release_id"] = release_id
    df["dataset_digest"] = snapshot.digest
    return df
```

**4.2 Implement CF Parser**
```python
def _parse_conversion_factor(self, file_path: Path) -> pd.DataFrame:
    """
    Parse conversion factor file (ZIP/XLSX/TXT).
    
    Returns:
        DataFrame with columns: year, cf_value, effective_from, effective_to, source_url, checksum
    """
    # Support multiple formats
    if file_path.suffix == ".zip":
        # Extract and find CF file inside
        with zipfile.ZipFile(file_path) as z:
            # Find CF file (e.g., "Conversion Factor.xlsx")
            cf_file = next((f for f in z.namelist() if "conversion" in f.lower()), None)
            if cf_file:
                with z.open(cf_file) as f:
                    df = pd.read_excel(f)
    elif file_path.suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    elif file_path.suffix == ".txt":
        df = pd.read_csv(file_path, sep="\t")
    else:
        raise ValueError(f"Unsupported CF file format: {file_path.suffix}")
    
    # Normalize to canonical columns
    column_mapping = {
        "Year": "year",
        "Conversion Factor": "cf_value",
        "CF": "cf_value",
        "Effective From": "effective_from",
        "Effective To": "effective_to"
    }
    df = df.rename(columns=column_mapping)
    
    # Enforce dtypes
    df["year"] = df["year"].astype(int)
    df["cf_value"] = pd.to_numeric(df["cf_value"], errors='coerce')
    df["effective_from"] = pd.to_datetime(df["effective_from"]).dt.date
    df["effective_to"] = pd.to_datetime(df["effective_to"], errors='coerce').dt.date
    
    # Add metadata
    df["source_url"] = str(file_path)
    df["checksum"] = hashlib.sha256(file_path.read_bytes()).hexdigest()
    
    return df
```

**4.3 Normalize Stage Implementation**
```python
async def normalize_stage(self, validated_batch: RawBatch) -> AdaptedBatch:
    """Normalize stage: build canonical DataFrames."""
    normalized_data = {}
    
    # Load RVU slice
    rvu_entry = next((f for f in validated_batch.source_files if f.dataset_id == "rvu_items"), None)
    if rvu_entry:
        normalized_data["rvu"] = self._load_rvu_slice(rvu_entry.release_id)
    
    # Load GPCI slice
    gpci_entry = next((f for f in validated_batch.source_files if f.dataset_id == "gpci_indices"), None)
    if gpci_entry:
        normalized_data["gpci"] = self._load_gpci_slice(gpci_entry.release_id)
    
    # Parse CF
    cf_entry = next((f for f in validated_batch.source_files if f.dataset_id == "mpfs_cf"), None)
    if cf_entry:
        cf_path = Path(validated_batch.raw_data[cf_entry.filename]["path"])
        normalized_data["cf"] = self._parse_conversion_factor(cf_path)
    
    # Normalize to canonical columns (lower snake_case, typed)
    for table_name, df in normalized_data.items():
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        # Enforce dtypes per schema
    
    return AdaptedBatch(
        batch_id=validated_batch.batch_id,
        adapted_data=normalized_data,
        metadata=validated_batch.metadata
    )
```

### Phase 5 – Enrichment & Curated Views (3.5 days)
**Objective:** Materialize MPFS datasets consumed by API.

**5.1 Create Builder Module**
Create new file `cms_pricing/ingestion/datasets/mpfs_builder.py`:

```python
class MPFSBuilder:
    """
    Builds curated MPFS datasets from normalized RVU, GPCI, and CF inputs.
    
    Computes payments using CMS formula and produces all required curated views.
    """
    
    def build_curated_datasets(
        self,
        rvu_df: pd.DataFrame,
        gpci_df: pd.DataFrame,
        cf_df: pd.DataFrame,
        release_id: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Build all curated MPFS datasets.
        
        Returns:
            Dict with keys: mpfs_rvu, mpfs_gpci, mpfs_cf_vintage, mpfs_payment_curated,
            mpfs_indicators_all, mpfs_link_keys, mpfs_locality
        """
        curated = {}
        
        # Join RVU + GPCI + CF
        joined = rvu_df.merge(gpci_df, on="locality_id", how="inner")
        joined = joined.merge(cf_df, on="year", how="inner")
        
        # Compute payments
        joined["facility_amount"] = (
            joined["work_rvu"] * joined["gpci_work"] +
            joined["pe_fac_rvu"] * joined["gpci_pe"] +
            joined["mp_rvu"] * joined["gpci_mp"]
        ) * joined["cf_value"]
        
        joined["non_facility_amount"] = (
            joined["work_rvu"] * joined["gpci_work"] +
            joined["pe_nf_rvu"] * joined["gpci_pe"] +
            joined["mp_rvu"] * joined["gpci_mp"]
        ) * joined["cf_value"]
        
        # mpfs_payment_curated: Full payment table
        curated["mpfs_payment_curated"] = joined[[
            "hcpcs", "locality_id", "facility_amount", "non_facility_amount",
            "status_code", "global_days", "effective_from", "effective_to",
            "release_id"
        ]].copy()
        
        # mpfs_rvu: Core RVU data
        curated["mpfs_rvu"] = rvu_df.copy()
        
        # mpfs_gpci: GPCI indices
        curated["mpfs_gpci"] = gpci_df.copy()
        
        # mpfs_cf_vintage: Conversion factors
        curated["mpfs_cf_vintage"] = cf_df.copy()
        
        # mpfs_indicators_all: Exploded policy flags
        curated["mpfs_indicators_all"] = self._build_indicators_table(joined)
        
        # mpfs_link_keys: Minimal join keys
        curated["mpfs_link_keys"] = joined[[
            "hcpcs", "locality_id", "site_of_service"
        ]].drop_duplicates().copy()
        
        # mpfs_locality: Locality dimension (from locality reference)
        curated["mpfs_locality"] = self._build_locality_table(gpci_df)
        
        return curated
    
    def _build_indicators_table(self, joined_df: pd.DataFrame) -> pd.DataFrame:
        """Build exploded indicators table from status codes."""
        # Implementation: explode status_code into individual indicator rows
        pass
    
    def _build_locality_table(self, gpci_df: pd.DataFrame) -> pd.DataFrame:
        """Build locality dimension table."""
        # Join with locality county reference data
        pass
```

**5.2 Implement Enrich Stage**
```python
async def enrich_stage(self, adapted_batch: AdaptedBatch) -> StageFrame:
    """Enrich stage: call builder, attach observability metrics."""
    from cms_pricing.ingestion.datasets.mpfs_builder import MPFSBuilder
    
    builder = MPFSBuilder()
    
    # Build curated datasets
    curated_datasets = builder.build_curated_datasets(
        rvu_df=adapted_batch.adapted_data["rvu"],
        gpci_df=adapted_batch.adapted_data["gpci"],
        cf_df=adapted_batch.adapted_data["cf"],
        release_id=self.current_release_id
    )
    
    # Integrate locality info (via locality reference data)
    # This is handled in builder._build_locality_table()
    
    # Compose observability metrics
    metrics = {
        "rows": {name: len(df) for name, df in curated_datasets.items()},
        "coverage_by_locality": self._calculate_locality_coverage(curated_datasets["mpfs_payment_curated"])
    }
    
    return StageFrame(
        batch_id=adapted_batch.batch_id,
        release_id=self.current_release_id,
        enriched_data=curated_datasets,
        metadata={**adapted_batch.metadata, "observability": metrics}
    )
```

**5.3 Implement Publish Stage**
```python
async def publish_stage(self, stage_frame: StageFrame) -> Dict[str, Any]:
    """Publish stage: write curated parquet, register snapshots."""
    curated_dir = Path(self.output_dir) / "curated" / "mpfs" / self.current_release_id
    curated_dir.mkdir(parents=True, exist_ok=True)
    
    curated_views = {}
    
    # Write curated parquet
    for table_name, df in stage_frame.enriched_data.items():
        parquet_path = curated_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, compression="snappy", index=False)
        
        # Calculate digest
        digest = self._calculate_dataset_digest(df)
        
        # Register snapshot
        self.snapshot_service.register_snapshot(
            dataset_id=table_name,
            release_id=self.current_release_id,
            digest=digest,
            effective_from=df["effective_from"].min(),
            effective_to=df["effective_to"].max() if "effective_to" in df.columns else None,
            manifest_url=str(curated_dir / "manifest.json"),
            curated_path=str(parquet_path)
        )
        
        curated_views[table_name] = {
            "row_count": len(df),
            "path": str(parquet_path),
            "digest": digest
        }
    
    # Optional: Load relational tables (fee_mpfs_*)
    if self.db_session:
        await self._load_relational_tables(stage_frame.enriched_data)
    
    return {
        "status": "success",
        "release_id": self.current_release_id,
        "batch_id": stage_frame.batch_id,
        "curated_views": curated_views
    }
```

### Phase 6 – Testing & QA (1.5 days)
**Objective:** Comprehensive test coverage with mocked services and golden comparisons.

**6.1 Update E2E Tests**
Update `tests/ingestors/test_mpfs_ingestor_e2e.py`:

```python
@pytest.fixture
def mock_snapshot_service():
    """Mock DatasetSnapshotService."""
    service = Mock(spec=DatasetSnapshotService)
    service.get_latest_snapshot.return_value = {
        "path": "data/curated/rvu/rvu_2025_D/pprrvu.parquet",
        "release_id": "rvu_2025_D",
        "checksum": "abc123",
        "effective_from": date(2025, 1, 1),
        "effective_to": date(2025, 12, 31)
    }
    return service

@pytest.fixture
def mock_cf_fetcher():
    """Mock ConversionFactorFetcher."""
    fetcher = Mock(spec=ConversionFactorFetcher)
    fetcher.ensure_conversion_factor.return_value = {
        "path": "data/ingestion/mpfs/raw/2025/cf.xlsx",
        "checksum": "def456",
        "effective_from": date(2025, 1, 1),
        "effective_to": date(2025, 12, 31),
        "source_url": "https://cms.gov/..."
    }
    return fetcher

@pytest.mark.asyncio
async def test_mpfs_discovery_uses_snapshots(mock_snapshot_service, mock_cf_fetcher):
    """Verify discovery returns expected dataset_ids."""
    ingestor = MPFSIngestor(
        snapshot_service=mock_snapshot_service,
        cf_fetcher=mock_cf_fetcher
    )
    
    source_files = await ingestor.discover_source_files()
    
    dataset_ids = [f.dataset_id for f in source_files]
    assert "rvu_items" in dataset_ids
    assert "gpci_indices" in dataset_ids
    assert "mpfs_cf" in dataset_ids

@pytest.mark.asyncio
async def test_mpfs_e2e_curated_tables(mock_snapshot_service, mock_cf_fetcher):
    """Run end-to-end on fixture data; assert curated tables non-empty."""
    # Use fixture RVU/GPCI/CF data
    # Run ingestion
    # Assert curated tables non-empty, payment columns correct
    pass

@pytest.mark.asyncio
async def test_mpfs_golden_comparison():
    """Golden comparison vs stored PFREV sample (assert within $0.01)."""
    # Load stored PFREV sample from tests/fixtures/mpfs/pfrev_sample.json
    # Run ingestion
    # Compare computed payments vs PFREV amounts
    # Assert difference <= $0.01
    pass
```

**6.2 Add Unit Tests for ConversionFactorFetcher**
Create `tests/ingestion/services/test_conversion_factor_fetcher.py`:

```python
class TestConversionFactorFetcher:
    def test_ensure_cf_downloads_when_missing(self):
        """Test CF fetcher downloads file when not cached."""
        pass
    
    def test_ensure_cf_uses_cache_when_exists(self):
        """Test CF fetcher uses cached file when available."""
        pass
    
    def test_ensure_cf_manual_override(self):
        """Test CF fetcher respects manual override path."""
        pass
```

**6.3 Extend Contract Tests**
Update `tests/contracts/test_code_pricing_item.py`:

```python
def test_mpfs_endpoint_includes_datasets_used():
    """Verify /v1/mpfs includes new datasets in datasets_used."""
    response = client.get("/v1/mpfs?code=99213&year=2025&locality=CA01")
    assert response.status_code == 200
    
    data = response.json()
    datasets_used = data.get("datasets_used", [])
    dataset_ids = [d["dataset_id"] for d in datasets_used]
    
    assert "mpfs_cf" in dataset_ids
    assert "mpfs_rvu" in dataset_ids
    assert "mpfs_gpci" in dataset_ids
```

**6.4 Optional Integration Test**
```python
@pytest.mark.integration
async def test_mpfs_snapshot_reuse_and_cf_download():
    """Integration test simulating snapshot reuse + CF download."""
    # Use real snapshot service (with test DB)
    # Mock CF download
    # Run full pipeline
    # Verify snapshot reuse works correctly
    pass
```

**6.5 Performance Smoke Test**
Optional: Add Locust/synthetic load test verifying:
- Ingestion completes < 30 minutes
- Results accessible under SLO (< 500ms API response)

### Phase 7 – Documentation & Ops Updates (1 day)
**Objective:** Update all documentation to reflect new implementation.

**7.1 Update Implementation Plan**
- Mark completed phases in `artifacts/mpfs_implementation_plan.md`
- Document any deviations or learnings

**7.2 Update Runbook**
Expand `artifacts/mpfs_opps_ingestion_runbook.md` with:
- Operational steps (trigger command, expected log lines, validation thresholds)
- Example commands for running ingestion
- Troubleshooting guide for common issues

**7.3 Update PRDs**
- **`prds/REF-cms-pricing-source-map-prd-v1.0.md`**: Describe new curated tables (`mpfs_rvu`, `mpfs_cf`, `mpfs_payment_curated`, etc.) and link to CF artifact list
- **`prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md`**: Mark MPFS ingestion gating criteria (run evidence, provenance, test results)
- **`artifacts/ingestor_gap_analysis.md`**: Refresh MPFS status to "✅ Complete"

**7.4 Release Notes**
- Add entry to `docs/release_notes/phase2_refactor.md` summarizing:
  - MPFS ingestion completion
  - Scraper deprecation (removed `CMSMPFSScraper`)
  - New snapshot-based discovery model
  - New curated datasets available

### Phase 8 – Verification & Handoff (0.5 day)
**Objective:** Execute production run and coordinate handoff.

**8.1 Execute Production Run**
- Run MPFS ingestion on latest production vintage (2025D)
- Capture artifacts:
  - Manifest path
  - Curated table counts
  - Sample `datasets_used` from API response
  - Validation metrics

**8.2 Coordinate Testing**
- Coordinate with API/QA to run end-to-end regression suite
- Verify `/v1/mpfs` endpoints return correct data
- Confirm provenance metadata in responses

**8.3 Document Results**
- Document results in readiness checklist
- Update `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md` with run evidence
- Schedule knowledge transfer with on-call/support

---

## 2A. Execution Order (Recommended Sequence)

**This prioritizes replacing the scraper first so subsequent stages build on the new discovery model:**

1. **Introduce ConversionFactorFetcher + snapshot helpers** (Phase 1.3-1.4)
   - Create `ConversionFactorFetcher` service
   - Extend `DatasetSnapshotService` with `get_latest_snapshot()` helper
   - Add unit tests for new services

2. **Refactor MPFSIngestor discovery/land** (Phase 2)
   - Update `discover_source_files()` to use snapshot service and CF fetcher
   - Update `land_stage()` to handle snapshot reuse vs download
   - Update tests with mocked services

3. **Remove old scraper code** (Phase 1.1-1.2)
   - Delete `cms_mpfs_scraper.py` and all imports
   - Update ingestor constructor
   - Clean up documentation references

4. **Implement validation/normalize/enrich/publish logic** (Phases 3-5)
   - Replace placeholder validators with real logic
   - Implement normalization methods (`_load_rvu_slice`, `_load_gpci_slice`, `_parse_conversion_factor`)
   - Create `mpfs_builder.py` module
   - Implement enrichment and publish stages

5. **Add curated snapshot registration + provenance wiring** (Phase 5.3)
   - Register snapshots for each curated dataset
   - Wire provenance metadata throughout pipeline

6. **Update tests and fixtures** (Phase 6)
   - Update E2E tests with mocked services
   - Add golden comparison tests
   - Extend contract tests
   - Remove old scraper test code

7. **Run MPFS ingestion locally** (Phase 8)
   - Execute with latest vintage
   - Verify outputs match expectations
   - Update documentation

This sequence ensures:
- New services are tested in isolation first
- Discovery/land refactoring is validated before other stages
- Old scraper code is removed only after new model is working
- Full pipeline implementation builds on validated foundation
- Tests and docs are updated as implementation progresses

---

## 3. File & Module Impact Matrix

| Area | Files / Modules | Lines Changed (Est.) |
|------|-----------------|---------------------|
| Ingestor orchestration | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | ~500 lines (refactor + new methods) |
| Source discovery | `cms_pricing/ingestion/services/dataset_snapshot_service.py` (extend), *(new)* `cms_pricing/ingestion/services/conversion_factor_fetcher.py` | ~150 lines (new), ~50 lines (extend) |
| Snapshot service | `cms_pricing/ingestion/services/dataset_snapshot_service.py` | ~50 lines (`get_latest_snapshot` helper) |
| Curated view builder | *(new)* `cms_pricing/ingestion/datasets/mpfs_builder.py` | ~300 lines (full builder module) |
| Tests | `tests/ingestors/test_mpfs_ingestor_e2e.py`, `tests/contracts/test_code_pricing_item.py`, *(new)* `tests/ingestion/services/test_conversion_factor_fetcher.py`, optional new `tests/fixtures/mpfs/` | ~200 lines (update + new) |
| Docs/PRDs | `prds/REF-cms-pricing-source-map-prd-v1.0.md`, `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md`, `artifacts/ingestor_gap_analysis.md`, `artifacts/mpfs_opps_ingestion_runbook.md` | Various updates |
| **Files to Delete** | `cms_pricing/ingestion/scrapers/cms_mpfs_scraper.py` | Remove ~380 lines |

**Total New Code:** ~700 lines | **Total Modified Code:** ~550 lines | **Total Deleted Code:** ~380 lines

**Reference Implementation:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (1237 lines) - use as pattern for structure

---

## 4. Delivery Checklist

### Phase Completion
- [ ] Phase 0: Prerequisites & alignment (vintage targets confirmed)
- [ ] Phase 1: Scraper retired, services created (`get_latest_snapshot`, `ConversionFactorFetcher`)
- [ ] Phase 2: Discovery & land stages refactored (snapshot reuse working)
- [ ] Phase 3: Validation pipeline implemented (structural, domain, statistical)
- [ ] Phase 4: Normalization logic complete (`_load_rvu_slice`, `_load_gpci_slice`, `_parse_conversion_factor`)
- [ ] Phase 5: Enrichment & curated views (builder module, publish stage)
- [ ] Phase 6: Tests passing (unit, golden, contract)
- [ ] Phase 7: Documentation updated (PRDs, runbook, release notes)
- [ ] Phase 8: Production run executed, handoff completed

### Success Criteria
- [ ] MPFS scraper reuses RVU/GPCI snapshots and captures CF artifact manifest
- [ ] Normalize/enrich/publish stages output non-empty curated tables
- [ ] Dataset snapshots registered for MPFS family (`mpfs_rvu`, `mpfs_cf`, `mpfs_payment`)
- [ ] Tests passing (unit, golden, contract)
- [ ] Ingestion run executed on latest vintage; metrics archived
- [ ] Docs/PRDs updated; readiness plan references ingestion evidence
- [ ] `/v1/mpfs` endpoint returns data with correct `datasets_used` metadata

---

## 5. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| CF artifact format changes (XLSX → PDF) | Parsing failure | Keep parsing utility resilient (ZIP/XLSX/TXT), raise alert if unsupported | 
| Locality misalignment (missing GPCI locality) | incorrect payments | Add validation comparing locality set vs RVU locality table; quarantine unknown localities |
| Multiple CF updates per year | incorrect vintage selection | Include effective_from/effective_to metadata and choose by valuation date; allow manual override |
| National payment diff > tolerance | regression | Compare vs CMS PFREV; if diff > tolerance, flag in QA dashboard before release |

---

## 6. Owner & Contacts

- **Implementation Owner:** Data Engineering (MPFS squad)  
- **Reviewers:** Platform API, QA, Compliance  
- **Support Docs:** `artifacts/mpfs_opps_ingestion_runbook.md`, `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md`

---

## 7. Common Pitfalls & Quick Fixes

| Pitfall | Symptom | Quick Fix |
|---------|---------|-----------|
| **RVU snapshot not found** | `get_latest_snapshot("rvu_items")` returns `None` | Verify RVU ingestor has run successfully; check `dataset_snapshots` table |
| **SourceFile missing `dataset_id`** | Type errors in discovery stage | Add `dataset_id` field to `SourceFile` dataclass in `ingestor_spec.py` |
| **CF file format mismatch** | Parser fails on unexpected format | Add fallback parsing (try XLSX, then CSV, then TXT) with clear error messages |
| **Locality join fails** | Missing `locality_id` in RVU data | Verify RVU snapshot includes locality columns; check join key alignment |
| **Snapshot path resolution** | `_resolve_curated_path()` returns None | Use manifest_url from snapshot or construct path: `data/curated/{dataset}/{release_id}/*.parquet` |
| **Validation rules too strict** | False positives block ingestion | Start with warnings, escalate to errors only for critical issues |
| **Golden test tolerance too tight** | Tests fail on rounding differences | Use `$0.01` tolerance (1 cent) for payment comparisons |
| **DB session not passed** | `db_session is None` errors | Ensure `MPFSIngestor` receives `db_session` from `SessionLocal()` |

**Reference:** See `cms_pricing/ingestion/ingestors/rvu_ingestor.py` for working patterns (lines 30-150 for discovery, 200-300 for normalization).

**Troubleshooting Quick Commands:**
```bash
# Check if snapshots exist
python -c "from cms_pricing.database import SessionLocal; from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService; db = SessionLocal(); svc = DatasetSnapshotService(db); print('RVU:', svc.get_latest_snapshot('rvu_items')); print('GPCI:', svc.get_latest_snapshot('gpci_indices')); db.close()"

# Verify SourceFile dataclass has dataset_id field
python -c "from cms_pricing.ingestion.contracts.ingestor_spec import SourceFile; import inspect; print([f.name for f in inspect.fields(SourceFile)])"

# Check curated parquet paths
find data/curated -name "*.parquet" -type f | head -5

# Verify test fixtures
ls -la tests/fixtures/mpfs/ 2>/dev/null || echo "⚠️ Fixtures directory not created yet"
```

--- 
