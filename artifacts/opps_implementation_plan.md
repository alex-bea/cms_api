# OPPS Ingestor Implementation Plan

**Date:** 2025-01-15  
**Status:** Draft v1.0  
**Priority:** 🔴 Critical  
**Estimated Time:** 2-3 weeks

---

## Overview

This document provides detailed step-by-step implementation plan for completing the OPPS ingestor. The ingestor has scaffold code but needs parsing implementations for Addendum A/B, wage index enrichment, and database publishing.

**Current File:** `cms_pricing/ingestion/ingestors/opps_ingestor.py` (1022 lines)

---

## Phase 1: Parsing Implementation

### Step 1.1: Implement Addendum A Parsing (APC Payment Rates)

**Location:** `opps_ingestor.py:782-790` (`_parse_addendum_a`)

**Current State:**
```python
async def _parse_addendum_a(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
    # For now, return a placeholder
    return pd.DataFrame()
```

**Implementation:**
```python
async def _parse_addendum_a(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
    """Parse Addendum A (APC Payment Rates)"""
    logger.info("Parsing Addendum A", file=file_info.filename)
    
    file_path = Path(file_info.local_path)
    
    # OPPS files are typically Section 508 CSV or XLSX
    if file_path.suffix == ".csv":
        df = pd.read_csv(file_path, encoding='utf-8')
    elif file_path.suffix in [".xlsx", ".xls"]:
        # OPPS XLSX files may have multiple sheets
        # Addendum A is typically the first sheet
        df = pd.read_excel(file_path, sheet_name=0)
    elif file_path.suffix == ".txt":
        # Fixed-width format (if provided)
        df = self._parse_fixed_width_addendum_a(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    # Normalize column names
    df = self._normalize_addendum_a_columns(df)
    
    # Validate required columns
    required_columns = ["apc", "relative_weight", "national_unadj_rate"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Addendum A: {missing}")
    
    # Add metadata
    df['year'] = file_info.metadata.get('year')
    df['quarter'] = file_info.metadata.get('quarter')
    df['effective_from'] = self._calculate_effective_from(
        file_info.metadata.get('year'),
        file_info.metadata.get('quarter')
    )
    df['effective_to'] = self._calculate_effective_to(
        file_info.metadata.get('year'),
        file_info.metadata.get('quarter')
    )
    
    logger.info("Addendum A parsed", rows=len(df))
    return df

def _normalize_addendum_a_columns(self, df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Addendum A column names to canonical schema"""
    column_mapping = {
        "APC": "apc",
        "APC Code": "apc",
        "Relative Weight": "relative_weight",
        "RelativeWeight": "relative_weight",
        "National Unadjusted Payment Rate": "national_unadj_rate",
        "NationalUnadjRate": "national_unadj_rate",
        "Payment Rate": "national_unadj_rate"
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ensure numeric columns are numeric
    if "relative_weight" in df.columns:
        df["relative_weight"] = pd.to_numeric(df["relative_weight"], errors='coerce')
    if "national_unadj_rate" in df.columns:
        df["national_unadj_rate"] = pd.to_numeric(df["national_unadj_rate"], errors='coerce')
    
    return df
```

**Files to Create/Modify:**
- `cms_pricing/ingestion/ingestors/opps_ingestor.py` - Implement parsing method

**Testing:**
- Test with OPPS Addendum A CSV files (download from CMS)
- Test with OPPS Addendum A XLSX files
- Verify column normalization works correctly

---

### Step 1.2: Implement Addendum B Parsing (HCPCS Crosswalk)

**Location:** `opps_ingestor.py:791-799` (`_parse_addendum_b`)

**Current State:**
```python
async def _parse_addendum_b(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
    # For now, return a placeholder
    return pd.DataFrame()
```

**Implementation:**
```python
async def _parse_addendum_b(self, file_info: ScrapedFileInfo) -> pd.DataFrame:
    """Parse Addendum B (HCPCS → APC Crosswalk)"""
    logger.info("Parsing Addendum B", file=file_info.filename)
    
    file_path = Path(file_info.local_path)
    
    # OPPS Addendum B is typically Section 508 CSV or XLSX
    if file_path.suffix == ".csv":
        df = pd.read_csv(file_path, encoding='utf-8')
    elif file_path.suffix in [".xlsx", ".xls"]:
        # Addendum B may be in a specific sheet
        # Try common sheet names: "Addendum B", "HCPCS", "Sheet1"
        sheet_name = None
        for name in ["Addendum B", "HCPCS", "Sheet1"]:
            try:
                df = pd.read_excel(file_path, sheet_name=name)
                break
            except ValueError:
                continue
        else:
            # Default to first sheet
            df = pd.read_excel(file_path, sheet_name=0)
    elif file_path.suffix == ".txt":
        df = self._parse_fixed_width_addendum_b(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    # Normalize column names
    df = self._normalize_addendum_b_columns(df)
    
    # Validate required columns
    required_columns = ["hcpcs", "status_indicator", "apc"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Addendum B: {missing}")
    
    # Add metadata
    df['year'] = file_info.metadata.get('year')
    df['quarter'] = file_info.metadata.get('quarter')
    df['effective_from'] = self._calculate_effective_from(
        file_info.metadata.get('year'),
        file_info.metadata.get('quarter')
    )
    df['effective_to'] = self._calculate_effective_to(
        file_info.metadata.get('year'),
        file_info.metadata.get('quarter')
    )
    
    logger.info("Addendum B parsed", rows=len(df))
    return df

def _normalize_addendum_b_columns(self, df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Addendum B column names to canonical schema"""
    column_mapping = {
        "HCPCS": "hcpcs",
        "HCPCS Code": "hcpcs",
        "Code": "hcpcs",
        "Modifier": "modifier",
        "Status Indicator": "status_indicator",
        "StatusIndicator": "status_indicator",
        "SI": "status_indicator",
        "APC": "apc",
        "APC Code": "apc"
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ensure HCPCS codes are strings and 5 characters
    if "hcpcs" in df.columns:
        df["hcpcs"] = df["hcpcs"].astype(str).str.strip().str[:5]
    
    # Ensure status indicator is string
    if "status_indicator" in df.columns:
        df["status_indicator"] = df["status_indicator"].astype(str).str.strip()
    
    return df
```

**Testing:**
- Test with OPPS Addendum B CSV files
- Test with OPPS Addendum B XLSX files
- Verify HCPCS → APC mapping is correct

---

### Step 1.3: Implement ZIP File Parsing

**Location:** `opps_ingestor.py:800-808` (`_parse_zip_file`)

**Current State:**
```python
async def _parse_zip_file(self, file_info: ScrapedFileInfo) -> Dict[str, pd.DataFrame]:
    # For now, return a placeholder
    return {}
```

**Implementation:**
```python
async def _parse_zip_file(self, file_info: ScrapedFileInfo) -> Dict[str, pd.DataFrame]:
    """Parse ZIP file containing multiple addenda"""
    logger.info("Parsing OPPS ZIP file", file=file_info.filename)
    
    file_path = Path(file_info.local_path)
    parsed_data = {}
    
    with zipfile.ZipFile(file_path, 'r') as z:
        # List files in ZIP
        file_list = z.namelist()
        
        for zip_filename in file_list:
            # Determine file type from name
            if "addendum_a" in zip_filename.lower() or "apc" in zip_filename.lower():
                # Extract and parse Addendum A
                with z.open(zip_filename) as f:
                    # Write to temp file
                    temp_path = Path(self.stage_dir) / zip_filename
                    temp_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(temp_path, 'wb') as out:
                        out.write(f.read())
                    
                    # Parse as Addendum A
                    temp_file_info = ScrapedFileInfo(
                        filename=zip_filename,
                        url=file_info.url,
                        file_type="addendum_a",
                        metadata=file_info.metadata
                    )
                    temp_file_info.local_path = str(temp_path)
                    df = await self._parse_addendum_a(temp_file_info)
                    parsed_data["apc_payment"] = df
            
            elif "addendum_b" in zip_filename.lower() or "hcpcs" in zip_filename.lower():
                # Extract and parse Addendum B
                with z.open(zip_filename) as f:
                    temp_path = Path(self.stage_dir) / zip_filename
                    temp_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(temp_path, 'wb') as out:
                        out.write(f.read())
                    
                    temp_file_info = ScrapedFileInfo(
                        filename=zip_filename,
                        url=file_info.url,
                        file_type="addendum_b",
                        metadata=file_info.metadata
                    )
                    temp_file_info.local_path = str(temp_path)
                    df = await self._parse_addendum_b(temp_file_info)
                    parsed_data["hcpcs_crosswalk"] = df
    
    logger.info("ZIP file parsed", tables=list(parsed_data.keys()))
    return parsed_data
```

**Testing:**
- Test with OPPS ZIP files containing multiple addenda
- Verify all addenda are extracted and parsed correctly

---

## Phase 2: Enrichment Implementation

### Step 2.1: Implement Wage Index Enrichment

**Location:** `opps_ingestor.py:601-631` (`_enrich_stage`)

**Current State:** Returns same data without enrichment

**Implementation:**
```python
async def _enrich_with_wage_index(
    self, 
    apc_payment_df: pd.DataFrame, 
    wage_index_data: pd.DataFrame
) -> pd.DataFrame:
    """Enrich APC payment rates with wage index"""
    logger.info("Enriching with wage index", rows=len(apc_payment_df))
    
    # Create enriched DataFrame
    enriched_df = apc_payment_df.copy()
    
    # For now, add wage_index column (will be joined with facility data later)
    # Wage index is facility-specific (CCN → CBSA → wage_index)
    # This is a simplified version - full implementation requires facility CCN
    
    # Add wage_index column (default to 1.0 for national rate)
    enriched_df['wage_index'] = 1.0
    
    # If we have facility-specific data, join with wage_index_data
    # For now, this is a placeholder for future enhancement
    
    logger.info("Wage index enrichment completed", rows=len(enriched_df))
    return enriched_df

async def _load_wage_index_data(self) -> pd.DataFrame:
    """Load wage index reference data"""
    # Wage index data comes from IPPS annual releases
    # For now, load from database if available, or return empty DataFrame
    
    if self.db_session:
        from cms_pricing.models.fee_schedules import WageIndex
        from sqlalchemy import select
        
        # Query latest wage index data
        query = select(WageIndex).order_by(WageIndex.year.desc(), WageIndex.effective_from.desc())
        result = self.db_session.execute(query)
        wage_index_records = result.scalars().all()
        
        if wage_index_records:
            # Convert to DataFrame
            wage_data = []
            for record in wage_index_records:
                wage_data.append({
                    "cbsa": record.cbsa,
                    "wage_index": record.wage_index,
                    "year": record.year,
                    "effective_from": record.effective_from,
                    "effective_to": record.effective_to
                })
            return pd.DataFrame(wage_data)
    
    # Return empty DataFrame if no wage index data available
    logger.warning("No wage index data available")
    return pd.DataFrame()
```

**Testing:**
- Test wage index enrichment with sample data
- Verify wage-adjusted rates are calculated correctly

---

### Step 2.2: Implement SI Lookup Enrichment

**Location:** `opps_ingestor.py:601-631` (`_enrich_stage`)

**Implementation:**
```python
async def _enrich_with_si_lookup(
    self, 
    hcpcs_crosswalk_df: pd.DataFrame, 
    si_lookup_data: pd.DataFrame
) -> pd.DataFrame:
    """Enrich HCPCS crosswalk with SI lookup data"""
    logger.info("Enriching with SI lookup", rows=len(hcpcs_crosswalk_df))
    
    # Create enriched DataFrame
    enriched_df = hcpcs_crosswalk_df.copy()
    
    # Join with SI lookup data
    if not si_lookup_data.empty:
        enriched_df = enriched_df.merge(
            si_lookup_data,
            on="status_indicator",
            how="left",
            suffixes=("", "_si")
        )
    
    logger.info("SI lookup enrichment completed", rows=len(enriched_df))
    return enriched_df

async def _load_si_lookup_data(self) -> pd.DataFrame:
    """Load status indicator lookup data"""
    # SI lookup data comes from OPPS I/OCE notes or reference tables
    # For now, use hardcoded SI definitions or load from database
    
    si_definitions = {
        "A": {"description": "Not payable by Medicare", "payment_type": "none"},
        "B": {"description": "Bundled", "payment_type": "bundled"},
        "C": {"description": "Inpatient only", "payment_type": "none"},
        "E": {"description": "Not valid for OPPS", "payment_type": "none"},
        "F": {"description": "Corneal tissue acquisition", "payment_type": "separate"},
        "G": {"description": "Pass-through drugs and biologicals", "payment_type": "separate"},
        "H": {"description": "Pass-through device", "payment_type": "separate"},
        "J": {"description": "Hospital Part B services", "payment_type": "separate"},
        "K": {"description": "Non-pass-through drugs", "payment_type": "separate"},
        "L": {"description": "Influenza vaccine", "payment_type": "separate"},
        "M": {"description": "Service not billable to MAC", "payment_type": "none"},
        "N": {"description": "Packaged", "payment_type": "packaged"},
        "P": {"description": "Partial hospitalization", "payment_type": "separate"},
        "Q": {"description": "Conditional packaging", "payment_type": "conditional"},
        "R": {"description": "Blood and blood products", "payment_type": "separate"},
        "S": {"description": "Not paid separately", "payment_type": "none"},
        "T": {"description": "Pass-through device category", "payment_type": "separate"},
        "U": {"description": "Brachytherapy", "payment_type": "separate"},
        "V": {"description": "Clinic or emergency department visit", "payment_type": "separate"},
        "X": {"description": "Ancillary", "payment_type": "separate"}
    }
    
    # Convert to DataFrame
    si_data = []
    for si_code, definition in si_definitions.items():
        si_data.append({
            "status_indicator": si_code,
            "description": definition["description"],
            "payment_type": definition["payment_type"]
        })
    
    return pd.DataFrame(si_data)
```

**Testing:**
- Test SI lookup enrichment with sample data
- Verify SI descriptions are added correctly

---

## Phase 3: Publishing Implementation

### Step 3.1: Implement Database Writes

**Location:** `opps_ingestor.py:632-660` (`_publish_stage`)

**Implementation:**
```python
async def _publish_stage(
    self, 
    enriched_data: Dict[str, pd.DataFrame], 
    batch_info: OPPSBatchInfo
) -> Dict[str, Any]:
    """Publish stage: Write to database and curated parquet"""
    logger.info("Starting publish stage", batch_id=batch_info.batch_id)
    
    # Calculate dataset digest
    dataset_digest = self._calculate_dataset_digest(enriched_data)
    
    # Write to database tables
    if self.db_session:
        await self._write_to_database(enriched_data, batch_info, dataset_digest)
    
    # Write curated parquet
    await self._write_curated_parquet(enriched_data, batch_info)
    
    # Register in dataset_snapshots
    await self._register_dataset_snapshot(dataset_digest, batch_info, enriched_data)
    
    logger.info("Publish stage completed", batch_id=batch_info.batch_id)
    return {
        "status": "success",
        "batch_id": batch_info.batch_id,
        "dataset_digest": dataset_digest,
        "tables_published": list(enriched_data.keys()),
        "records_published": sum(len(df) for df in enriched_data.values())
    }

async def _write_to_database(
    self, 
    enriched_data: Dict[str, pd.DataFrame], 
    batch_info: OPPSBatchInfo,
    dataset_digest: str
):
    """Write enriched data to database tables"""
    from cms_pricing.models.fee_schedules import FeeOPPS
    
    # Write HCPCS crosswalk to fee_opps
    if "hcpcs_crosswalk" in enriched_data:
        df = enriched_data["hcpcs_crosswalk"]
        
        for _, row in df.iterrows():
            fee_opps = FeeOPPS(
                year=int(row["year"]),
                quarter=str(row["quarter"]),
                hcpcs=str(row["hcpcs"]),
                status_indicator=str(row["status_indicator"]) if pd.notna(row["status_indicator"]) else None,
                apc=str(row["apc"]) if pd.notna(row["apc"]) else None,
                national_unadj_rate=float(row.get("national_unadj_rate")) if pd.notna(row.get("national_unadj_rate")) else None,
                packaging_flag=str(row.get("packaging_flag")) if pd.notna(row.get("packaging_flag")) else None,
                effective_from=row["effective_from"],
                effective_to=row.get("effective_to"),
                release_id=batch_info.batch_id,
                batch_id=batch_info.batch_id
            )
            self.db_session.add(fee_opps)
    
    # Write APC payment rates (if separate table needed)
    # For now, APC rates are in fee_opps via apc column
    
    self.db_session.commit()
```

**Testing:**
- Test database writes with sample data
- Verify provenance columns populated
- Verify no duplicate key violations

---

### Step 3.2: Implement Curated Parquet Output

```python
async def _write_curated_parquet(
    self, 
    enriched_data: Dict[str, pd.DataFrame], 
    batch_info: OPPSBatchInfo
):
    """Write curated data to parquet files"""
    curated_dir = Path(self.curated_dir) / batch_info.batch_id
    curated_dir.mkdir(parents=True, exist_ok=True)
    
    for table_name, df in enriched_data.items():
        parquet_path = curated_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, compression="snappy", index=False)
        logger.info("Wrote curated parquet", table=table_name, path=str(parquet_path))
```

**Testing:**
- Verify parquet files created
- Verify parquet files readable
- Verify data integrity

---

### Step 3.3: Implement Dataset Snapshot Registration

```python
async def _register_dataset_snapshot(
    self, 
    dataset_digest: str, 
    batch_info: OPPSBatchInfo,
    enriched_data: Dict[str, pd.DataFrame]
):
    """Register dataset snapshot in dataset_snapshots table"""
    from cms_pricing.models.snapshots import DatasetSnapshot
    
    snapshot = DatasetSnapshot(
        dataset_id="OPPS",
        release_id=batch_info.batch_id,
        digest=dataset_digest,
        effective_from=batch_info.effective_from,
        effective_to=batch_info.effective_to,
        manifest_url=f"{self.curated_dir}/{batch_info.batch_id}/manifest.json"
    )
    
    self.db_session.add(snapshot)
    self.db_session.commit()
```

**Testing:**
- Verify snapshot registered in dataset_snapshots table
- Verify snapshot queryable by API

---

## Phase 4: License Acceptance Automation (Optional)

### Step 4.1: Manual Workaround (Initial Implementation)

**Recommendation:** Start with manual download approach

**Process:**
1. Manually download OPPS files from CMS website
2. Accept AMA license terms in browser
3. Store downloaded files in `test_data/ingestion_2025/opps/`
4. Use local files for testing

**Files to Store:**
```
test_data/ingestion_2025/opps/
├── 2025q1/
│   ├── addendum_a.csv
│   └── addendum_b.csv
└── 2025q2/
    ├── addendum_a.csv
    └── addendum_b.csv
```

### Step 4.2: Automated License Acceptance (Future Enhancement)

**Implementation:**
```python
async def _accept_license_and_download(self, url: str) -> Path:
    """Accept AMA license and download file"""
    # Use Playwright or Selenium for headless browser
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to license page
        await page.goto(url)
        
        # Accept license (click accept button)
        await page.click('button:has-text("Accept")')
        
        # Wait for redirect to download
        await page.wait_for_url('**/*.zip', timeout=30000)
        
        # Download file
        async with page.expect_download() as download_info:
            await page.click('a:has-text("Download")')
        download = await download_info.value
        
        # Save to local path
        local_path = Path(self.raw_dir) / download.suggested_filename
        await download.save_as(local_path)
        
        await browser.close()
        return local_path
```

**Note:** This is a future enhancement. Start with manual approach.

---

## Phase 5: Testing & Validation

### Step 5.1: Unit Tests

**Create:** `tests/ingestors/test_opps_ingestor_parsing.py`

**Test Cases:**
- Addendum A parsing (CSV, XLSX)
- Addendum B parsing (CSV, XLSX)
- ZIP file parsing
- Column normalization
- Wage index enrichment
- SI lookup enrichment

### Step 5.2: Integration Tests

**Create:** `tests/ingestors/test_opps_ingestor_e2e.py`

**Test Cases:**
- Full pipeline with local test data
- Database writes
- Curated parquet generation
- Dataset snapshot registration

### Step 5.3: End-to-End Tests

**Test with real data:**
- Run with manually downloaded OPPS files
- Verify API endpoints return data
- Verify provenance metadata in responses

---

## Implementation Checklist

- [ ] Phase 1: Parsing Implementation
  - [ ] Implement Addendum A parsing
  - [ ] Implement Addendum B parsing
  - [ ] Implement ZIP file parsing
  - [ ] Unit tests for parsing
- [ ] Phase 2: Enrichment Implementation
  - [ ] Implement wage index enrichment
  - [ ] Implement SI lookup enrichment
  - [ ] Unit tests for enrichment
- [ ] Phase 3: Publishing Implementation
  - [ ] Implement database writes
  - [ ] Implement curated parquet output
  - [ ] Implement dataset snapshot registration
  - [ ] Integration tests
- [ ] Phase 4: License Acceptance (Optional)
  - [ ] Manual download workaround
  - [ ] Automated license acceptance (future)
- [ ] Phase 5: Testing & Validation
  - [ ] Unit tests
  - [ ] Integration tests
  - [ ] End-to-end tests
  - [ ] API validation

---

## References

- **Architecture Plan:** `artifacts/mpfs_opps_architecture_plan.md`
- **PRD-OPPS:** `prds/PRD-opps-prd-v1.0.md`
- **Source Map:** `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **RVU Ingestor (Reference):** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`
- **Test Data:** `test_data/ingestion_2025/opps/` (to be created)

