# RVU Scraper Data Flow

## What Happens After Scraper Completes

### Phase 1: Discovery (Scraper Output)

**Scraper completes and produces:**

1. **DiscoveryManifest** (saved to disk)
   - Location: `data/cms_rvu/manifests/cms_rvu_manifest_TIMESTAMP.jsonl`
   - Contains: File URLs, metadata, versions, content types
   - Format: DIS-compliant JSONL

2. **List of RVUFileInfo objects** (returned to caller)
   - Contains validated download URLs
   - Metadata enriched (dates, sizes, versions)
   - Ready for optional downloading

### Phase 2: Download (Optional, Behind Approval Gate)

**Via HistoricalDataManager:**

```python
manager = HistoricalDataManager("./data/historical_rvu")
summary = await manager.download_historical_data(
    start_year=2023, 
    end_year=2025, 
    download=True  # ← Triggers actual downloads
)
```

**Downloads save to:**
- `data/historical_rvu/downloads/{year}/{filename}`
- Example: `data/historical_rvu/downloads/2025/rvu25a-20250110.zip`

### Phase 3: Ingestion (RVUIngestor)

**The ingestor consumes:**

1. **Manifests** OR **RVUFileInfo objects**
2. Downloads files if not already downloaded
3. Extracts data (PPRRVU, GPCI, OPPSCap, etc.)
4. Validates and normalizes
5. Enriches with reference data
6. **SQLAlchemy writes to PostgreSQL**

**Database tables created:**
- `releases` - Metadata about RVU releases
- `rvu_items` - Individual RVU records (HCPCS + modifiers)
- `gpci_indices` - Geographic indices
- `oppscap_items` - OPPS cap data
- `anescf_items` - Anesthesia conversion factors
- `locality_county` - Geography mappings

### Phase 4: API Access

**FastAPI endpoints expose data:**
- GET `/rvu/releases` - List releases
- GET `/rvu/releases/{release_id}` - Get specific release
- GET `/rvu/items` - Query RVU items
- GET `/rvu/gpci` - Get GPCI data

**Data served from:** PostgreSQL database

---

## File Flow Diagram

```
CMS.gov Website
     ↓ (HTTP GET to landing page)
RVU Scraper (cms_rvu_scraper.py)
     ↓
DiscoveryManifest (metadata only, no files yet)
     ↓ Saved to disk
data/cms_rvu/manifests/cms_rvu_manifest_*.jsonl
     ↓
     ├─→ Option A: Download files (HistoricalDataManager)
     │   │
     │   ↓ Saves ZIP files to disk
     │   data/historical_rvu/downloads/{year}/*.zip
     │   │
     │   └─→ RVUIngestor reads ZIP files
     │
     └─→ Option B: RVUIngestor directly downloads
         │
         ↓ Downloads + Extracts + Validates + Enriches
         
         PostgreSQL Database (6 tables)
         │
         ↓ Data available via API
         
         FastAPI Endpoints (cms_pricing/routers/rvu.py)
```

---

## Key Points

### 1. **Discovery is Separate from Download**
- Scraper discovers file URLs and metadata
- Downloads are optional and happen later
- Allows for approval gates, change detection, etc.

### 2. **Manifests are the Contract**
- `DiscoveryManifest` is the scraper output
- Contains all metadata needed for downloading
- Change detection compares manifest diffs

### 3. **Multiple Paths to Data**
- **Direct:** Scraper → RVUIngestor (downloads on the fly)
- **Staged:** Scraper → HistoricalDataManager (downloads + caches) → RVUIngestor (reads cached)
- Both paths work, staged is recommended for production

### 4. **Database is the Final Destination**
- All RVU data ends up in PostgreSQL
- Parsed, validated, and enriched
- Accessed via FastAPI REST endpoints

---

## Example Workflow

```python
# 1. Discovery (new files found)
scraper = CMSRVUScraper()
files = await scraper.scrape_rvu_files(start_year=2024, end_year=2025)
# Output: 9 RVUFileInfo objects + manifest saved

# 2. Download (optional, approval required)
if approved:
    manager = HistoricalDataManager()
    summary = await manager.download_historical_data(
        start_year=2024, end_year=2025, download=True
    )
    # Output: 9 ZIP files downloaded to disk

# 3. Ingestion (parse + load to database)
ingestor = RVUIngestor()
result = await ingestor.ingest(release_id="rvu2025")
# Output: 10,000+ records in PostgreSQL tables

# 4. API Access
# GET /rvu/releases returns all releases
# GET /rvu/items?release_id=rvu2025 returns 10,000 records
```

---

## File Locations

| Component | Path Pattern |
|-----------|-------------|
| **Discovery Manifests** | `data/cms_rvu/manifests/cms_rvu_manifest_*.jsonl` |
| **Downloaded ZIPs** | `data/historical_rvu/downloads/{year}/*.zip` |
| **Parsed Data** | PostgreSQL database (in memory during processing) |
| **Exported Files** | `data/output/releases/{release_id}/curated/*.parquet` |

---

## Summary

**Scraper Output** → DiscoveryManifest (metadata)
                  ↓
**Download (optional)** → ZIP files on disk
                  ↓
**Ingestion** → Parse + validate + enrich
                  ↓
**Database** → PostgreSQL tables
                  ↓
**API** → FastAPI endpoints serving JSON

The scraper's job is discovery and validation. It doesn't download files by default (that's now optional behind an approval gate). The ingestor consumes manifests and handles the full pipeline to the database.
