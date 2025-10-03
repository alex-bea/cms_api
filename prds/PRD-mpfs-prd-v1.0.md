# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

**Status:** Draft v1.0  
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** Data Engineering, Pricing API, Analytics, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0:** Data ingestion lifecycle and storage patterns
- **STD-qa-testing-prd-v1.0:** Testing requirements for MPFS ingestion
- **REF-nearest-zip-resolver-prd-v1.0.md:** ZIP resolver for geography mapping

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — to support downstream **network + price + access** and analytics use cases. The ingester stores everything; **no price math in-ingest** (computation lives downstream).

## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  

- **Excluded (in v1)**  
  - Price calculation (CF × (RVU × GPCI) etc.)  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  

- **Adjacent / references**  
  - PFS Look-Up Tool (for parity QC)  
  - OPPS Addendum B (for future site-neutral linkage)

## Sources & Cadence  
- **PFS Relative Value Files** (e.g. RVU25A/B/C/D) — quarterly refreshes  
- **PFS documentation / abstracts**  
- **Localities & GPCI** (yearly sets + occasional updates)  
- **Conversion Factors** (annually)  
- **Update cadence & vintage**  
  - Ingest CY 2023–2025 now; retain 6 years  
  - Monthly scraper to detect updates  
  - Freeze historical quarterlies; diff reports between versions  

## Schema / Data Model  

### Raw tables (1:1 with CMS artifacts)  
- `rvu_raw_YYYYQ` — all columns from the corresponding RVU file  
- `pfs_abstract_raw_YYYYQ` — abstracts / national payment snippets  
- `locality_raw_YYYY` — locality metadata (carrier, locality, name, state, etc.)  
- `gpci_raw_YYYY` — GPCI indices (Work, PE, MP) by locality  
- `cf_raw_YYYY` — Physician & Anesthesia conversion factors  

### Curated / derived views  
- `mpfs_rvu` — core RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`  
- `mpfs_indicators_all` — exploded table of policy/indicator flags for analytics  
- `mpfs_locality` — locality dimension (id, code, name)  
- `mpfs_gpci` — GPCI indices per locality and vintage  
- `mpfs_cf_vintage` — CF values per year  
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)  

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  
- Parity sampling: recompute sample lines vs PFS Look-Up Tool / national values (non-blocking)  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- Do not compute pay amounts in the ingester; the ingester is strictly storage + light QC  
- Price formula (downstream) is:  
    Payment = Conversion_Factor × [ (Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP) ]

- Sequestration or other adjustments are applied after base price  
- Geographic floors / overrides handled as metadata flags (not transforms)  

## Licensing / ToS  
- CMS publishes these as open program data; include attribution in manifests  
- Record source URL, last-modified, robots/ToS metadata  

## Roadmap / Future Enhancements  
- Incorporate sequestration reductions downstream  
- Add flags / override logic for geographic floors (e.g. PE floor)  
- Site-neutral analytics via joining into OPPS data  
- Support retroactive updates or corrections (CMS errata)  
