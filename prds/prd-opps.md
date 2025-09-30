# PRD: OPPS Addendum B Ingest (stub)

## Objective  
Ingest CMS’s OPPS Addendum B files (HCPCS-level outpatient payment data) to support future site-neutral analytics and comparison with MPFS.

## Scope  
- Ingest **Addendum B** quarterly files (OPPS rates, status indicators, APC categories)  
- Keep full published columns  
- No price transformations here; only storage + diffing

## Sources & Cadence  
- CMS OPPS quarterly addenda (B)  
- Monthly polling for updates  
- Retain 6 years

## Schema / Data Model  
- `opps_addendum_b_raw_YYYYQ` — raw published table  
- `opps_b` — curated view with key fields: `hcpcs`, `modifier`, `APC`, `status_indicator`, `opps_rate`, `effective_from`, `effective_to`, `source_vintage`

## Keys & Joins  
- Key on `(hcpcs, modifier, quarter_vintage)`  
- Designed to join later with MPFS via those keys for cross-system comparisons

## Quality / QC / Diffing  
- Schema consistency checks  
- Idempotent downloads  
- Quarterly diff (new/retired codes, rate changes)  
- Manifest with metadata (source URL, release date)  

## Licensing / ToS  
- CMS public data; include attribution in manifest  