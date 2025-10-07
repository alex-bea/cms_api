# PRD: NCCI / MUE Ingest (v1)

## Objective  
Ingest and persist the NCCI (National Correct Coding Initiative) reference tables — PTP edits and MUEs — to support downstream edit validation, rule engines, and analytics. This PRD stores the data; applying rules / logic is left to consumers.

## Scope  
- **Included**  
  - PTP edits (Column 1 / Column 2 code pairs, modifier flags, effective dates)  
  - MUE tables (MUE values, MAI / adj indicator, effective dates)  
  - All published fields  

- **Excluded**  
  - Edit adjudication logic (which codes pair, bundling enforcement, MUE enforcement)  
  - Claim-level transformations  

## Sources & Cadence  
- PTP edits (quarterly updates by CMS)  
- MUE tables (published by CMS, often quarterly or annually)  
- Monthly scraper to detect new releases  
- Retain 6 years of historical versions  

## Schema / Data Model  

### Raw tables  
- `ncci_ptp_raw_YYYYQ` — as published PTP edits  
- `ncci_mue_raw_YYYYQ` — as published MUE table  

### Curated views  
- `ncci_ptp`  
  - Fields: `col1_code`, `col2_code`, `modifier_flag`, `effective_from`, `effective_to`, `notes`, `source_vintage`  
- `ncci_mue`  
  - Fields: `hcpcs`, `mue_value`, `mai` (or adjust indicator), `effective_from`, `effective_to`, `source_vintage`  

## Keys & Joins  
- PTP: join to MPFS via `(hcpcs, modifier, quarter_vintage)` for simulation / analytics  
- MUE: join similarly via HCPCS (modifier context as needed)  
- Always preserve all fields; no logic baked in  

## Quality / QC / Diffing  
- Schema compliance (presence of all published fields)  
- Idempotent download (checksum, content validation)  
- Quarter-to-quarter diffs (new / removed PTP pairs; changes in MUE values)  
- Manifest with source URLs, last-modified, and metadata  

## Licensing / ToS  
- CMS program data; record attribution and legal metadata  

## Roadmap / Extensions  
- Support rule engine linking of PTP/MUE logic  
- Versioned branching for multiple rule sets (e.g. payer variants)  
- Integration of CMS “Correction” tables / errata into retention logic  