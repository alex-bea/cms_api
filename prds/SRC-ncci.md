# NCCI MUE Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** NCCI Ingester, Claims Processing  
**Change control:** PR review  

## Overview
This document describes the National Correct Coding Initiative (NCCI) Medically Unlikely Edit (MUE) data source.

## Data Source Details
- **Source:** CMS NCCI MUE Files
- **Format:** CSV, XLSX
- **Cadence:** Quarterly
- **Key Fields:** HCPCS, MUE Value, Effective Date, Type

## Access Information
- **Primary URL:** [CMS NCCI Files](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- Quarterly updates with corrections
- MUE values may change based on medical necessity
- Different types of edits apply

## TODO
This is a placeholder file. Full documentation needed including:
- [ ] Detailed field specifications
- [ ] Edit type classifications
- [ ] Processing requirements
- [ ] Integration with NCCI ingester

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
