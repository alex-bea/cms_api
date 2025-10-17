# CMS MPFS RVU Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** MPFS Ingester, RVU Services  
**Change control:** PR review  

## Overview
This document describes the CMS Medicare Physician Fee Schedule (MPFS) Resource-Based Relative Value Units (RVU) data source.

## Data Source Details
- **Source:** CMS MPFS RVU Files
- **Format:** CSV, TXT, XLSX
- **Cadence:** Quarterly (with annual updates)
- **Key Fields:** HCPCS, Work RVU, Practice Expense RVU, Malpractice RVU, Global Days

## Access Information
- **Primary URL:** [CMS MPFS Search](https://www.cms.gov/medicare/physician-fee-schedule/search)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- Files include PPRRVU, GPCI, Conversion Factors
- Multiple file formats for same data
- Quarterly correction releases common

## TODO
This is a placeholder file. Full documentation needed including:
-  Detailed field specifications
-  File format variations
-  Processing requirements
-  Integration with MPFS ingester

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
