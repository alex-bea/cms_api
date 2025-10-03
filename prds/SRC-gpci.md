# GPCI Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** MPFS Ingester, RVU Services  
**Change control:** PR review  

## Overview
This document describes the Geographic Practice Cost Indices (GPCI) data source used for geographic adjustments to physician payments.

## Data Source Details
- **Source:** CMS GPCI Files
- **Format:** CSV, XLSX
- **Cadence:** Annual
- **Key Fields:** Locality, Work GPCI, Practice Expense GPCI, Malpractice GPCI

## Access Information
- **Primary URL:** [CMS GPCI Files](https://www.cms.gov/medicare/physician-fee-schedule/geographic-practice-cost-indices)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- Annual updates with geographic adjustments
- Locality codes correspond to payment localities
- Indices affect final payment calculations

## TODO
This is a placeholder file. Full documentation needed including:
- [ ] Detailed field specifications
- [ ] Locality code mappings
- [ ] Processing requirements
- [ ] Integration with RVU services

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
