# Carrier Locality Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** Geography Services, Locality Mapping  
**Change control:** PR review  

## Overview
This document describes the Carrier Locality crosswalk data source used for mapping ZIP codes to payment localities.

## Data Source Details
- **Source:** CMS Carrier Locality Files
- **Format:** CSV, XLSX
- **Cadence:** Annual
- **Key Fields:** ZIP5, Carrier, Locality, State, Effective Date

## Access Information
- **Primary URL:** [CMS Carrier Locality Files](https://www.cms.gov/medicare/physician-fee-schedule/geographic-practice-cost-indices)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- Annual updates with geographic changes
- ZIP codes may map to different localities over time
- Carrier assignments affect payment calculations

## TODO
This is a placeholder file. Full documentation needed including:
-  Detailed field specifications
-  Locality code definitions
-  Processing requirements
-  Integration with geography services

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
