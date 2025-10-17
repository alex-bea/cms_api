# CMS OPPS Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** OPPS Ingester, Hospital Services  
**Change control:** PR review  

## Overview
This document describes the CMS Hospital Outpatient Prospective Payment System (OPPS) data source.

## Data Source Details
- **Source:** CMS OPPS Addendum Files
- **Format:** CSV, TXT, XLSX
- **Cadence:** Quarterly
- **Key Fields:** HCPCS, APC, Payment Rate, Status Indicator, Wage Index

## Access Information
- **Primary URL:** [CMS OPPS Files](https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- Quarterly addenda with corrections
- Multiple file types per quarter
- Status indicators affect payment calculations

## TODO
This is a placeholder file. Full documentation needed including:
-  Detailed field specifications
-  Quarterly release schedule
-  Processing requirements
-  Integration with OPPS ingester

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
