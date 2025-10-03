# Census Gazetteer Data Source

**Status:** Placeholder v0.1  
**Owners:** Data Engineering  
**Consumers:** Geography Services, ZIP Resolver  
**Change control:** PR review  

## Overview
This document describes the Census Gazetteer ZIP code data source used for geographic calculations and locality mapping.

## Data Source Details
- **Source:** US Census Bureau Gazetteer Files
- **Format:** CSV, Fixed-width
- **Cadence:** Annual
- **Key Fields:** ZIP5, ZCTA, Latitude, Longitude, State, County

## Access Information
- **Primary URL:** [Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
- **Authentication:** Public access
- **Rate Limits:** None specified

## Data Quality Notes
- ZIP codes may not have corresponding ZCTA
- Some ZIP codes may be PO Boxes only
- Geographic coordinates are approximate centroids

## TODO
This is a placeholder file. Full documentation needed including:
- [ ] Detailed field specifications
- [ ] Data quality validation rules
- [ ] Processing requirements
- [ ] Integration with ZIP resolver

---
**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
