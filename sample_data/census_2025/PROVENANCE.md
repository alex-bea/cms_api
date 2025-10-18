# Census 2025 Gazetteer Counties - Provenance

## Source

**Authority:** U.S. Census Bureau  
**Dataset:** 2025 Gazetteer Files - National Counties  
**URL:** https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_counties_national.zip  
**Download Date:** 2025-10-17  
**Release Date:** September 8, 2025 (per file timestamp)

## Files

- `2025_Gaz_counties_national.zip` - Original download (135 KB)
- `2025_Gaz_counties_national.txt` - Extracted pipe-delimited file (325 KB)

## Dataset Characteristics

**Record Count:** 3,222 counties/equivalents  
**Format:** Pipe-delimited (|) text file with header  
**Encoding:** UTF-8  
**Coverage:** All US states, DC, and territories

**Columns:**
- `USPS` - State abbreviation (2-char)
- `GEOID` - 5-digit FIPS code (SSCCC format)
- `GEOIDFQ` - Fully qualified GEOID
- `ANSICODE` - ANSI feature code
- `NAME` - Canonical county name with suffix (e.g., "Autauga County", "Orleans Parish")
- `ALAND` - Land area (sq meters)
- `AWATER` - Water area (sq meters)
- `ALAND_SQMI` - Land area (sq miles)
- `AWATER_SQMI` - Water area (sq miles)
- `INTPTLAT` - Internal point latitude
- `INTPTLONG` - Internal point longitude

## County Equivalents Included

- **Counties:** 2,999 (standard "County" designation)
- **Louisiana Parishes:** 64 (e.g., "Orleans Parish")
- **Virginia Independent Cities:** 41 (e.g., "Alexandria city")
- **Alaska Boroughs:** 13 (e.g., "Aleutians East Borough")
- **Alaska Census Areas:** 11 (e.g., "Bethel Census Area")
- **Alaska City and Boroughs:** 4 (e.g., "Juneau City and Borough")
- **Alaska Municipalities:** 2 (e.g., "Anchorage Municipality")

## Data Quality Notes

- **Diacritics preserved:** Yes (e.g., "Doña Ana County" in New Mexico)
- **Canonical naming:** Includes full suffix (County/Parish/Borough/Census Area/city)
- **Deterministic order:** Sorted by GEOID (state FIPS, then county FIPS)

## Processing

**Script:** `tools/build_county_reference.py`  
**Output:** `data/reference/census/fips_counties/2025/us_counties.csv`  
**Transformation:**
- Extracts state_fips (chars 0-2) and county_fips (chars 2-5) from GEOID
- Parses county_type from NAME suffix
- Joins with state names from `us_states.csv`
- Exports 8 columns: state_fips, state_abbr, state_name, county_fips, county_geoid, county_name, county_name_canonical, county_type

## Verification

**Spot Checks (Passed):**
- ✓ Diacritics: "Doña Ana County" (FIPS 35013)
- ✓ VA independent city: "Alexandria city" (FIPS 51510)
- ✓ LA parish: "Orleans Parish" (FIPS 22071)
- ✓ AK census area: "Bethel Census Area" (FIPS 02050)

## References

- **Census Gazetteer Landing Page:** https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
- **TIGER/Line Technical Documentation:** https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2025/TGRSHP2025_TechDoc.pdf
- **Geographic Entities & Concepts:** https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_13

## Usage in CMS Pricing API

**Purpose:** Authoritative FIPS code lookup for CMS Locality-County crosswalk normalization (Stage 2)  
**Integration:** `cms_pricing/ingestion/normalize/normalize_locality_fips.py`  
**PRD Reference:** STD-data-architecture-impl-v1.0.md §4.2 (Dual-Mode Reference Data Access)

## License & Attribution

**Public Domain:** U.S. Census Bureau geographic products are in the public domain and not subject to copyright protection.  
**Attribution:** U.S. Census Bureau, 2025 Gazetteer Files  
**Disclaimer:** The Census Bureau provides this data "as is" without warranty of any kind.

## Future Maintenance

**Update Cadence:** Annual (Census releases updated Gazetteer files each year)  
**Next Update:** ~September 2026 (2026 Gazetteer expected)  
**Automated Scraper:** See GitHub Tasks - "Census Reference Data Scraper"

