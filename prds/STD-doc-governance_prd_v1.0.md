# Documentation Governance Standard (v1.0)

## 0. Overview
This standard defines how product requirement documents (PRDs), runbooks, and related governance artifacts are named, versioned, and organized within the repository. All new documents must comply; existing files should be renamed during their next major revision.

**Status:** Draft v1.0 (proposed)  
**Owners:** Platform/Product Operations  
**Consumers:** Engineering, Product, QA, SRE  
**Change control:** ADR + Docs Guild review  

## 1. File Naming Conventions
### 1.1 Prefix (scope indicator)
- `STD-` — Standards (org-wide policy)
- `BP-` — Blueprints / implementation packs
- `PRD-` — Dataset or API product requirements
- `SCR-` — Scraper-specific PRDs (optional if included in PRD)
- `RUN-` — Operational runbooks
- `DOC-` — Misc guides/reference not covered above

### 1.2 Slug Formatting
- Use lowercase hyphenated slugs following the prefix: `STD-global-api-program`
- Avoid spaces, camel case, or underscores in the slug portion.

### 1.3 Version Suffix
- Append `_vMAJOR.MINOR.md` (e.g., `_v1.0.md`).
- Increment MAJOR for breaking structural/content changes; MINOR for additive updates.

### 1.4 Examples
- `STD-global-api-program_prd_v1.0.md`
- `PRD-opps-ingestion_v1.0.md`
- `RUN-global-operations_v1.0.md`

## 2. Directory Expectations
- All governance documents live under `/prds`.
- Subfolders are discouraged unless a program has >10 directly related docs; in that case, create `/prds/<program>/` with a README linking back to this standard.

## 3. Metadata Requirements
Each document must include at minimum (in the header block):
- Status (e.g., Draft, Adopted, Deprecated)
- Owners (role or team)
- Consumers
- Change control process

## 4. Version Control & Change Log
- Major updates require a dated changelog table inside the document.
- Minor updates can be captured in Git history but should still update the changelog when BC behavior changes.

## 5. Cross-Referencing Guidelines
- Refer to documents by prefix + slug (e.g., “see `STD-global-api-program_prd_v1.0.md`”).
- Avoid hardcoding version numbers in prose; instead, link to the latest and note when version-specific behavior applies.

## 6. Compliance & Review
- Docs Guild conducts quarterly audits; non-compliant files are flagged for remediation.
- Automated lint (planned) will warn on misnamed files during PR review.

## 7. Change Log
| Date       | Version | Author | Summary |
|------------|---------|--------|---------|
| 2025-09-30 | v1.0    | Team   | Initial draft of documentation governance standard. |

