# Documentation Governance Standard (v1.0)

## 0. Overview
This standard defines how product requirement documents (PRDs), runbooks, and related governance artifacts are named, versioned, and organized within the repository. All new documents must comply; existing files should be renamed during their next major revision.

**Master Catalog.** This repository’s canonical system catalog is `DOC-master-catalog_prd_v1.0.md` (the “Master PRD”). All new or modified documents (STD/BP/PRD/SCR/RUN/DOC) **must be registered** in the Master Catalog in the same pull request before merge. The Master Catalog is a catalog and dependency map only; no business logic or schema definitions live there.

**Status:** Draft v1.0 (proposed)  
**Owners:** Platform/Product Operations  
**Consumers:** Engineering, Product, QA, SRE  
**Change control:** ADR + Docs Guild review  

**Cross-References:**
- **DOC-master-catalog_prd_v1.0.md:** Master system catalog and dependency map

## 1. File Naming Conventions
### 1.1 Prefix (scope indicator)
- `STD-` — Standards (org-wide policy)
- `BP-` — Blueprints / implementation packs
- `PRD-` — Dataset or API product requirements
- `SCR-` — Scraper-specific PRDs (optional if included in PRD)
- `RUN-` — Operational runbooks
- `DOC-` — Misc guides/reference not covered above.
  - **Reserved filename:** `DOC-master-catalog_prd_vX.Y.md` — the Master Catalog that indexes all other docs and shows dependencies/ownership.

### 1.2 Slug Formatting
- Use lowercase hyphenated slugs following the prefix: `STD-global-api-program`
- Avoid spaces, camel case, or underscores in the slug portion.

### 1.3 Version Suffix
- Append `_vMAJOR.MINOR.md` (e.g., `_v1.0.md`).
- Increment MAJOR for breaking structural/content changes; MINOR for additive updates.

### 1.4 Examples
- `STD-api-architecture_prd_v1.0.md`
- `PRD-opps_prd_v1.0.md`
- `RUN-global-operations_prd_v1.0.md`
- `DOC-master-catalog_prd_v1.0.md`

## 2. Directory Expectations
- All governance documents live under `/prds`.
- Subfolders are discouraged unless a program has >10 directly related docs; in that case, create `/prds/<program>/` with a README linking back to this standard.

## 3. Metadata Requirements
Each document must include at minimum (in the header block):
- Status (e.g., Draft, Adopted, Deprecated)
- Owners (role or team)
- Consumers
- Change control process

**Additional requirements for the Master Catalog (`DOC-master-catalog`):**
- Owner, Status, Version, **Review cadence**, and **Diagram standard** (e.g., C4).
- Tables that register **Standards (STD)**, **Reference Architectures (REF)**, **Product PRDs (PRD)**, **Data Sources (SRC)**, and **Runbooks (RUN)** with Owner/Status/Reviewed date.
- A **Change Log** recording adoption/deprecation of linked docs and version bumps.

## 4. Required Header Block
Each document must include a governance header immediately after the title with the following fields:

```
**Status:** <Draft/Adopted/Deprecated> <version>
**Owners:** <team or role>
**Consumers:** <teams that rely on the doc>
**Change control:** <process (e.g., ADR + PR review)>
```

For the Master Catalog, also include `Review cadence:` and `Diagram standard:` in the header.

## 5. Version Control & Change Log
- Major updates require a dated changelog table inside the document.
- Minor updates can be captured in Git history but should still update the changelog when BC behavior changes.
- The Master Catalog version **must bump** whenever a linked document is added, adopted, deprecated, or retired.
- Git history serves as the source of truth for diffs; the Master Catalog’s Change Log references the PR that introduced each change.

## 6. Cross-Referencing Guidelines
- Refer to documents by prefix + slug (e.g., "see `STD-api-architecture_prd_v1.0.md`").
- Avoid hardcoding version numbers in prose; instead, link to the latest and note when version-specific behavior applies.
- All new or renamed documents must add/update an entry in `DOC-master-catalog_prd_v*.md` as part of the same PR (acceptance criterion).
- When referencing other docs in prose, prefer linking to their entry in the Master Catalog to improve discoverability; avoid hardcoding versions unless version-specific behavior is discussed.

## 7. Compliance & Review
- Docs Guild conducts quarterly audits; non-compliant files are flagged for remediation.
- Automated lint blocks misnamed files during PR review (regex: `^(STD|BP|PRD|SCR|RUN|DOC)-[a-z0-9-]+_v[0-9]+\.[0-9]+\.md$`).
- Pre-commit hook verifies that any new or renamed doc has a corresponding row in the Master Catalog tables; missing entries block merge.
- Status transitions (Draft → Adopted → Deprecated/Retired) must include evidence of approval by the designated Owner and Security (or delegate) in the PR.

## 8. Change Log
| Date       | Version | Author | Summary |
|------------|---------|--------|---------|
| 2025-10-02 | v1.0    | Team   | Established Master Catalog filename and registration requirements. |
| 2025-09-30 | v1.0    | Team   | Initial draft of documentation governance standard. |
