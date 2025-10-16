# Documentation Governance Standard (v1.0.2)

## 0. Overview
This standard defines how product requirement documents (PRDs), runbooks, and related governance artifacts are named, versioned, and organized within the repository. All new documents must comply; existing files should be renamed during their next major revision.

**Master Catalog.** This repository's canonical system catalog is `DOC-master-catalog-prd-v1.0.md` (the "Master PRD"). All new or modified documents (STD/BP/PRD/SCR/RUN/DOC) **must be registered** in the Master Catalog in the same pull request before merge. The Master Catalog is a catalog and dependency map only; no business logic or schema definitions live there.

**Status:** Draft v1.0.2 (proposed)  
**Owners:** Platform/Product Operations  
**Consumers:** Engineering, Product, QA, SRE  
**Change control:** ADR + Docs Guild review  

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map

## 1. File Naming Conventions
### 1.1 Prefix (scope indicator)
- `STD-` — Standards (org-wide policy)
- `BP-` — Blueprints / implementation packs
- `PRD-` — Dataset or API product requirements
- `REF-` — Reference architectures and integration patterns
- `SCR-` — Scraper-specific PRDs (optional if included in PRD)
- `RUN-` — Operational runbooks
- `DOC-` — Misc guides/reference not covered above.
  - **Reserved filename:** `DOC-master-catalog-prd-vX.Y.md` — the Master Catalog that indexes all other docs and shows dependencies/ownership.

### 1.2 Slug Formatting
- Use lowercase hyphenated slugs following the prefix: `STD-global-api-program`
- Avoid spaces, camel case, or underscores in the slug portion.

### 1.3 Version Suffix
- Append `-vMAJOR.MINOR.md` (e.g., `-v1.0.md`).
- Increment MAJOR for breaking structural/content changes; MINOR for additive updates.

**Note on `-prd` suffix:**
- `STD-`, `PRD-`, `RUN-`, `DOC-` documents: Include `-prd` before version (e.g., `STD-api-architecture-prd-v1.0.md`)
- `REF-` documents: May omit `-prd` suffix (e.g., `REF-scraper-ingestor-integration-v1.0.md`)
- Legacy `REF-*-prd-v*.md` files are acceptable and will migrate during next major revision
- Companion implementation guides: Use `-impl` suffix (e.g., `STD-data-architecture-impl-v1.0.md`)

### 1.4 Examples
- `STD-api-architecture-prd-v1.0.md` (main standard)
- `STD-api-architecture-impl-v1.0.md` (companion implementation guide)
- `PRD-opps-prd-v1.0.md` (product requirement)
- `REF-scraper-ingestor-integration-v1.0.md` (reference architecture, no `-prd`)
- `REF-cms-pricing-source-map-prd-v1.0.md` (reference architecture, legacy `-prd`)
- `RUN-global-operations-prd-v1.0.md` (operational runbook)
- `DOC-master-catalog-prd-v1.0.md` (master catalog)

### 1.5 Companion Documents (Implementation Guides)

For standards that require detailed implementation guidance, create a companion document using the `-impl` suffix:

**Naming Pattern:**
- Main document: `{PREFIX}-{slug}-prd-v{X}.{Y}.md`
- Companion guide: `{PREFIX}-{slug}-impl-v{X}.{Y}.md`

**Purpose:**
- Main document (`-prd`): Requirements, policies, architecture, standards
- Companion guide (`-impl`): Templates, code examples, step-by-step guides, reference tables

**Key Characteristics:**
- Same prefix and slug ensure association
- Different suffix (`-prd` vs `-impl`) indicates purpose
- Alphabetically adjacent in file listings
- Can version independently
- Both must be registered in Master Catalog

**Cross-Referencing Requirements:**
- Main document MUST link to companion in a dedicated section (e.g., "Implementation Guide")
- Companion MUST reference main document in header under "Companion to:"
- Both MUST be registered in `DOC-master-catalog-prd-v*.md`

**Versioning:**
- Versions can evolve independently
- Main document version bumps for policy/requirement changes
- Companion version bumps for implementation updates, new examples, code changes
- Breaking changes in main document may require companion update

**When to Create Companion:**
- Implementation requires >500 lines of code examples/templates
- Multiple working examples needed
- Step-by-step tutorials required
- Reference tables and quick-start guides
- Frequent code updates that shouldn't trigger policy reviews

**Examples:**
- `STD-data-architecture-prd-v1.0.md` + `STD-data-architecture-impl-v1.0.md`
- `STD-scraper-prd-v1.0.md` + `STD-scraper-impl-v1.0.md` (future)
- `STD-api-architecture-prd-v1.0.md` + `STD-api-architecture-impl-v1.0.md` (future)

### 1.6 Automated Validation

**Main documents (PRD):**
```regex
^(STD|PRD|RUN|DOC)-[a-z0-9-]+-prd-v[0-9]+\.[0-9]+\.md$
```

**Reference architectures (REF):**
```regex
^REF-[a-z0-9-]+(-prd)?-v[0-9]+\.[0-9]+\.md$
```
Note: REF documents may include or omit `-prd` suffix. Legacy files with `-prd` are acceptable.

**Companion implementation guides:**
```regex
^(STD|REF|PRD|RUN|DOC)-[a-z0-9-]+-impl-v[0-9]+\.[0-9]+\.md$
```

Companion patterns share the same `{prefix}-{slug}` portion, differing only in the suffix (`-prd` vs `-impl`).

**Automated checks (via `tools/audit_doc_catalog.py`):**
- Companion documents match naming pattern
- Companion documents have corresponding main document
- Companion documents reference their main document
- Main documents reference their companion (warning if missing)
- Slug consistency between main and companion
- Both registered in Master Catalog

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

### 5.1 Project-Level CHANGELOG.md

The repository MUST maintain a `CHANGELOG.md` file at the project root following [Keep a Changelog](https://keepachangelog.com) format:

**Requirements:**
- **Format:** Keep a Changelog v1.0.0 standard
- **Versioning:** Semantic Versioning (MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-label)
- **Sections:** Added, Changed, Deprecated, Removed, Fixed, Security
- **Dates:** ISO 8601 format (YYYY-MM-DD)
- **Links:** Commit hashes, PR numbers, git tags
- **Cross-references:** Link to relevant PRDs for each release

**Structure:**
```markdown
## [Unreleased]
### Added
- New features in progress

## [X.Y.Z] - YYYY-MM-DD
### Added
- Feature descriptions with [commit links]
### Changed
- Breaking changes with migration notes
### References
- [STD-parser-contracts-prd-v1.0.md]
- [PRD-mpfs-prd-v1.0.md]
```

**Maintenance:**
- Updated on every release/milestone
- Git tags MUST match changelog versions
- Unreleased section tracks work in progress
- Automated via `tools/audit_changelog.py`

### 5.2 Document-Level Change Logs

- Major updates require a dated changelog table inside the document.
- Minor updates can be captured in Git history but should still update the changelog when BC behavior changes.
- The Master Catalog version **must bump** whenever a linked document is added, adopted, deprecated, or retired.
- Git history serves as the source of truth for diffs; the Master Catalog's Change Log references the PR that introduced each change.

## 6. Cross-Referencing Guidelines
- Refer to documents by prefix + slug (e.g., "see `STD-api-architecture-prd-v1.0.md`").
- Avoid hardcoding version numbers in prose; instead, link to the latest and note when version-specific behavior applies.
- All new or renamed documents must add/update an entry in `DOC-master-catalog-prd-v*.md` as part of the same PR (acceptance criterion).
- When referencing other docs in prose, prefer linking to their entry in the Master Catalog to improve discoverability; avoid hardcoding versions unless version-specific behavior is discussed.

## 7. Compliance & Review
- Docs Guild conducts quarterly audits; non-compliant files are flagged for remediation.
- Automated lint blocks misnamed files during PR review:
  - Main documents: `^(STD|BP|PRD|SCR|RUN|DOC)-[a-z0-9-]+-prd-v[0-9]+\.[0-9]+\.md$`
  - Companion guides: `^(STD|BP|PRD|SCR|RUN|DOC)-[a-z0-9-]+-impl-v[0-9]+\.[0-9]+\.md$`
- Pre-commit hook verifies that any new or renamed doc has a corresponding row in the Master Catalog tables; missing entries block merge.
- Companion documents are validated for proper cross-references and slug consistency.
- Status transitions (Draft → Adopted → Deprecated/Retired) must include evidence of approval by the designated Owner and Security (or delegate) in the PR.

## 8. Change Log
| Date       | Version | Author | Summary |
|------------|---------|--------|---------|
| 2025-10-16 | v1.0.2  | Team   | Added project-level CHANGELOG.md requirement (§5.1): Keep a Changelog format, SemVer, ISO 8601 dates, automated validation via `tools/audit_changelog.py`. Requires changelog for all releases with commit links, PRD cross-references, and migration notes. |
| 2025-10-15 | v1.0.1  | Team   | Added companion document conventions (§1.5-1.6): `-impl` suffix pattern, cross-referencing requirements, versioning rules, and automated validation checks. Updated §1.3 to allow REF documents without `-prd` suffix (new pattern) while maintaining backward compatibility with legacy REF docs. |
| 2025-10-02 | v1.0    | Team   | Established Master Catalog filename and registration requirements. |
| 2025-09-30 | v1.0    | Team   | Initial draft of documentation governance standard. |

## Appendix M — GitHub Labels (reference)
Use the following repository labels when triaging CI/doc-governance tasks:

- `source-map-drift` — automation detected a mismatch between discovery manifests and reference documentation.
- `manifest-schema-change` — updates to the discovery manifest format/schema that require downstream review.
- `database-test-patterns` — database testing pattern or isolation improvements requiring coordination.
