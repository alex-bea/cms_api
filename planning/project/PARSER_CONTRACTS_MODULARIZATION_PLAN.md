# Parser Contracts Modularization Plan

**Date:** 2025-10-17  
**Current File:** `STD-parser-contracts-prd-v1.0.md` (4,477 lines)  
**Problem:** Context overload - mixing policy, implementation, and operations  
**Goal:** Split into 3 focused documents for better AI/human digestibility  

**Governance:** Per STD-doc-governance-prd-v1.0.md §1 (prefixes), §1.5 (companion docs)

---

## Executive Summary

**Proposed Split:**

1. **STD-parser-contracts-v2.0.md** (~600 lines) - **Core Policy**
   - Immutable architectural decisions (ParseResult contract, SemVer, natural keys)
   - High-level requirements (what parsers must do)
   - AI Context: Use for compliance checks, signature validation

2. **REF-parser-implementation-v1.0.md** (~2,500 lines) - **Implementation Guide**
   - Code-heavy guidance (alias maps, type handling, row hashing)
   - All §21 sections (templates, checklists, phased approach)
   - Anti-patterns with fixes
   - AI Context: Primary source for code generation

3. **RUN-parser-qa-runbook-v1.0.md** (~1,400 lines) - **Operational Procedures**
   - Pre-implementation checklists (§21.4)
   - SLAs, metrics, logging requirements
   - Observability patterns, safe metrics calculation
   - AI Context: Use for QA/validation tasks, metric instrumentation

**Benefits:**
- ✅ Faster AI context loading (3 focused docs vs 1 monolith)
- ✅ Clear separation of concerns (policy vs code vs ops)
- ✅ Independent versioning (impl can evolve without policy changes)
- ✅ Easier onboarding (read policy first, impl as needed, ops when shipping)

**Version Bump:** STD-parser-contracts v1.11 → v2.0 (MAJOR - structural change)

---

## Automation Support

### `tools/prd_modularizer.py`

- **Purpose:** Analyze large PRDs/STDs and recommend a governance-compliant modular split (core policy, companion impl, routing REF, quality REF, RUN, appendix).
- **Usage:**
  ```bash
  python tools/prd_modularizer.py prds/STD-parser-contracts-prd-v1.0.md \
    --output planning/project/STD-parser-contracts-modularization.md
  ```
  The script prints a Markdown restructuring plan to stdout (or `--output`) with section assignments, proposed filenames, and the required governance checklist.
- **Validation:** Example run captured in `.cursor/output/prd-modularizer-preview.txt` shows the parser contracts doc (4,476 lines) broken into the six planned documents with line ranges and anchor references.
- **Governance alignment:** The generated checklist includes the header/change-log requirements from `STD-doc-governance-prd-v1.0.md`, cross-reference updates, catalog registration, and audit tooling (`audit_doc_links`, `audit_cross_references`, `audit_doc_catalog`).
- **Post-split coverage check:** Pass `--compare category=path` arguments after drafting new docs to verify each target file still contains the assigned section titles. Example:
  ```bash
  python tools/prd_modularizer.py prds/STD-parser-contracts-prd-v1.0.md \
    --compare policy=prds/STD-parser-contracts-prd-v2.0.md \
    --compare implementation=prds/STD-parser-contracts-impl-v2.0.md
  ```
  The output warns about any missing titles so they can be restored before merge.
- **Section extraction:** Use `--export category=path` to copy the original markdown for a bucket into a new document without rewriting. Example:
  ```bash
  python tools/prd_modularizer.py prds/STD-parser-contracts-prd-v1.0.md \
    --export routing_ref=prds/REF-parser-routing-detection-v1.0.md \
    --export-mode append
  ```
  This appends the router/layout sections exactly as written in v1.0, so only the governance header and new overview need to be authored manually.

---

## Detailed Section Mapping

### Document 1: STD-parser-contracts-v2.0.md (~600 lines)

**Purpose:** Core policy, architectural decisions, contracts  
**Audience:** All engineers, architects, compliance reviewers  
**Stability:** High (rarely changes)  
**AI Use Case:** Compliance validation, signature checking

**Sections to Keep:**

| Old § | Section Name | Lines | Rationale |
|-------|--------------|-------|-----------|
| §1 | Summary | ~50 | Core definition of parser purpose |
| §2 | Goals & Non-Goals | ~30 | Strategic alignment |
| §3 | Users & Scenarios | ~40 | Context for requirements |
| §4 | Key Decisions & Rationale | ~80 | Immutable architectural choices |
| §6 | Contracts | ~200 | **CORE**: ParseResult, router, schema, metadata |
| §12 | Compatibility & Versioning | ~100 | SemVer rules, breaking changes |
| §15 | Acceptance Criteria | ~50 | What "done" means |
| §16 | Out of Scope | ~30 | Boundary definition |
| §18 | References | ~20 | Cross-references to REF/RUN |

**New Sections to Add:**

```markdown
## 19. Implementation Guide

See companion document: **REF-parser-implementation-v1.0.md**

Contains detailed implementation guidance including:
- §21 Parser Implementation Templates
- §5.2.3 Alias Map Best Practices
- §5.2.4 Defensive Type Handling Patterns
- §7.1 Router & Format Detection
- §10.3 Safe Metrics Calculation Patterns
- §20 Anti-Patterns & Fixes

## 20. Operational Runbook

See companion document: **RUN-parser-qa-runbook-v1.0.md**

Contains operational procedures including:
- §21.4 Pre-Implementation Verification Checklist
- §5.5 Constraints & SLAs
- §10 Observability & Metrics
- §11 Logging Requirements
- §13 Compliance & Licensing
```

**Total Estimated Lines:** ~600 lines (86% reduction from 4,477)

---

### Document 2: REF-parser-implementation-v1.0.md (~2,500 lines)

**Purpose:** Implementation guidance, code patterns, anti-patterns  
**Audience:** Engineers actively coding parsers  
**Stability:** Medium (evolves with lessons learned)  
**AI Use Case:** Code generation, pattern application

**Sections to Move:**

| Old § | Section Name | Lines | Why Move | AI Value |
|-------|--------------|-------|----------|----------|
| §5.2 | Processing Requirements | ~400 | Detailed encoding, normalization, casting specs | Code generation patterns |
| §5.2.3 | Alias Map Best Practices | ~150 | Code-heavy header mapping guidance | Direct code application |
| §5.2.4 | Defensive Type Handling | ~100 | Safe casting patterns with code examples | Error prevention |
| §7.1 | Router & Format Detection | ~300 | 6 subsections with flowcharts, code | Format detection implementation |
| §7.2 | Layout Registry | ~200 | Registry API, colspec structure | Layout definition |
| §7.3 | Layout-Schema Alignment | ~150 | 5 MUST rules with code examples | Validation implementation |
| §8 | Validation Requirements | ~300 | Schema validation, categorical checks | Validation code |
| §9 | Error Taxonomy | ~150 | Exception hierarchy, error codes | Error handling |
| §10.1.1 | Safe Metrics Calculation | ~100 | Safe min/max/count patterns | Metrics implementation |
| §14 | Schema Contract Details | ~200 | JSON schema format, precision specs | Schema creation |
| §20 | Anti-Patterns | ~200 | 11 anti-patterns with fixes | Avoid common errors |
| **§21** | **Parser Implementation Template** | ~800 | **HIGHEST VALUE FOR AI** | Step-by-step coding guide |
| §21.1 | Standard Parser Structure | ~200 | 11-step template | Core implementation |
| §21.2 | Validation Phases & Rejects | ~150 | 4-phase validation pattern | Validation logic |
| §21.3 | Tiered Validation Thresholds | ~100 | INFO/WARN/ERROR levels | Threshold implementation |
| §21.6 | Incremental Implementation | ~100 | 3-phase strategy (TXT→CSV→XLSX) | Development workflow |

**New Sections to Add:**

```markdown
## 0. Companion To

This document is the implementation companion to **STD-parser-contracts-v2.0.md**.

**Read the main standard first** for:
- Core contracts (ParseResult, router API)
- Versioning rules (SemVer)
- Architectural decisions

**Use this guide for:**
- Coding parsers (§21 templates)
- Implementing specific features (alias maps, validation)
- Avoiding anti-patterns (§20)

## 1. Overview

Purpose: Provide code-heavy implementation guidance for parser development.

Scope:
- Step-by-step templates
- Code patterns and anti-patterns
- Type handling, metrics, validation
- Format detection and routing

Non-Scope:
- High-level policy (see STD-parser-contracts-v2.0)
- Operational procedures (see RUN-parser-qa-runbook-v1.0)
```

**Total Estimated Lines:** ~2,500 lines

---

### Document 3: RUN-parser-qa-runbook-v1.0.md (~1,400 lines)

**Purpose:** Operational procedures, QA checklists, SLAs  
**Audience:** QA engineers, SREs, parser implementers (pre-coding)  
**Stability:** Medium (updates with process improvements)  
**AI Use Case:** QA validation, metric instrumentation, pre-flight checks

**Sections to Move:**

| Old § | Section Name | Lines | Why Move | AI Value |
|-------|--------------|-------|----------|----------|
| §5.5 | Constraints & SLAs | ~100 | Performance targets, size limits | SLO validation |
| §10 | Observability & Metrics | ~250 | Required metrics, logging format | Metric instrumentation |
| §10.1 | Per-File Metrics | ~100 | Metric structure definition | Metrics code |
| §10.2 | Aggregate Metrics | ~50 | Run-level metrics | Aggregation logic |
| §10.3 | Safe Metrics Calculation | ~100 | Safe min/max/count patterns | Moved to REF? Or both? |
| §11 | Logging Requirements | ~100 | Structured logging format | Logging setup |
| §13 | Compliance & Licensing | ~100 | License tracking, audit trails | Compliance checks |
| §21.4 | Format Verification Pre-Implementation Checklist | ~400 | **CRITICAL RUNBOOK** | Pre-coding verification |
| §21.5 | Golden-First Workflow | ~100 | Development sequence | QA workflow |

**New Sections to Add:**

```markdown
## 0. Companion To

This runbook supports **STD-parser-contracts-v2.0.md** and **REF-parser-implementation-v1.0.md**.

**When to Use This Runbook:**
- Before starting parser implementation (§1 Pre-Implementation)
- During QA/validation phase (§2 Metrics & Logging)
- When troubleshooting (§3 Common Issues)
- Before production deployment (§4 Acceptance Checklist)

## 1. Pre-Implementation Verification

§1.1: Format Inspection Checklist (from §21.4)
§1.2: Variance Analysis (Step 2c)
§1.3: Layout Verification (Step 2b)
§1.4: Decision Tree (proceed/investigate/stop)

## 2. Metrics & Observability

§2.1: Required Per-File Metrics
§2.2: Aggregate Run Metrics
§2.3: Safe Calculation Patterns (could also go in REF)
§2.4: Logging Format & Requirements

## 3. SLAs & Performance Targets

§3.1: Parse Time SLOs
§3.2: Memory Constraints
§3.3: Throughput Requirements

## 4. Production Readiness Checklist

§4.1: Acceptance Criteria
§4.2: Compliance Requirements
§4.3: License Tracking
§4.4: Deployment Gates
```

**Total Estimated Lines:** ~1,400 lines

---

## Section-by-Section Mapping Table

| Current §  | Section Name | Lines | → Destination | Reasoning |
|------------|--------------|-------|---------------|-----------|
| §1 | Summary | 50 | **STD** v2.0 | Core definition |
| §2 | Goals & Non-Goals | 30 | **STD** v2.0 | Strategic policy |
| §3 | Users & Scenarios | 40 | **STD** v2.0 | Context |
| §4 | Key Decisions | 80 | **STD** v2.0 | Immutable architecture |
| §5.1 | Inputs | 100 | **REF** impl | Technical specs |
| §5.2 | Processing Requirements | 400 | **REF** impl | Code patterns |
| §5.2.3 | Alias Map Best Practices | 150 | **REF** impl | Implementation guide |
| §5.2.4 | Defensive Type Handling | 100 | **REF** impl | Code patterns |
| §5.3 | Output Artifacts | 100 | **STD** v2.0 | Contract definition |
| §5.4 | CMS File Type Support | 50 | **STD** v2.0 | Scope definition |
| §5.5 | Constraints & SLAs | 100 | **RUN** runbook | Operational targets |
| §6 | Contracts | 200 | **STD** v2.0 | **CORE POLICY** |
| §7.1 | Router & Format Detection | 300 | **REF** impl | Implementation details |
| §7.2 | Layout Registry | 200 | **REF** impl | Technical reference |
| §7.3 | Layout-Schema Alignment | 150 | **REF** impl | Validation rules |
| §7.4 | CI Test Snippets | 100 | **RUN** runbook | QA procedures |
| §8 | Validation Requirements | 300 | **REF** impl | Validation code |
| §9 | Error Taxonomy | 150 | **REF** impl | Error handling |
| §10 | Observability & Metrics | 250 | **RUN** runbook | Operational metrics |
| §10.3 | Safe Metrics Calculation | 100 | **REF** impl | Code patterns |
| §11 | Logging Requirements | 100 | **RUN** runbook | Operational logging |
| §12 | Compatibility & Versioning | 100 | **STD** v2.0 | Core policy |
| §13 | Compliance & Licensing | 100 | **RUN** runbook | Operational compliance |
| §14 | Schema Contract Details | 200 | **REF** impl | Technical specs |
| §15 | Acceptance Criteria | 50 | **STD** v2.0 | Policy |
| §16 | Out of Scope | 30 | **STD** v2.0 | Boundary definition |
| §17 | Security | 50 | **STD** v2.0 | Policy requirement |
| §18 | References | 20 | **STD** v2.0 | Cross-refs (update to REF/RUN) |
| §19 | Glossary | 50 | **STD** v2.0 | Shared definitions |
| §20 | Anti-Patterns | 200 | **REF** impl | Implementation guidance |
| §21 | Parser Implementation Template | 800 | **REF** impl | **HIGHEST AI VALUE** |
| §21.1 | 11-Step Template | 200 | **REF** impl | Core coding guide |
| §21.2 | Validation Phases | 150 | **REF** impl | Validation patterns |
| §21.3 | Tiered Validation Thresholds | 100 | **REF** impl | Threshold implementation |
| §21.4 | Pre-Implementation Checklist | 400 | **RUN** runbook | **CRITICAL PROCEDURE** |
| §21.5 | Golden-First Workflow | 100 | **RUN** runbook | QA workflow |
| §21.6 | Incremental Implementation | 100 | **REF** impl | Development strategy |
| §21.7 | Acceptance Criteria | 50 | **RUN** runbook | QA gates |

---

## Proposed File Structure

### File 1: STD-parser-contracts-v2.0.md (~600 lines)

```markdown
# Parser Contracts Standard (v2.0)

## 0. Overview
- Purpose: Define immutable parser contracts for DIS ingestion
- Companion Documents:
  - REF-parser-implementation-v1.0.md (code guidance)
  - RUN-parser-qa-runbook-v1.0.md (operational procedures)
- Version: v2.0 (MAJOR - modularized from v1.11)

## 1. Summary
{Current §1 content - 50 lines}

## 2. Goals & Non-Goals
{Current §2 content - 30 lines}

## 3. Users & Scenarios
{Current §3 content - 40 lines}

## 4. Key Decisions & Rationale
{Current §4 content - 80 lines}
- Two-stage transformation (raw → normalize/enrich)
- ParseResult contract (data, rejects, metrics)
- Metadata injection pattern
- Schema-driven precision

## 5. Parser Contracts
{Current §6 content - 200 lines}

### 5.1 ParseResult Contract (Immutable)
def parse_{dataset}(...) -> ParseResult(data, rejects, metrics)

### 5.2 Router Contract
Router selects parser by filename + optional content sniff

### 5.3 Schema Contract Format
JSON schema with columns, types, natural_keys

### 5.4 Metadata Injection Contract
Required fields: release_id, schema_id, product_year, etc.

### 5.5 Integration with DIS Pipeline
How parsers fit into Land → Normalize → Enrich → Publish

## 6. Versioning & Compatibility
{Current §12 content - 100 lines}
- Parser SemVer (v1.2.3)
- Schema SemVer (v1.0)
- Layout SemVer (v2025.4.1)
- Breaking changes policy

## 7. Scope
{Current §5.3, §5.4 - 150 lines}

### 7.1 Output Artifacts
Arrow/Parquet with deterministic row hashes

### 7.2 Supported CMS File Types
TXT, CSV, XLSX, ZIP (with inner file routing)

## 8. Acceptance Criteria
{Current §15 content - 50 lines}

## 9. Out of Scope
{Current §16 content - 30 lines}

## 10. Security
{Current §17 content - 50 lines}

## 11. Glossary
{Current §19 content - 50 lines}

## 12. Cross-References
- REF-parser-implementation-v1.0.md (§21 templates, code patterns)
- RUN-parser-qa-runbook-v1.0.md (checklists, metrics, SLAs)
- STD-data-architecture-prd-v1.0.md (DIS pipeline)
- STD-qa-testing-prd-v1.0.md (QTS compliance)

## 13. Change Log
{Add v2.0 entry: Modularization from v1.11}
```

---

### File 2: REF-parser-implementation-v1.0.md (~2,500 lines)

```markdown
# Parser Implementation Reference Guide (v1.0)

**Companion to:** STD-parser-contracts-v2.0.md  
**Purpose:** Detailed implementation guidance and code patterns  
**Audience:** Engineers coding parsers  
**Type:** Reference (REF-)  

## 0. Overview

This guide provides code-level implementation details for parsers complying with
STD-parser-contracts-v2.0.md.

**Read this for:**
- Step-by-step parser templates (§2)
- Alias maps, type handling, metrics (§3-§7)
- Anti-patterns to avoid (§8)
- Format detection, validation patterns (§9-§12)

**Don't read this for:**
- High-level policy (see STD-parser-contracts-v2.0)
- Pre-implementation checklists (see RUN-parser-qa-runbook-v1.0)

## 1. Quick Start

Follow this sequence:
1. Read STD-parser-contracts-v2.0 (understand contracts)
2. Run RUN-parser-qa-runbook-v1.0 §1 (pre-implementation checks)
3. Use §2 below (11-step template) to implement
4. Apply §3-§7 (alias maps, validation, metrics)
5. Avoid §8 (anti-patterns)
6. Run RUN-parser-qa-runbook-v1.0 §4 (acceptance)

## 2. Parser Implementation Template
{§21.1: 11-Step Standard Structure - 200 lines}
{§21.2: Validation Phases & Rejects - 150 lines}
{§21.3: Tiered Validation Thresholds - 100 lines}
{§21.6: Incremental Implementation Strategy - 100 lines}

## 3. Processing Patterns
{§5.2: Encoding, normalization, casting - 400 lines}
{§5.2.3: Alias Map Best Practices - 150 lines}
{§5.2.4: Defensive Type Handling - 100 lines}

## 4. Format Detection & Routing
{§7.1: Router & Format Detection - 300 lines}
  - Two-phase detection (extension + content sniffing)
  - ZIP handling with inner file routing
  - Flowchart & decision tree
  - Common pitfalls

## 5. Layout Registry
{§7.2: Layout Registry API - 200 lines}
{§7.3: Layout-Schema Alignment - 150 lines}

## 6. Validation Implementation
{§8: Schema validation, categorical checks - 300 lines}

## 7. Error Handling
{§9: Exception hierarchy, error codes - 150 lines}

## 8. Metrics & Safe Calculation
{§10.1.1: Safe Metrics Calculation Patterns - 100 lines}
{§10.3: safe_min_max, safe_count, safe_percentage - patterns}

## 9. Schema Contracts
{§14: JSON schema format, precision specs - 200 lines}

## 10. Anti-Patterns & Fixes
{§20: 11 anti-patterns with code fixes - 200 lines}

## 11. Cross-References
- STD-parser-contracts-v2.0.md (core policy, contracts)
- RUN-parser-qa-runbook-v1.0.md (checklists, metrics, SLAs)
- _parser_kit.py (utility functions)

## 12. Change Log
v1.0 (2025-10-17): Initial implementation guide (split from STD-parser-contracts-v1.11)
```

---

### File 3: RUN-parser-qa-runbook-v1.0.md (~1,400 lines)

```markdown
# Parser QA & Operations Runbook (v1.0)

**Companion to:** STD-parser-contracts-v2.0.md, REF-parser-implementation-v1.0.md  
**Purpose:** Operational procedures, QA checklists, SLAs  
**Audience:** QA engineers, SREs, parser implementers (pre-coding phase)  
**Type:** Runbook (RUN-)  

## 0. Overview

This runbook provides operational procedures for parser development, QA, and production monitoring.

**Use this for:**
- Pre-implementation verification (§1)
- QA acceptance criteria (§2)
- Metrics & observability setup (§3)
- Production SLAs & monitoring (§4)

**Don't use this for:**
- Coding guidance (see REF-parser-implementation-v1.0)
- Policy/contracts (see STD-parser-contracts-v2.0)

## 1. Pre-Implementation Verification
{§21.4: Format Verification Pre-Implementation Checklist - 400 lines}

### 1.1 Step 1: Format Inspection
{Step 1 content}

### 1.2 Step 2a: Verify Fixed-Width Layout Positions
{Step 2a content}

### 1.3 Step 2b: Verify Layout Positions (Tool-Assisted)
{Step 2b content with verify_layout_positions.py}

### 1.4 Step 2c: Real Data Format Variance Analysis
{Step 2c content - NEW from v1.11}

### 1.5 Step 3-7: Header Mapping, Schema Validation, etc.
{Steps 3-7 content}

## 2. QA Workflows
{§21.5: Golden-First Workflow - 100 lines}
{§21.7: Acceptance Criteria - 50 lines}

### 2.1 Golden-First Development
Extract fixture → Write test → Implement → Verify → Commit

### 2.2 Acceptance Checklist
- [ ] All formats parse successfully
- [ ] Schema-compliant output
- [ ] Natural key uniqueness validated
- [ ] Coverage ≥ 90%

## 3. Metrics & Observability
{§10: Observability & Metrics - 250 lines}
{§10.1: Per-File Metrics - 100 lines}
{§10.2: Aggregate Metrics - 50 lines}
{§11: Logging Requirements - 100 lines}

### 3.1 Required Per-File Metrics
{Structure: total_rows, valid_rows, reject_rows, etc.}

### 3.2 Safe Metrics Calculation
{§10.3 content - OR reference REF doc}

### 3.3 Logging Format
{Structured logging with structlog}

## 4. Performance SLAs
{§5.5: Constraints & SLAs - 100 lines}

### 4.1 Parse Time Targets
- Small files (<1MB): < 100ms
- Medium files (1-10MB): < 1s
- Large files (10-100MB): < 10s

### 4.2 Memory Constraints
- Peak RSS < 500MB for standard files
- No memory leaks (run GC between files)

## 5. Compliance & Licensing
{§13: Compliance & Licensing - 100 lines}

### 5.1 License Tracking
{Track CMS licenses, attribution}

### 5.2 Audit Trails
{ingestion_runs table, provenance}

## 6. Production Monitoring
{§10.2 aggregate metrics, alerts}

### 6.1 Alert Thresholds
- Parse failures > 5% → Page
- Performance regression > 20% → Warn

### 6.2 Dashboard KPIs
- Success rate, parse duration, reject rate

## 7. Cross-References
- STD-parser-contracts-v2.0.md (core contracts)
- REF-parser-implementation-v1.0.md (implementation guidance)
- STD-qa-testing-prd-v1.0.md (QTS requirements)

## 8. Change Log
v1.0 (2025-10-17): Initial runbook (split from STD-parser-contracts-v1.11)
```

---

## Modularization Benefits

### For AI (Cursor)

**Current State (4,477 lines in 1 file):**
- ❌ Context window consumed quickly
- ❌ AI struggles to find relevant sections
- ❌ Mixes policy with code with procedures
- ❌ Long response times loading full file

**Proposed State (3 focused files):**
- ✅ Load only STD (~600 lines) for compliance checks
- ✅ Load only REF (~2,500 lines) for code generation
- ✅ Load only RUN (~1,400 lines) for QA/validation
- ✅ 3-4x faster context loading
- ✅ More accurate responses (less noise)

### For Humans

**Current State:**
- ❌ Intimidating (4,477 lines)
- ❌ Hard to find specific guidance
- ❌ Mixes what/why/how in one file

**Proposed State:**
- ✅ Start with STD (600 lines) - understand "what"
- ✅ Code with REF (2,500 lines) - implement "how"
- ✅ Ship with RUN (1,400 lines) - validate "ready"
- ✅ Clear reading order by role/task

---

## Implementation Plan

### Phase 1: Create New Documents (2-3 hours)

**Step 1.1: Extract STD-parser-contracts-v2.0.md**
- Copy sections: §1-4, §6, §12, §15-19
- Add cross-references to REF and RUN
- Update changelog (v1.11 → v2.0 MAJOR bump)
- **Time:** 30 min

**Step 1.2: Create REF-parser-implementation-v1.0.md**
- Move sections: §5.1-5.2, §7-9, §14, §20-21 (except §21.4-21.5)
- Add "Companion to" header
- Add overview and quick start
- **Time:** 60 min

**Step 1.3: Create RUN-parser-qa-runbook-v1.0.md**
- Move sections: §5.5, §10-11, §13, §21.4-21.5, §21.7
- Add "Companion to" header
- Organize by workflow (pre-impl → QA → prod)
- **Time:** 45 min

### Phase 2: Update Cross-References (30-45 min)

**Step 2.1: Update Internal Links**
- STD → REF/RUN references (15 min)
- REF → STD/RUN references (15 min)
- RUN → STD/REF references (15 min)

**Step 2.2: Update External References**
- Update all PRDs referencing STD-parser-contracts
- Add REF-parser-implementation where code cited
- Add RUN-parser-qa-runbook where checklists cited
- **Time:** 30 min

### Phase 3: Register & Validate (15-20 min)

**Step 3.1: Master Catalog Registration**
- Add REF-parser-implementation-v1.0 entry
- Add RUN-parser-qa-runbook-v1.0 entry
- Update STD-parser-contracts entry (v1.11 → v2.0)

**Step 3.2: Validation**
- Run `tools/audit_doc_links.py`
- Run `tools/audit_cross_references.py`
- Verify no broken links

### Phase 4: Archive Old Version (5 min)

**Step 4.1: Archive**
- Rename `STD-parser-contracts-prd-v1.0.md` → `STD-parser-contracts-prd-v1.11-ARCHIVED.md`
- Add deprecation notice at top
- Keep for reference during transition

---

## Migration Strategy

### Immediate (Week 1)
- Create all 3 new documents
- Update Master Catalog
- Test with next parser (validate AI experience)

### Transition Period (2 weeks)
- Both old and new docs coexist
- New work references v2.0 + companions
- Archive old v1.11 after 2 weeks

### Long-Term (Month 1+)
- Delete archived v1.11 file
- All parsers reference modular docs
- Update PRD references complete

---

## Success Criteria

**For AI Context:**
- ✅ Can load STD-parser-contracts-v2.0 in < 1s (vs 3-4s currently)
- ✅ More accurate code generation (REF focused on implementation)
- ✅ Faster QA responses (RUN focused on procedures)

**For Engineers:**
- ✅ Clear reading path: STD (policy) → REF (code) → RUN (qa)
- ✅ Each doc < 3,000 lines (readable in one sitting)
- ✅ Find guidance faster (focused TOC per doc)

**For Governance:**
- ✅ STD versions rarely (policy stable)
- ✅ REF versions frequently (lessons learned)
- ✅ RUN versions occasionally (process improvements)
- ✅ Independent version control

---

## Special Considerations

### §10.3 Safe Metrics Calculation - Which Doc?

**Question:** Should §10.3 go in REF (code patterns) or RUN (metrics runbook)?

**Recommendation:** **Duplicate in both** with different focus:
- **REF**: Code patterns (safe_min_max implementation)
- **RUN**: When to use (metrics calculation procedures)

**Rationale:** 
- Engineers coding need code patterns (REF)
- QA engineers need to know it exists (RUN)
- Content overlap acceptable for critical patterns

### §21.4 Pre-Implementation Checklist - Mostly RUN, Some REF

**Recommendation:** 
- **RUN**: Steps 1-2c, Step 7 (procedures, checklists)
- **REF**: Steps 3-6 (header mapping, schema validation - code-adjacent)

**Alternative:** Keep all in RUN, reference REF for code examples

---

## Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Broken cross-references | High | Comprehensive link audit before merge |
| AI confusion during transition | Medium | Keep old doc for 2 weeks, clear deprecation notice |
| Engineers miss guidance | Medium | Update onboarding docs, add pointers |
| Version drift between docs | Low | Regular sync reviews, companion doc policy |

---

## Decision Tree

**Should we modularize STD-parser-contracts?**

```
Is the doc > 3,000 lines? YES (4,477 lines)
  ↓
Does it mix policy + code + procedures? YES
  ↓
Will AI benefit from focused context? YES (3-4x faster loading)
  ↓
Can we maintain 3 docs effectively? YES (proven with STD-data-architecture + -impl)
  ↓
DECISION: ✅ PROCEED WITH MODULARIZATION
```

**Estimated Total Time:** 3-4 hours  
**Estimated ROI:** Saves 5-10 min per AI interaction × 50+ uses = 4-8 hours saved

---

## Recommendation

**APPROVE modularization with the following priority:**

**High Priority (Do First):**
1. Create STD-parser-contracts-v2.0.md (core policy)
2. Create REF-parser-implementation-v1.0.md (§21 templates)
3. Update Master Catalog

**Medium Priority (Do Second):**
1. Create RUN-parser-qa-runbook-v1.0.md (checklists, metrics)
2. Update cross-references in related PRDs

**Low Priority (Cleanup):**
1. Archive old v1.11 file
2. Update onboarding documentation

**Time Estimate:** 3-4 hours total  
**Best Time:** After Locality parser complete (now!) ✅  

**Would you like me to proceed with this modularization?**
