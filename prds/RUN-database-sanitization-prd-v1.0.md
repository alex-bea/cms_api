# Runbook: Database Sanitization & Non-Prod Refresh

**Status:** Stub v0.1 (in progress)  
**Owners:** Security, Data Engineering, Platform Engineering  
**Consumers:** Service teams, QA, Release Mgmt.  
**Change control:** PR review + Security approval  
**Last updated:** 2025-10-21

**Cross-References**
- `prds/STD-database-platform-prd-v1.0.md` §2 (data parity & masking)  
- `security/phi-registry.yaml` (masking rules – TBD)  
- `prds/STD-incident-response-prd-v1.0.md` (breach response)  
- `prds/RUN-database-migrations-prd-v1.0.md` (dry-run requires sanitized snapshot)

---

## 0. Purpose
Document how to refresh non-production databases with sanitized/tokenized data while complying with PHI policies.

---

## 1. PHI Column Registry Management
- Registry location: `security/phi-registry.yaml`.  
- Format and masking rule definitions.  
- Quarterly review workflow (Security + DBA).  
- CI enforcement: pipeline fails if PHI column lacks masking rule.

*(TODO: provide template, lint script reference, review checklist.)*

---

## 2. Tokenization Pipeline Overview
- High-level architecture (extract → transform → load).  
- Script entrypoint (e.g., `scripts/tokenize_phi.py`).  
- Supported masking rules (hash, redact, cohort, synthetic).  
- Logging/metrics requirements.

*(TODO: link to code, add diagram.)*

---

## 3. Non-Prod Refresh Procedure
1. Restore latest prod backup into isolated staging DB.  
2. Run tokenization pipeline with environment-specific config.  
3. Execute automated PII scan to verify sanitization success.  
4. Drop original backup; retain tokenized copy only.  
5. Promote sanitized DB to staging/dev.  

Checklist includes environment tagging, PHI scanner output, change ticket updates.

*(TODO: include exact commands, environment variables, sample logs.)*

---

## 4. Verification & Compliance Checks
- Automated scanners (e.g., regex, ML-based PII detection).  
- Manual spot checks (QA).  
- Sign-off responsibilities and evidence storage.  

*(TODO: list tooling, acceptance thresholds.)*

---

## 5. Seed Data & Synthetic Fixtures
- Location of deterministic seed scripts (`scripts/seed_*.py`).  
- Versioning strategy (align with schema tag).  
- Process to regenerate fixtures after schema change.  

*(TODO: describe edge cases coverage, integration with tests.)*

---

## 6. Staging Soak & Promotion Gate
- Track staging migration results for ≥24 h.  
- Monitor `no_migration_errors` metric, application health checks.  
- Approval workflow for prod promotion (incident commander exception path).  

---

## 7. Security Controls
- Role/credential requirements in staging/dev (no superuser).  
- Storage of sanitized snapshots (retention limits, access controls).  
- Audit logging of refresh operations.

---

## 8. Tooling Backlog
- Build PHI registry lint (`scripts/lint_phi_registry.py`).  
- Automate PII scanning (integration with CI).  
- Slack notifications for refresh completion/approval.

---

## 9. References & Templates
- Change ticket template for refresh events.  
- PHI registry documentation (link TBD).  
- Incident escalation contacts.  

---

## Change Log
| Version | Date | Summary |
|---------|------|---------|
| v0.1 (stub) | 2025-10-21 | Initial scaffold created. Sections flagged TODO for detailed scripts, validation steps, and compliance evidence templates. |

