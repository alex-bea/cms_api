# Database Platform Standard (STD) — v1.0

**Status:** Draft v1.0  
**Owners:** Platform Engineering, SRE  
**Consumers:** Service teams, Data Engineering, Release Management  
**Change control:** ADR + Architecture review  
**Review cadence:** Quarterly or on major database tooling updates

**Cross-References:**
- `prds/DOC-master-catalog-prd-v1.0.md` (master catalog - register in §3)
- `prds/PRD-render-hosting-prd-v1.0.md` (compute/hosting policy companion)
- `prds/RUN-render-deployment-prd-v1.0.md` (deployment runbook)
- `prds/STD-observability-monitoring-prd-v1.0.md` (monitoring baseline)
- `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (recent migration checklist insight)

---

## 1. Goal & Non-Goals
**Goal.** Establish a migrations-first PostgreSQL platform standard that:
- Prevents schema drift (no runtime `create_all` tables)
- Provides clear guidance for migrations, backups, access, and performance
- Reduces deployment risk and accelerates incident recovery

**Non-Goals.**
- Detailed runbook steps (see corresponding RUN docs)
- Covering non-database storage (object storage, caches) — future work
- Vendor-specific pricing or Render infrastructure decisions (owned by Render PRD)

**Scoping Decision Notes:**
- **Timing:** Continue current Render deployment using the pragmatic fix (stamp head), and develop this standard in parallel to prevent future incidents.
- **Document Type:** Standard (`STD`) so expectations apply to all services using the shared PostgreSQL platform.
- **Schema Evolution Policy:** Migrations-first — all schema changes flow through Alembic migrations; application-startup `metadata.create_all` is prohibited.
- **Relationship to Render PRD:** Companion standard referenced by hosting PRD/runbook; no duplication.
- **Coverage:** PostgreSQL only (current stack); extend to other data stores in future revisions.

---

## 2. Users & Environments

### 2.1 Role Matrix & Responsibilities

- **dba**: platform owner; manages configuration, extensions, backups/PITR, restore drills.
- **migrate**: CI/Render job identity for applying Alembic migrations; no ad‑hoc DDL.
- **app_rw**: application service account for OLTP reads/writes; no DDL, no SUPERUSER.
- **ro**: read‑only role for analytics/ops triage; no writes.
- **audit_reader**: can read audit/event logs in downstream store (not prod DB tables).
- **security_analyst**: can query pgaudit stream/alerts; cannot access raw PHI tables by default.

### 2.2 Access Governance

- Grant **least privilege**; forbid role nesting that effectively grants SUPERUSER.
- **Quarterly access recertification** by Platform + Security; revoke stale credentials.
- Secrets are issued via 1Password/Vault with rotation every **90 days** (prod) and **180 days** (non‑prod).

### 2.3 Environment Expectations

- **dev**: ephemeral DBs permitted; **no live PHI**; use synthetic/tokenized datasets.
- **staging**: production‑sized snapshots allowed **only** after PHI is irreversibly **tokenized/masked** via the standard pipeline; same DB extensions/config as prod.
- **prod**: PHI allowed; HIPAA controls enforced (BAA, RLS, pgaudit, encryption, network isolation).

### 2.4 Data Parity & Seeding

**Production-sized snapshots defined:**
- Staging must match prod schema exactly (same PostgreSQL version, extensions, config)
- Data volume targets: ≥10GB or ≥1M rows per major table
- Refresh cadence: Monthly or after major schema changes

**Seed data requirements:**
- Deterministic seed scripts in `scripts/seed_*.py`
- Versioned with schema (same git tag)
- Include representative edge cases from production
- Document: data source, masking rules applied, coverage percentage

**PHI Column Registry:**
- Location: `security/phi-registry.yaml` (TBD v1.1)
- Format:
  ```yaml
  tables:
    - name: patient_records
      phi_columns:
        - name: ssn
          masking_rule: hash_with_salt
        - name: dob
          masking_rule: age_cohort
  ```
- CI enforcement: Fail build if PHI column lacks masking rule
- Quarterly review and update by Security + DBA

**Non-prod refresh process:**
1. Restore prod backup to isolated staging DB
2. Run tokenization pipeline (`scripts/tokenize_phi.py`)
3. Validate no PHI remains (automated PII scanner)
4. Drop original backup
5. Promote tokenized DB to staging

### 2.5 Promotion Rules

- Schema changes must pass: unit/integration tests, migration dry‑run on a prod‑sized snapshot, and review/approval by Platform.
- **Staging soak**: minimum **24 hours** with no migration errors before prod promotion unless approved by incident commander.

---

## 3. Schema Lifecycle & Migrations
- **Migrations‑first**: All schema changes flow through Alembic; **application‑startup DDL is prohibited**.
- **Forward‑only**: No auto‑generated downgrades in prod; rollbacks are handled via restore/cutover patterns.
- **Online migration patterns** (required for large/locking changes):
  - New table → backfill via batched jobs; maintain parity with triggers; **cutover** with minimal lock.
  - Indexes created/dropped with **CONCURRENTLY** in prod.
  - Avoid `ALTER TYPE` on large columns; use add‑column → backfill → swap.
  - At migration start, set session-local safety timeouts to avoid long locks:
    ```
    SET LOCAL lock_timeout = '5s';
    SET LOCAL statement_timeout = '30s';
    ```
- **Runtime budgets & windows**
  - Any migration estimated > **5 minutes** requires a maintenance window and backout plan.
  - Migrations run via **Render Job/CI** with clear ownership; prohibit ad‑hoc `psql` DDL in prod.
- **`alembic stamp` usage**
  - Allowed only when the physical schema **exactly** matches the target revision; requires Platform approval and recorded change ticket.
- **Pre‑merge checklist**
  - Tests green; lint passes; dry‑run on prod‑sized snapshot completed; performance impact reviewed; RLS policies updated where applicable.

### 3.6 Fresh Database Initialization

For new environments where no schema exists, two patterns are permitted:

**Option A: Migrations-First (Required for Production)**

1. Empty database starts at no revision
2. Run `alembic upgrade head` from revision `None`
3. All tables created via migrations
4. Clean migration history with full audit trail

**Requires:** Migration `000_initial_schema.py` that creates all base tables using `op.create_table()`.

**Option B: Models-First Bootstrap (Development/Staging Only)**

Permitted ONLY when:
- Environment is non-production (dev/staging)
- Platform approval obtained via change ticket
- Documented in deployment log

**Process:**
1. Run `Base.metadata.create_all()` to create tables from SQLAlchemy models
2. Verify physical schema exactly matches target migration revision
3. Run `alembic stamp head` with Platform approval
4. Validation: Compare `\d tablename` output against migration DDL for all tables
5. Document: Which revision was stamped, validation results, approver name

**Validation checklist before stamp:**
```sql
-- Verify all expected tables exist
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;

-- Verify critical indexes exist
SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public';

-- Verify foreign keys
SELECT conname, conrelid::regclass, confrelid::regclass 
FROM pg_constraint WHERE contype = 'f';
```

**Case Study (2025-10-21 Render Deployment):**
- **Issue:** Models had v1.3 schema; migrations assumed v1.2 existed
- **Resolution:** `create_all()` + `stamp head` after validation
- **Documented:** `.cursor/RENDER_DEPLOYMENT_LOG.md`
- **Lesson:** Migrations-first prevents this conflict; use for all future environments

**Transition Plan:**

For codebases currently using models-first (detected via `Base.metadata.create_all` in app startup):
1. Create migration `000_initial_schema.py` that recreates all current tables via `op.create_table()`
2. Remove `create_all()` from application startup
3. Add CI check: fail if `Base.metadata.create_all` detected in non-test code
4. Document transition in ADR

### 3.7 Naming Conventions & Validation

**Index naming standard:**
- Pattern: `idx_{table}_{column(s)}` or `uq_{table}_{column(s)}` for unique constraints
- Index names MUST be globally unique across ALL tables
- Use abbreviated table names if full name causes length issues

**Example violation (detected 2025-10-21):**
```python
# WRONG: Same index name on different tables
class OPPSCap:
    __tablename__ = "opps_caps"
    __table_args__ = (Index("idx_opps_effective", "effective_start", "effective_end"),)

class FeeOPPS:
    __tablename__ = "fee_opps"
    __table_args__ = (Index("idx_opps_effective", "effective_from", "effective_to"),)  # Duplicate name!

# CORRECT: Table-specific index names
class OPPSCap:
    __tablename__ = "opps_caps"
    __table_args__ = (Index("idx_opps_cap_effective", "effective_start", "effective_end"),)

class FeeOPPS:
    __tablename__ = "fee_opps"
    __table_args__ = (Index("idx_fee_opps_effective", "effective_from", "effective_to"),)
```

**Constraint naming:**
- Foreign keys: `fk_{source_table}_{referenced_table}_{column}`
- Unique constraints: `uq_{table}_{column(s)}`
- Check constraints: `ck_{table}_{condition_summary}`
- Primary keys: `pk_{table}` (or let PostgreSQL auto-generate)

**CI enforcement (required before v1.2):**
- Pre-commit hook: Parse all models and migration files
- Extract index/constraint names
- Fail on duplicates with clear error message
- Tool location: `scripts/lint_schema_names.py` (TBD)

**Length limits:**
- PostgreSQL identifier max: 63 characters
- Recommended max: 50 characters (allows prefixes/suffixes)
- Use abbreviations: `locality_id` → `loc`, `effective` → `eff`

---

## 4. Change Management & Access Control
- **Approvals**: All schema/data changes require PR review by Platform + owning service team; change ticket linked.
- **Roles & grants**
  - `migrate` can `CREATE/ALTER/DROP` within schema; `app_rw` limited to DML; `ro` can `SELECT` only.
  - Grants are explicit per schema; **PUBLIC** privileges revoked on PHI tables.
  - **Search path & PUBLIC hardening**
    ```
    ALTER DATABASE :db SET search_path = app, public;
    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    ```
- **RLS by default (mandatory on PHI tables)**
  - Enable RLS on PHI tables; ship a standard tenant isolation policy; block‑by‑default.
  - Example:
    ```sql
    ALTER TABLE public.phi_example ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.phi_example FORCE ROW LEVEL SECURITY;
    CREATE POLICY tenant_isolation ON public.phi_example
    USING (tenant_id = current_setting('app.tenant_id')::uuid);
    REVOKE ALL ON public.phi_example FROM PUBLIC;
    ```
  - **Tenant context under PgBouncer**: The application MUST set 
  `SET LOCAL app.tenant_id = :tenant` at the start of **each transaction** (e.g., SQLAlchemy event hook). Avoid session-level 
  `SET` due to transaction pooling.
- **Extensions policy**
  - **Allow**: `pgcrypto`, `pg_trgm`, `btree_gin`, `pg_stat_statements`, `pgaudit`.
  - **Deny unless exception**: `dblink`, unvetted FDWs, `COPY TO PROGRAM`.
  - **Export guardrails**: Allow 
  `COPY TO STDOUT` only for `migrate` and a dedicated `export_ro` role; forbid `COPY ... PROGRAM` entirely; large exports must run as audited jobs.
- **Secrets & rotation**
  - Store credentials in Vault/1Password; rotate on schedule (see §2) and on personnel change.

---

## 5. Backup, Restore & Disaster Recovery
- **Automated backups**
  - Nightly full backups per environment with retention: **dev 7d**, **staging 14d**, **prod 35d**.
  - **PITR** enabled via WAL archiving; WAL retained **35d** in prod.
- **Immutable copies**
  - Store backup manifests and audit logs in **WORM/immutable storage** with object‑lock; maintain audit log retention for **≥ 6 years**.
- **Pre‑risk guardrail**
  - Run `pg_dump` (schema-only or full per risk) before high-risk migrations; store alongside change ticket. Use a dump tool version that matches the server version to ensure restore compatibility.
- **RTO/RPO**
  - Prod targets: **RTO ≤ 2h**, **RPO ≤ 15m**. Non‑prod: best effort.
- **Restore drills**
  - Perform quarterly restore drills for prod; document outcomes and gaps in the runbook.
- **Rollback vs restore**
  - Use decision tree in RUN doc; prefer **online cutover/backfill** over destructive rollback.

---

## 6. Performance & Capacity Management
- **Connection management**
  - PgBouncer required in prod; set `max_connections` and pool mode per environment; coordinate with SQLAlchemy pools.
  - **Prepared statements in pooling**: In PgBouncer transaction pooling, disable server-side prepares or configure the driver accordingly (e.g., set `prepareThreshold=0`) to avoid prepared-statement reuse across backends.
- **Baseline Postgres settings (OLTP)**
  - Timeouts: `statement_timeout=30s`, `lock_timeout=5s`, `idle_in_transaction_session_timeout=60s`, `deadlock_timeout=1s`.
  - Logging: `log_min_duration_statement=200ms`, `log_statement='ddl'`, `log_lock_waits=on`, `log_temp_files=0`.
  - Observability: enable `pg_stat_statements` and `pgaudit`.
  - Reference `postgresql.conf` starter:

    ```
    # Timeouts
    statement_timeout = '30s'
    lock_timeout = '5s'
    idle_in_transaction_session_timeout = '60s'
    deadlock_timeout = '1s'

    # Logging & observability
    log_min_duration_statement = 200
    log_statement = 'ddl'
    log_lock_waits = on
    log_temp_files = 0
    shared_preload_libraries = 'pg_stat_statements,pgaudit'
    pg_stat_statements.max = 10000
    pg_stat_statements.track = all

    # pgaudit (scope PHI)
    pgaudit.log = 'read,write,ddl,role,grant'
    pgaudit.log_client = off
    pgaudit.log_parameter = off
    ```

- **Indexing & concurrency**
  - Require `CREATE/DROP INDEX CONCURRENTLY` in prod; monthly index review; remove zombie/unused indexes.
- **Partitioning & lifecycle**
  - Use native partitioning (time/tenant) for large PHI sets; cold partitions archived; define TTL/retention per table class.
- **Autovacuum tuning**
  - Table‑class profiles with tuned `autovacuum_*` settings; maintain index bloat SLOs; schedule `VACUUM (ANALYZE)` post‑bulk loads.
- **SLOs**
  - Track p95 read/write latency, lock wait, deadlock rate; tie alerts and scaling actions to SLO breaches.
- **Analytics & CDC**
  - No heavy analytics on OLTP; define CDC to HIPAA‑eligible warehouse; mask/minimize PHI in downstreams.

---

## 7. Observability & Alerting
- **Required metrics**: migration duration, queue depth, slow query rate, connection pool saturation, autovacuum activity, disk/WAL usage.
- **Logging/audit**
  - Enable `pg_stat_statements`; ingest to dashboards weekly for top N queries and regressions.
  - Enable `pgaudit` for SQL‑level auditing on PHI tables; stream to immutable/WORM storage; retain **≥ 6 years**.
  - Require clients to set 
  `application_name` (per service and role) for traceability in slow-query and audit triage.
  - Application logging: disable SQL/ORM parameter logging in prod (`sqlalchemy.engine` logger at WARNING, `echo=False`); do not log bind values.
- **Alerts**
  - Slow query SLO breach, replication/WAL backlog growth, autovacuum freeze risk, unusual full‑table scans on PHI tables, large exports.
- **Dashboards & on‑call**
  - Tie alerts to on‑call rotations; link to playbooks in `prds/STD-observability-monitoring-prd-v1.0.md`.

---

## 8. Security & Compliance
- **HIPAA vendor gates (BAA required before PHI)**
  - **Render**: Use HIPAA‑enabled workspace; BAA signed and archived; PHI is prohibited until enabled.
  - **Observability (e.g., Datadog)**: Use HIPAA‑eligible plan/features with BAA; disable non‑allowed features; no PHI in logs beyond what the BAA permits.
- **Encryption & keys**
  - In transit: TLS 1.2+ enforced; client TLS recommended for admin access.
  - At rest: AES‑256 with KMS‑managed keys; document rotation and separation of duties (DBA ≠ Key admin).
- **Network isolation**
  - Private networking only; no public DB endpoints; IP allowlists; bastion/VPN for admin access.
- **Data classification**
  - Maintain **PHI Column Registry** with masking/tokenization rules; apply **minimum necessary** access.
  - **CI enforcement**: The pipeline fails if any column tagged `phi: true` in `security/phi-registry.yaml` lacks an associated masking rule.
- **Non‑prod policy**
  - No live PHI in dev/stage; use deterministic tokenization/synthetic data pipeline for refreshes.
- **Auditing & retention**
  - pgaudit enabled on PHI tables; audit/event logs retained **≥ 6 years** in immutable storage; quarterly access‑to‑logs review.
- **Incident response**
  - Map DB signals (pgaudit anomalies, export thresholds) to the IR playbook, including breach notification steps.

---

## 9. Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation | Owner |
|------|--------|------------|------------|-------|
| **Schema drift (models vs migrations mismatch)** | Deployment failures, data corruption, rollback required | High if models-first | Enforce migrations-first via CI linting; prohibit `create_all` in app startup; add pre-commit hook to detect `Base.metadata.create_all` outside test code | Platform |
| **Failed migration in production** | Service downtime, data loss, emergency rollback | Medium | Dry-run on prod-sized snapshot before every prod migration; enforce 5-minute runtime budget; require backout plan for risky changes; migration job with timeout control | Release Mgmt |
| **Stale or untested backups** | Catastrophic data loss on restore attempt | Low | Quarterly restore drills with pass/fail criteria; automated backup validation (test restore to ephemeral DB); alert on backup age > 36 hours | SRE |
| **Privilege creep** | Unauthorized data access, compliance violation, security breach | Medium | Quarterly access recertification by Platform + Security; automated role audit script; remove stale users; log all grant/revoke operations via pgaudit | Security |
| **Long-running migration blocks deploys** | Deployment delays, lock contention, user-facing errors | Medium | Require online migration patterns (CREATE INDEX CONCURRENTLY); set `lock_timeout=5s`; maintenance windows for >5min migrations; backout plan tested in staging | Platform |
| **Duplicate index/constraint names across tables** | `create_all()` failures, migration conflicts, deployment blocked | Low (after fix) | CI pre-commit hook validates global uniqueness of index/constraint names; enforce naming convention `idx_{table}_...`; lint models and migrations | Engineering |
| **Connection pool exhaustion** | 500 errors, service degradation, cascading failures | Medium | PgBouncer required in prod; monitor pool saturation; alert at 80%; autoscaling rules; circuit breaker pattern in application | SRE |
| **Backup restore version mismatch** | Restore fails due to PostgreSQL version incompatibility | Low | Match `pg_dump` tool version to server version; test restores in staging; document upgrade paths; pin PostgreSQL major version per environment | DBA |

**Known gaps for future work (post v1.0):**
- Cross-region database replication (not yet implemented; planned for geographic HA)
- Multi-tenant RLS in complex joins (pattern needs definition with performance testing)
- Redis/object storage policies (PostgreSQL-only for v1.0; expand in v1.1)
- CDC to data warehouse (architecture TBD; requires HIPAA-eligible warehouse selection)

---

## 10. Acceptance Criteria (go-live checklist)
- **BAAs in place**: Render HIPAA workspace enabled; observability vendor on HIPAA plan; signed BAAs archived.
- **RLS enforced** on all PHI tables; test users demonstrate tenant isolation.
- **FORCE RLS** present on all PHI tables.
- **Tenant isolation test** suite passes (tenant A cannot read tenant B; table owner restricted by RLS).
- **pgaudit enabled**; audit stream verified in immutable storage; **≥ 6‑year** retention set.
- **TLS enforced** end‑to‑end; KMS keys configured with documented rotation.
- **PITR validated**; latest quarterly restore drill passed; RTO/RPO documented.
- **PgBouncer configured**; baseline `postgresql.conf` applied; SLO dashboard live.
- **Migrations job** runs in CI/Render with forward‑only policy; dry‑run on prod‑sized snapshot completed.
- **Secrets** stored in Vault/1Password; rotation schedule active; stale creds revoked.
- **Extension policy** enforced (allow/deny list); no unapproved extensions in prod.

---

## 11. References

**Implementation Guides:**
- `prds/RUN-render-deployment-prd-v1.0.md` (Render deployment, Parts 1-8 including CI/CD)
- `prds/RUN-database-operations-prd-v1.0.md` (TBD v1.1 - migration procedures, restore playbooks, pg_dump workflows)

**Related Standards:**
- `prds/PRD-render-hosting-prd-v1.0.md` (compute/hosting policy, service packaging, deploy triggers)
- `prds/STD-observability-monitoring-prd-v1.0.md` (monitoring requirements, SLO framework)
- `prds/STD-data-architecture-prd-v1.0.md` (data pipeline standards, DIS lifecycle)
- `prds/STD-api-security-and-auth-prd-v1.0.md` (API-level security, authentication patterns)

**Operational Checklists:**
- `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (migration example with backfill script)
- `.cursor/RENDER_DEPLOYMENT_LOG.md` (2025-10-21 deployment log with stamp head decision)
- `prds/RUN-global-operations-prd-v1.0.md` (operational runbooks)

**Security & Compliance:**
- `security/phi-registry.yaml` (TBD v1.1 - authoritative PHI column registry with masking rules)
- `prds/STD-incident-response-prd-v1.0.md` (TBD v1.1 - IR playbook, breach notification mapping)
- HIPAA compliance documentation (external, maintained by Compliance team)

**Architecture Decisions:**
- ADR-001: Migrations-First Enforcement (TBD - document 2025-10-21 decision and transition plan)
- ADR-002: Alembic Stamp Policy (TBD - codify approval criteria from §3.6)
- ADR-003: Database Naming Conventions (TBD - formalize §3.7 patterns)

**Code & Scripts:**
- `scripts/create_tables.py` (bootstrap helper for models-first transition)
- `scripts/lint_schema_names.py` (TBD v1.1 - CI validation for duplicate names)
- `scripts/seed_*.py` (deterministic seed data per dataset)
- `scripts/tokenize_phi.py` (TBD v1.1 - PHI masking pipeline for non-prod)

**Future Expansions (Planned for v1.1+):**
- Redis caching platform standard (security, expiration, eviction policies)
- Object storage standard (Parquet artifacts, encryption, lifecycle, access patterns)
- Multi-region database replication (failover, consistency, geo-distribution)
- Cross-service CDC patterns (change data capture, event streaming)

**Master Catalog:**
- `prds/DOC-master-catalog-prd-v1.0.md` (register this standard in §3)

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| **1.1** | 2025-10-21 | Added fresh database initialization patterns (§3.6) including models-first bootstrap with stamp approval criteria and validation checklist; schema naming conventions with duplicate detection (§3.7); complete risk table with 8 production risks and mitigations (§9); full references with ADR links, scripts, and future work roadmap (§11); expanded data parity with quantified staging requirements (§2.4); restructured §2 with five subsections for clarity. Based on critical learnings from Render deployment: models vs migrations conflict, duplicate index names (`idx_opps_effective`), alembic stamp decision criteria. Case study from 2025-10-21 deployment documented in §3.6. |
| **1.0.1** | 2025-10-21 | Added HIPAA-ready controls (BAA gating, RLS-by-default, pgaudit + 6-year immutable audit retention), optimization baselines (timeouts, logging, PgBouncer, partitioning, index hygiene), and strengthened DR/observability requirements. |
| **1.0** | 2025-10-21 | Initial database platform standard drafted in response to migration/resourcing issues observed during GPCI v1.3 deployment. |

---

**End of STD-database-platform-prd-v1.0.md**
