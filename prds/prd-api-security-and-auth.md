# API Security & Auth PRD (v1.0)

## 0) Overview
This PRD defines the **Security & Authentication standard** for the CMS Pricing API. It covers identity, key lifecycle, tenant isolation, rate limiting, observability, compliance posture, migration, and acceptance criteria. It aligns with the **API Architecture & Layering PRD v1.0** (thin routers, services, repositories, engines).

**Status:** Draft v1.0 (for approval)  
**Owners:** Platform Eng (API Guild) + Security  
**Consumers:** Product, Engineering, Ops, Legal  
**Change control:** ADR + Architecture Board review  

---

## 1) Goals & Non-Goals
**Goals**
- Simple, robust **API key** auth for machine clients; **user auth** only for Admin/Key UI.
- **Strict tenant isolation** at the repository layer; safe defaults everywhere.
- Clear **rate limits, quotas, and error codes** with first-class observability.
- SOC 2–friendly logging and controls; HIPAA-ready posture (no PHI v1).

**Non-Goals**
- OAuth2/JWT for external APIs (deferred to v2).
- Database Row-Level Security (RLS) in v1 (optional in v2 for defense-in-depth).
- PHI handling in v1.

---

## 2) Scope
- Applies to all **external HTTP APIs** and **internal admin/key management UI**.
- Covers **transport**, **authn/z**, **key mgmt**, **tenant isolation**, **rate limiting**, **monitoring**, **runbooks**, **migration**, and **compliance evidence**.

---

## 3) AuthN/Z Model

### 3.1 Subjects & Roles
- **Subjects**
  - **API clients (machine):** Organization/tenant scoped via **API Keys**.
  - **Human users:** Admin/Key Management UI only (email + MFA; SSO later).
- **Roles (per key):** `read-only`, `read-write`, `admin`, `billing`.  
  **One role per key**. Organizations may issue **multiple keys** by environment/purpose.

### 3.2 API Key Format & Entropy
- **Format (final):** `cms_<env>_<tenantSlug>_<secret>`  
  - `env ∈ {sbx, dev, stg, prod}`  
  - `<secret>` = **256-bit CSPRNG**, Base62/URL-safe Base64, **≥43 chars**.
- **Key ID (KID):** Internal UUID; expose **last 6 chars** for UX. Never log full keys.

### 3.3 Key States & Lifecycle
- **States:** `active | disabled | expired | compromised` (immutable audit trail).
- **Rotation:** Self-serve/UI + API; **zero-downtime** via **overlap window** (old/new valid for ≤24h).
- **Expiration:** **Indefinite by default**, optional `expires_at` per key. Rotation **recommended quarterly**.
- **Display:** Full key shown **once** at creation; never retrievable thereafter.

---

## 4) Storage, Crypto & Transport

### 4.1 Key Storage
- **DB:** Postgres table `api_keys` stores **Argon2id hash**, per-record **salt** and **params**, metadata (tenant_id, role, env, created_at, expires_at, state, last_used_at), **KID**, and **suffix** (last 6).  
- **Encryption:** Disk encryption + **column-level envelope encryption** (KMS) for hash/salt/params.
- **Cache:** Optional Redis cache keyed by **hash** (or KID→hash) for hot lookups.

### 4.2 TLS & Transport
- **TLS:** Require **TLS 1.2+ (prefer 1.3)**, AEAD ciphers only, **HSTS (preload eligible)**, OCSP stapling.  
- TLS termination at **edge/CDN/WAF**, not on app nodes.  
- **mTLS (Enterprise)** optional; **IP allowlists** per tenant optional.

---

## 5) Tenant Isolation & Data Access

### 5.1 Tenant Model
- **Tenant:** Contracted organization/billing entity.  
- Optional **sub-tenants/accounts** via `parent_tenant_id`, but all operational filtering uses **concrete tenant_id**.

### 5.2 Isolation & Sharing
- **Isolation:** **Repository-enforced**; all methods require non-nullable `tenant_id`. Static checks forbid `tenant_id=None`.  
- **Cross-tenant reads:** **Forbidden (403)**.  
- **Partner sharing:** Explicit **data-sharing grants** that **duplicate/materialize data** into recipient tenant scope (no dynamic cross-tenant joins).

### 5.3 Reference vs Tenant Data
- **Tenant-scoped:** custom plans, uploads, jobs, results, usage, billing artifacts.  
- **Global shared:** CMS reference datasets, code sets, geography (read-only, cached).  
- **History:** Tenants may access their **full historical** artifacts and all **public reference vintages**.

---

## 6) Rate Limiting, Quotas & Fairness

### 6.1 Token Buckets
- **Scope:** **Per-tenant** buckets (prevents evasion via multiple keys). Per-endpoint overrides.  
- **Defaults (v1):**  
  - Burst: **6,000/min**  
  - Sustained: **60,000/hour**  
  - Heavy endpoints (bulk, comparisons, job submit): ≤ **300/min**  
  - **Concurrent jobs cap:** default **10** per tenant
- **Headers:** `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `Retry-After` on 429.

### 6.2 Quotas, Plans, Billing
- **Tiered monthly quotas** (e.g., Dev 100k, Pro 1M, Enterprise custom).  
- Track usage now for potential **usage-based billing**.  
- **Alerts:** Tenant notifications at **80%/95%/100%** of quota and on sustained 429s.  
- **Dashboards:** Tenant self-serve + internal real-time.

### 6.3 Credits Model (Prepaid/Hybrid)

**Decision:** Adopt a **Hybrid** model: subscription tiers with included monthly credits **plus** optional prepaid credit packs for overages/spikes.

**What is a credit?** A unit roughly proportional to compute/IO cost. Costs are **bounded** per call.

| Endpoint | Cost (credits) | Notes |
|---|---:|---|
| GET /reference/:code | 1 | Heavily cached |
| GET /reference/batch?limit<=100 | 50 | Bounded batch |
| POST /compare (sync) | 10 | Moderate compute |
| POST /bulk-jobs (submit) | 100 | Base submission |
| Bulk processing | +50 / 1k rows processed | Meter during execution |
| Webhooks | 0 | No charge; counted under primary action |

**Headers (responses):**
- `X-Credits-Remaining` — tenant wallet post-charge
- `X-Credits-Cost` — credits charged for this request
- `X-Credits-Source` — `subscription|pack|promo`

**Exhaustion behavior:** When balance ≤ 0 → return **402 PAYMENT_REQUIRED** (see Error Catalog) after a configurable **soft-grace** (default: 1% of last month’s usage).

**Why this matters:** Monetizes expensive endpoints fairly, gives cash-up-front via packs, and preserves simple rate-limit fairness separate from economics.

---

### 6.4 Tenant Tiers (Quota, Bursts, Features)

**Dev (Free):** 100k credits/mo, 300 req/min burst, no SLA, 1 sandbox key, no IP allowlist, no mTLS.  
**Pro:** 1M credits/mo, 6,000 req/min burst, standard support (NBD), IP allowlist, webhook HMAC, credit packs allowed.  
**Enterprise:** Contracted credits (incl. unlimited w/ fair use), custom bursts, **SSO (Admin UI)**, optional **mTLS**, Security review, SLA.

**Why this matters:** Aligns engineering controls (rate limits, features) with commercial packaging and sets clear upgrade paths.

---

## 7) Requests, Idempotency & Abuse Controls

### 7.1 Standard Headers
- **Request:** `X-API-Key`, `Idempotency-Key` (for mutating endpoints), `X-Correlation-Id` (optional).  
- **Response:** `X-RateLimit-*`, `Retry-After`, `X-Correlation-Id`.  
- **Cache-Control:** `no-store` for auth-sensitive responses.

### 7.2 Idempotency
- Required for **POST/PUT/PATCH** routes that are state-changing/expensive.  
- **24h TTL**, body-hash binding; replay returns original response; conflict (same key, different body) → **409**.

### 7.3 Abuse Guardrails
- **Max payload** 5–10MB (configurable); parse timeouts; reject compressed bombs.  
- **Pagination caps** (`limit ≤ 1,000`); backpressure via 429.  
- Brute-force: IP/tenant backoff; soft-lock on repeated fails (no CAPTCHA on API; CAPTCHA on UI only).  
- **DDoS:** CDN/WAF + cloud-provider protections; global denylist with 24h auto-expire.

### 7.4 Charging Semantics (Idempotency & Failures)

- **Idempotent retries:** Same `Idempotency-Key` + same body → charge **once**.
- **Failures:** No charge for 5xx. 4xx are **not charged** unless heavy work occurs *after* validation (avoid by validating early).
- **Async jobs:** Charge base cost **at submission**; incremental charges **per 1k rows processed**. If cancelled before processing starts → refund base cost.
- **429:** Rate-limited requests **do not** consume credits.

**Why this matters:** Prevents accidental double-billing and keeps incentives aligned with good client behavior.

---

## 8) Authorization & Enforcement

### 8.1 Ingress Middleware
- **SecurityMiddleware** validates key, parses prefix, finds tenant, checks **state** and **expiry**, loads role, enriches `request.state.auth = {kid, tenant_id, role, env}` and `request.state.key_hash`.  
- On failure → **401**; do not disclose whether “missing vs invalid vs revoked” (see error catalog).

### 8.2 Repository Guard
- All repository methods require **tenant_id** param; reject if missing.  
- Negative tests ensure **cross-tenant read/write is impossible**.

---

## 9) Admin/Key Management UI

- **Auth:** Email/password + **MFA required**; **SSO (SAML/OIDC)** in Enterprise tier (v2).  
- **Features:** Issue/disable/rotate keys; set optional expiry; list keys (KID + suffix + role + env + state).
- **Secrets:** Full key shown once; list view displays **KID** + **suffix**.  
- **CORS:** Deny by default; if enabling for UI, allow **explicit origins only** (never `*` with credentials).

### 9.1 Tech Stack & Hosting (Admin UI)

**Decision:** **React + TypeScript** (Vite) + Tailwind (or shared tokens), served as static assets behind the existing FastAPI auth/API.  
**Rationale:** Mature component ecosystem, type safety for privileged flows, and fast iteration.  
**Ops:** Deployed via CI; protected by MFA; origin-locked CORS.

**Why this matters:** Reduces risk of defects in sensitive key/credit operations and accelerates future consoles (usage, anomaly views).

---

## 10) Observability, Logging & Alerting

### 10.1 Structured Logging (metadata only)
- Log **decision code** (`AUTH_OK`, `AUTH_INVALID_KEY`, `AUTH_EXPIRED_OR_REVOKED`, `RATE_LIMITED`, `TENANT_FORBIDDEN`, etc.), `tenant_id`, `kid`, `env`, **path template** (no PII), method, status, latency, size, and reason.  
- **Sampling:** 100% of failures, sample successes (~5%).  
- **No bodies or secrets** in logs.

### 10.2 Metrics & Traces
- OTEL spans with auth decision attributes; Prometheus counters/histograms for auth successes/failures, 401/403/429, and per-endpoint latencies.

### 10.3 Alerts
- Triggers: surges in 401/403, 429 spikes, anomalous geo/IP, key used from multiple countries within short window (**key-leak**), admin actions (key create/delete), WAF blocks.  
- Notify ops/security on-call; tenants notified for their own anomalies and quota breaches.

### 10.4 Prometheus Metrics (Security & Credits)

**Extend existing counters/histograms** with labels and add a minimal credit/security set.

- Extend:
  - `REQUEST_COUNT{tenant, endpoint, decision}`  // decision = AUTH_OK|AUTH_INVALID_KEY|RATE_LIMITED|...  
  - `REQUEST_DURATION_BUCKETS{tenant, endpoint}`

- New:
  - `AUTH_DECISIONS_TOTAL{decision}`
  - `RATE_LIMIT_EVENTS_TOTAL{endpoint, tier}`
  - `KEY_STATE_CHANGES_TOTAL{state}`
  - `TENANT_QUOTA_USAGE{tenant}` (gauge, from exporter)
  - `CREDITS_CONSUMED_TOTAL{endpoint, tier}`
  - `CREDITS_BALANCE{tenant}` (gauge)
  - `CREDITS_EXPIRING_30D{tenant}` (gauge)

**Why this matters:** Makes auth and economics first-class signals for incident response and SOC 2 evidence.

#### Implementation (FastAPI / main.py – snippet)

```python
# metrics.py
from prometheus_client import Counter, Histogram, Gauge

AUTH_DECISIONS_TOTAL = Counter(
    "auth_decisions_total", "Auth decisions by type", ["decision"]
)
RATE_LIMIT_EVENTS_TOTAL = Counter(
    "rate_limit_events_total", "Rate limit triggers", ["endpoint", "tier"]
)
KEY_STATE_CHANGES_TOTAL = Counter(
    "key_state_changes_total", "API key state transitions", ["state"]
)
CREDITS_CONSUMED_TOTAL = Counter(
    "credits_consumed_total", "Credits consumed per endpoint", ["endpoint", "tier"]
)
CREDITS_BALANCE = Gauge(
    "credits_balance", "Current tenant credit balance", ["tenant"]
)
CREDITS_EXPIRING_30D = Gauge(
    "credits_expiring_30d", "Credits expiring in next 30 days", ["tenant"]
)
#### Alerting (Prometheus alert rules – snippet) 
groups:
- name: security-and-credits
  rules:
  - alert: SuspectedKeyLeak
    expr: rate(auth_decisions_total{decision="AUTH_OK"}[5m]) > 0
          and (sum by(tenant) (count_values("src_ip", label_replace(up==1, "src_ip","$1","instance","(.*)"))) > 3)
    for: 10m
    labels: { severity: critical }
    annotations:
      summary: "Key used from >3 distinct countries in 10m ({{ $labels.tenant }})"

  - alert: TenantLowCredit
    expr: credits_balance < 0.05 * (avg_over_time(credits_consumed_total[30d]) / 30)
    for: 15m
    labels: { severity: warning }
    annotations:
      summary: "Tenant {{ $labels.tenant }} low credit balance"

  - alert: RateLimitStorm
    expr: increase(rate_limit_events_total[5m]) > 100
    for: 5m
    labels: { severity: warning }

10.5 Alert Routing (Channels)
	•	Security: #security-alerts — key leaks, WAF blocks, anomalous auth patterns.
	•	Ops: #ops-oncall — availability, 5xx, limiter storms.
	•	Tenant: Email/webhook to tenant owners for quota 80/95/100% and credit low/cap events.

Why this matters: Avoids alert fatigue and ensures the right responders act quickly.
---

# 🧾 Insert into **“## 13) Error Catalog (public)”** table (add a new row)

```md
| 402  | `PAYMENT_REQUIRED`       | “Insufficient credits. Please top up or reduce usage.” |

#> 402 includes headers `X-Credits-Remaining: 0` and a link to top-up documentation.


## 10.6 Alert Channels

- Slack `#security-alerts`: critical auth anomalies, suspected key leaks, WAF blocks
- Slack `#ops-oncall`: availability, 5xx spikes, limiter storms
- Tenant notifications: email/webhook to tenant owners for quota thresholds and low/zero credits

**Why this matters:** Delivers the right signal to the right people quickly and avoids on-call noise.



---

## 11) Compliance & Retention

- **Posture:** No PHI v1; **HIPAA-ready controls** (encryption, access, audit).  
- **SOC 2 Type II** target in 12–18 months; maintain control mapping to CCs, quarterly access reviews for key-management roles, change-management records.  
- **Retention:** Security/audit logs ≥ **13 months**.  
- **Offboarding:** Purge tenant data within **30 days** of termination, keeping minimal immutable audit records.

---

## 12) Migration & Rollout

- **Legacy keys:** Import → map tenant → store **Argon2id hash**; accept legacy format during **60–90 day** grace period with deprecation header.  
- **Rollout plan:** Shadow-mode (log-only) → canary tenants → full cutover.  
- **DR:** Backups encrypted; **RTO ≤ 4h**, **RPO ≤ 1h**; quarterly restore test of key store.

---

## 13) Error Catalog (public)
All responses use the standard error envelope from the Architecture PRD.

| HTTP | Code                     | Message (example)                      |
|-----:|--------------------------|----------------------------------------|
| 401  | `AUTH_INVALID_KEY`       | “Invalid authentication credentials.”  |
| 401  | `AUTH_EXPIRED_OR_REVOKED`| “Authentication credentials expired.”  |
| 403  | `TENANT_FORBIDDEN`       | “Operation is forbidden for tenant.”   |
| 403  | `INSUFFICIENT_ROLE`      | “Insufficient permissions.”            |
| 429  | `RATE_LIMITED`           | “Rate limit exceeded.”                 |
| 413  | `REQUEST_TOO_LARGE`      | “Payload exceeds maximum size.”        |
| 409  | `IDEMPOTENCY_CONFLICT`   | “Idempotency key conflict.”            |
| 400  | `VALIDATION_ERROR`       | “Invalid request parameters.”          |
| 5xx  | `INTERNAL_ERROR`         | “Unexpected server error.”             |

> Responses deliberately **do not distinguish** “key not found vs invalid” to prevent enumeration.

---

## 14) Example Contracts & Schemas

### 14.1 OpenAPI (excerpt)
```yaml
components:
  securitySchemes:
    ApiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key

security:
  - ApiKeyAuth: []

parameters:
  IdempotencyKey:
    name: Idempotency-Key
    in: header
    required: false
    schema: { type: string, maxLength: 128 }
  CorrelationId:
    name: X-Correlation-Id
    in: header
    required: false
    schema: { type: string }

responses:
  RateLimited:
    description: Rate limit exceeded
    headers:
      Retry-After: { schema: { type: string }, description: Seconds to wait }

14.2 api_keys (DDL excerpt)

create table api_keys (
  id uuid primary key,
  tenant_id uuid not null,
  env text not null check (env in ('sbx','dev','stg','prod')),
  role text not null check (role in ('read-only','read-write','admin','billing')),
  key_hash bytea not null,          -- Argon2id digest
  key_salt bytea not null,
  argon_params jsonb not null,      -- {time_cost, memory_cost, parallelism}
  suffix char(6) not null,          -- last 6 chars for display
  state text not null check (state in ('active','disabled','expired','compromised')),
  created_at timestamptz not null default now(),
  last_used_at timestamptz,
  expires_at timestamptz,
  constraint uq_tenant_suffix unique (tenant_id, suffix)
);

14.3 FastAPI Middleware (sketch)

async def security_middleware(request: Request, call_next):
    key = request.headers.get("X-API-Key")
    ctx = AuthContext(anonymous=True)
    if key:
        kid, auth_decision = await auth_service.validate_key(key)  # Argon2id verify, state, expiry
        if auth_decision.ok:
            ctx = AuthContext(
                anonymous=False,
                tenant_id=auth_decision.tenant_id,
                role=auth_decision.role,
                env=auth_decision.env,
                kid=kid,
            )
            request.state.auth = ctx
        else:
            return error_401(auth_decision.public_code)
    else:
        return error_401("AUTH_INVALID_KEY")

    # attach correlation id
    cid = request.headers.get("X-Correlation-Id", str(uuid4()))
    request.state.correlation_id = cid
    response = await call_next(request)
    response.headers["X-Correlation-Id"] = cid
    return response





14.4 OpenAPI — 402 Error

```yaml
responses:
  PaymentRequired:
    description: Insufficient credits to process the request
    headers:
      X-Credits-Remaining:
        schema: { type: integer }
        description: Remaining credit balance after the attempt
      Link:
        schema: { type: string }
        description: URL to top-up instructions
    content:
      application/json:
        schema:
          type: object
          properties:
            error:
              type: object
              properties:
                code: { type: string, example: PAYMENT_REQUIRED }
                message: { type: string, example: Insufficient credits. Please top up or reduce usage. }

## 14A) Credits Data Model (DDL excerpts)

```sql
-- Wallet holds current balance; parent/child sharing optional via parent_tenant_id
create table credit_wallets (
  tenant_id uuid primary key,
  balance bigint not null default 0,               -- credits
  currency text not null default 'USD',
  updated_at timestamptz not null default now()
);

-- Immutable ledger for all credit movements (audit-friendly)
create table credit_ledger (
  id uuid primary key,
  tenant_id uuid not null,
  delta bigint not null,                           -- +pack, -consumption, +refund
  reason text not null,                            -- 'consume','pack','refund','promo','expire'
  endpoint text,                                   -- normalized path template
  request_id uuid,                                 -- for idempotency
  idempotency_key text,
  source text not null,                            -- 'subscription','pack','promo'
  expires_at timestamptz,                          -- for pack/promo
  created_at timestamptz not null default now()
);

-- Prepaid packs purchased
create table credit_packs (
  id uuid primary key,
  tenant_id uuid not null,
  purchased_credits bigint not null,
  price_cents integer not null,
  purchased_at timestamptz not null default now(),
  expires_at timestamptz not null
);

create index on credit_ledger (tenant_id, created_at);
create index on credit_ledger (request_id);

---
# Why this matters: Supports accurate billing, refunds, audits, and revenue recognition (deferred revenue on prepaid packs until consumed/expired).
# 🗺️ Add **Migration Details** after the existing Migration section

```md
## 12A) Migration Details (Keys & Timeline)

**Current key inventory:** Assume **mixed formats**. Migration importer will ingest all known keys, tag `source_format`, and rehash to Argon2id.

**Legacy validator:** Enabled behind a **feature flag** for **90 days**; requests validated by both systems during **shadow mode**.

**Timeline:**  
- **T-0 → T-14:** Shadow mode in prod; announce + open rotation UI.  
- **T-60:** Enforce new key format on **prod**; legacy blocked.  
- **T-75:** Enforce on **sandbox** (extra time for partners).

**Rollback:** Keep legacy validator and dual-write enabled during window; revert via config toggle if critical issues arise.

**Why this matters:** Predictable, auditable path that reduces partner breakage and gives early signals before enforcement.

⸻

15) Admin/Key UI Requirements (MVP)
	•	Create/disable/rotate keys; set optional expiry; list keys (KID + suffix + role + env + state).
	•	Enforce MFA; audit who did what, when, and why (free-form reason).
	•	Usage dashboard: requests by endpoint/day, 4xx/5xx rates, 429 events, quota progress.
	•	Alerts preferences (email/webhook) per tenant.

⸻

16) Webhook Authentication (Future-proof spec)
	•	Header: X-Gaia-Signature: sha256=<sig>; X-Gaia-Kid: <uuid>; X-Gaia-Timestamp: <unix>.
	•	Verify HMAC over (timestamp | method | path | bodySHA256); ±5 min skew; single-use nonce cache; rotateable webhook secrets.

⸻

17) Acceptance Criteria (must pass to ship)

Auth & Keys
	•	Keys are 256-bit, CSPRNG; Argon2id-hashed; never stored in plaintext; KID persisted; full value shown once.
	•	Key states enforced; transitions audited.
	•	Rotation overlap works without downtime (tests cover replay).

Isolation & Repos
	•	Static check forbids repository methods that accept tenant_id=None.
	•	Negative tests prove cross-tenant reads/writes are impossible.

Rate Limiting
	•	Per-tenant + per-endpoint token buckets; headers present; 429 includes Retry-After.
	•	Concurrent job caps enforced; heavy endpoints use lower limits.

Idempotency
	•	24h TTL; body-hash bound; replay returns cached response; conflict → 409.

Observability
	•	Structured logs with decision code, tenant_id, kid, env, path template, status, latency; no bodies/secrets.
	•	OTEL spans exported; dashboards show auth success/fail, rate-limit stats.

Security Posture
	•	TLS policy enforced; HSTS enabled; edge/CDN/WAF in front.
	•	CORS deny-by-default; if enabled, explicit origin allowlist only.
	•	Max payload and pagination caps enforced.

Compliance & DR
	•	Audit/security logs retained ≥13 months.
	•	DR restore test of key store meets RTO ≤ 4h / RPO ≤ 1h.
	•	Control mapping to SOC 2 CCs maintained; quarterly access reviews done.

Migration
	•	Legacy keys accepted for 60–90 days with deprecation header; rehashed on import.
	•	Shadow-mode validation deployed prior to enforcement; canary rollout completed.

⸻

18) Implementation Plan (phased)

Phase 1 — Foundations (Week 1–2)
	•	Argon2id key store, SecurityMiddleware, repo guardrails, rate limiter, idempotency cache.
	•	Structured logging, OTEL, basic dashboards.

Phase 2 — Reliability (Week 3–4)
	•	Admin/Key UI with MFA, rotation overlap, usage dashboards, alerts, DR restore test.

Phase 3 — Governance (Week 5–6)
	•	Pen test, WAF tuning, SOC 2 control mapping & evidence pipeline, legacy deprecation cutover.

## 18A) Security Testing & Suites

**Penetration testing:** **Required pre-GA** for Auth surface & Admin UI; annual thereafter and after material changes.

**Dedicated test suites:**
- Tenant isolation (negative tests)
- Brute-force/backoff behaviors
- Idempotency replay window & body-hash binding
- Rate limit correctness (`X-RateLimit-*`, `Retry-After`)
- Key lifecycle (states, overlap rotation)

**Why this matters:** Prevents regressions in high-impact areas and satisfies SOC 2 evidence needs.

⸻

19) ADRs Required
	•	ADR-SA-001: API key lifecycle & hashing parameters.
	•	ADR-SA-002: Tenant data-sharing via materialized views.
	•	ADR-SA-003: Rate-limit defaults & per-endpoint overrides.
	•	ADR-SA-004: Idempotency TTL & body-hash binding.
	•	ADR-SA-005: Logging fields & sampling policy.

⸻

20) Appendix — Example Error Envelopes

{
  "error": {
    "code": "AUTH_EXPIRED_OR_REVOKED",
    "message": "Authentication credentials expired."
  },
  "trace": {
    "correlation_id": "b6c2c1a6-6e5c-4f2a-9d1c-1c8e9a3e2f4d"
  }
}

{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Rate limit exceeded."
  },
  "trace": {
    "correlation_id": "5e27f1a2-2e6c-4a1e-9b98-8d41c7c2c5a1"
  }
}

# 3) (Optional) Commit message you can use

chore(prd): split Security & Auth into standalone PRD
	•	Remove embedded Security & Auth section from api_architecture_layering_prd_v_1.md
	•	Add pointer to new file
	•	Add new prds/api_security_auth_prd_v1.md (v1.0)

21) SLA (Enterprise Tier)

- **Uptime:** 99.9% monthly (≤ 43m 49s downtime/month)
- **Performance:** p95 < 300 ms for reference endpoints; p99 < 600 ms
- **Support:** L1 response within 4 business hours; P3 within 24h
- **Credits for breach:** Applied to next invoice per SLA addendum

**Why this matters:** Clear commitments for enterprise procurement and internal SLO targets for engineering.


## 22) Operational Runbooks

### 22.1 Security Incident Response Playbook

**Incident Types & Response:**
- **Key Leak:** Immediate key revocation, tenant notification, forensic analysis
- **DDoS Attack:** CDN/WAF activation, rate limiting, tenant communication
- **Brute Force:** IP blocking, tenant notification, security review
- **Data Breach:** Immediate containment, legal notification, forensic analysis

**Escalation Procedures:**
- **Level 1:** Alex (Security Team) notification within 15 minutes
- **Level 2:** Alex (CTO/Head of Security) notification within 30 minutes
- **Level 3:** Executive team notification within 1 hour

**Communication Templates:**
- **Tenant Notification:** Incident summary, impact assessment, remediation steps
- **Internal Status:** Incident timeline, response actions, next steps
- **Public Communication:** If required, coordinated with legal/PR

### 22.2 Disaster Recovery Procedures

**DR Testing Schedule:**
- **Monthly:** Key store backup verification (AWS RDS automated backups)
- **Quarterly:** Full DR restore test (RTO ≤ 4h, RPO ≤ 1h) using AWS RDS point-in-time recovery
- **Annually:** Complete system failover test with AWS Multi-AZ failover

**DR Procedures:**
- **Backup Verification:** AWS RDS automated daily backups with integrity checks
- **Restore Testing:** Quarterly restore to staging environment using RDS snapshots
- **Failover Process:** AWS Multi-AZ automated failover with manual verification
- **Communication:** DR status updates to stakeholders via existing alert system

### 22.3 Tenant Onboarding Procedures

**New Tenant Onboarding:**
- **Contract Review:** Alex handles legal review of tenant agreement
- **Security Assessment:** Alex conducts risk assessment and compliance review
- **Initial Key Generation:** Alex creates first API key with appropriate role
- **Documentation:** Tenant receives API documentation and usage guidelines
- **Monitoring Setup:** Tenant-specific monitoring and alerting configuration

**Tenant Offboarding:**
- **Data Retention:** 30-day data retention with immutable audit records
- **Key Revocation:** Immediate revocation of all tenant API keys
- **Data Purging:** Secure deletion of tenant data within 30 days
- **Audit Trail:** Maintain immutable audit records for compliance

---

## 23) Cross-PRD Integration

### 23.1 QTS Integration (QA Testing Standard)

**Security Testing Requirements:**
- **Unit Tests:** ≥90% coverage on auth middleware and key validation
- **Integration Tests:** Tenant isolation, rate limiting, idempotency
- **Contract Tests:** All security endpoints with Schemathesis
- **Security Tests:** Penetration testing, brute force protection, key leak detection

**Test Categories:**
- **Auth Tests:** Key validation, role enforcement, tenant isolation
- **Rate Limiting Tests:** Token bucket behavior, quota enforcement
- **Security Tests:** Attack simulation, vulnerability scanning
- **Compliance Tests:** Audit logging, data retention, access controls

### 23.2 Scraper Integration

**Scraper Authentication:**
- **Internal Scrapers:** Use internal API keys with `admin` role (RVU, OPPS, MPFS, etc.)
- **External Scrapers:** Use tenant-specific keys with `read-only` role (not accessed by end users)
- **Rate Limiting:** Scrapers subject to same rate limits as external APIs
- **Monitoring:** Scraper usage tracked separately from external API usage

**Scraper Security:**
- **Key Rotation:** Scrapers must support key rotation without downtime
- **Error Handling:** Scrapers must handle auth failures gracefully
- **Audit Logging:** All scraper API calls logged with tenant context

### 23.3 DIS Integration (Data Ingestion Standard)

**Security Observability Integration:**
- **Freshness:** Security data freshness monitoring (key usage, auth failures)
- **Volume:** Security event volume tracking (auth attempts, rate limits)
- **Schema:** Security data schema validation (key format, audit logs)
- **Quality:** Security data quality metrics (failed auths, key leaks)
- **Lineage:** Security data lineage tracking (key creation, usage, revocation)

**Security Metadata:**
- **Key Metadata:** Creation time, last used, usage patterns, risk score
- **Tenant Metadata:** Security posture, compliance status, risk assessment
- **Audit Metadata:** Security events, access patterns, anomaly detection

---

## 24) Compliance Procedures

### 24.1 SOC 2 Control Mapping

**Control Categories:**
- **CC6.1:** Logical access security software, infrastructure, and architectures
- **CC6.2:** Prior to issuing system credentials and access, management reviews and approves
- **CC6.3:** Management implements controls to prevent or detect system access by unauthorized individuals
- **CC6.4:** Management restricts access to information assets based on business need
- **CC6.5:** Management implements controls to prevent or detect unauthorized access to systems

**Evidence Collection:**
- **Access Reviews:** Quarterly review of API key access and usage
- **Change Management:** All security changes documented and approved
- **Incident Response:** Security incidents documented and reviewed
- **Training:** Security awareness training for all personnel

### 24.2 Quarterly Access Reviews

**Review Process:**
- **Key Inventory:** Complete inventory of all API keys and their access
- **Usage Analysis:** Review of key usage patterns and anomalies
- **Role Validation:** Verification that key roles match business needs
- **Risk Assessment:** Evaluation of key risk and security posture

**Review Participants:**
- **Security Team:** Alex conducts technical review of key security and usage
- **Business Owners:** Validation of business need for key access
- **Compliance Team:** Alex handles verification of compliance requirements
- **Management:** Alex approves access decisions and risk acceptance

### 24.3 Audit Log Access and Retention

**Log Access Controls:**
- **Security Team:** Alex has full access to security logs and audit trails
- **Compliance Team:** Alex has read-only access to compliance-related logs
- **Management:** Alex has summary access to security metrics and trends
- **External Auditors:** Controlled access during audit periods via Alex

**Retention Policies (Based on Current Infrastructure):**
- **Security Logs:** 13 months retention with immutable storage (PostgreSQL + S3)
- **Audit Logs:** 7 years retention for compliance requirements (PostgreSQL runs/snapshots tables)
- **Access Logs:** 3 years retention for operational purposes (structlog JSON + S3)
- **Error Logs:** 1 year retention for debugging and analysis (structlog + observability system)
- **Application Logs:** 13 months retention (matches RETENTION_MONTHS=13 config)
- **Trace Data:** 7 years retention (runs/run_trace tables for audit compliance)

---

## 25) Scale & Capacity Planning

**Tenant count (Year 1):** Plan for **hundreds**; Year 2 target **low thousands**.  
**Request volume:** Budget **50k–250k req/day per Pro tenant**; **1–2M/day** across all tenants initially.  
**Topology:** **Single write region + global CDN/WAF** (v1), edge caching for reference data; multi-region writes deferred to v2.

**Why this matters:** Right-sizes caches, metric cardinality, and limiter defaults without premature complexity.

