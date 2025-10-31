# ClearBill Product Requirement Document (v1.0)

doc_type: PRD  
normative: false  
requires:  
  - STD-doc-governance-prd-v1.0.2#overview  

**Status:** Draft v1.0  
**Owners:** Product Operations  
**Consumers:** Engineering, Product, Compliance  
**Change control:** ADR + PR Review  

---

## 0. Overview
This Product Requirement Document (PRD) defines **ClearBill**, a unified program combining the **CMS Pricing API** and the **Medical Bill Review App**.  
ClearBill provides transparent, verifiable, and negotiable healthcare cost information for patients, advocates, and developers.  
This PRD conforms to the repository's Documentation Governance Standard and will be registered in the Master Catalog.

---

## 1. Executive Summary
ClearBill makes healthcare billing transparent, verifiable, and negotiable. It integrates CMS pricing datasets (MPFS, OPPS, GPCI) into a consumer-friendly platform that helps users verify charges, benchmark costs, and negotiate medical bills.  
The system exposes a robust API for developers and a HIPAA-compliant web app for end users.

---

## 2. Problem Context
Medical billing in the United States is fragmented, opaque, and error-prone.  
Patients often receive inflated or confusing bills and lack access to trustworthy cost benchmarks.  
Existing CMS data is public but not usable; datasets are unstructured, inconsistent, and hard to interpret.  
ClearBill solves this by unifying CMS pricing data into a clean API and pairing it with a consumer-ready application for bill review and negotiation support.

---

## 3. Product Vision & Goals
**Vision:**  
A transparent healthcare pricing ecosystem where every bill is verifiable and every patient can advocate for fair costs.

**High-Level Goal:**  
Provide accurate, transparent, and actionable pricing insights that reduce out-of-pocket costs and promote fairness in billing.

| **Objective** | **Key Results** |
|---------------|-----------------|
| Improve data reliability | >=99.9% API uptime; <500ms latency |
| Increase pricing accuracy | >=95% OCR accuracy on medical bills |
| Expand transparency | CMS coverage for >=90% of common services |
| Deliver financial relief | >=15% average savings per disputed bill |
| Grow adoption | >=10,000 active monthly users within 12 months |

---

## 4. User Personas & Use Cases

### **Consumers**
Individuals reviewing medical bills for overcharges or insurance mismatches.  
**Needs:** Understand charges, verify fairness, and negotiate payments.  
**Use Case:** Upload bill -> identify overcharges -> auto-generate negotiation email.

### **Advocates**
Patient advocates or nonprofit organizations assisting multiple users.  
**Needs:** Batch processing, reporting, and communication tracking.  
**Use Case:** Manage several clients -> download variance reports -> track outcomes.

### **Developers**
Build or integrate ClearBill data and tools into third-party products.  
**Needs:** Reliable, versioned CMS API with easy authentication and documentation.  
**Use Case:** Use `/v1/compare` endpoint to power an external cost estimator.

### **Providers**
Hospitals or billing offices verifying or adjusting charges.  
**Needs:** Access to authoritative reference pricing to ensure compliance.  
**Use Case:** Query API to validate CPT/HCPCS reimbursement ranges.

---

## 5. Functional Requirements

### 5.1 CMS Pricing API

**Description:**  
The ClearBill API provides normalized, version-controlled CMS pricing data.

**Capabilities:**
- Access Medicare Physician Fee Schedule (MPFS) and Outpatient Prospective Payment System (OPPS) data.  
- Adjust by Geographic Practice Cost Index (GPCI).  
- Compare billed vs. benchmark prices for any CPT/HCPCS code.  
- Return provenance metadata for traceability.  
- Secure access via token authentication and role-based permissions.

**Example Endpoints:**

```
GET /v1/mpfs?code=99213&year=2025&locality=CA01
GET /v1/opps?apc=5071&year=2025
POST /v1/compare
{
  "cpt_code": "99213",
  "billed_amount": 185.00,
  "zip": "94114"
}
```

**Acceptance Criteria:**
- Latency <500ms for single-code lookups.  
- Deterministic responses (same input -> same result).  
- Versioned datasets with provenance logs (CMS source, release date).  
- 99.9% uptime and JSON Schema validation.

**Priority:** P0

---

### 5.2 Medical Bill Review App

**Description:**  
A consumer web app that lets users upload, analyze, and negotiate medical bills securely.

**Capabilities:**
- OCR extraction from PDFs or photos of bills and EOBs.  
- Code normalization and matching to CMS datasets.  
- Visual variance charts (billed vs. CMS fair price).  
- Negotiation email generator with tone customization.  
- Dispute tracking dashboard.

**Acceptance Criteria:**
- OCR accuracy >=95%.  
- Supports both hospital bills and insurance EOBs.  
- Displays pricing within +-5% of CMS data for valid codes.  
- HIPAA-compliant upload, storage, and consent process.

**Priority:** P1

---

## 6. System Architecture

```mermaid
graph TD
  U1[User (Consumer/Advocate)] --> F1[Web UI (Next.js/React)]
  F1 --> B1[Backend API (FastAPI/GraphQL)]
  B1 --> D1[(CMS Pricing API)]
  B1 --> D2[(User + Bill Database)]
  B1 --> S1[Security Layer (Auth + Audit)]
  F1 -->|HTTPS| B1
  B1 -->|Query| D1
  B1 -->|Read/Write| D2
  S1 -->|Enforces| B1
```

Stack Components:
- Frontend: Next.js + Tailwind (responsive, secure upload).  
- Backend: FastAPI/GraphQL with Celery workers for OCR and normalization.  
- Data: Postgres + Delta Lake for CMS data versioning.  
- Security: JWT auth, audit logging, encryption (AES-256), role-based access control.  
- Infra: AWS EKS, S3, and GitHub Actions CI/CD for version control and automated deployments.

---

## 7. Compliance & HIPAA Framework

| Safeguard Type | Control | HIPAA Reference |
|----------------|---------|-----------------|
| Administrative | Access policy, training, role definitions | §164.308(a)(3) |
| Technical | Encryption in transit (TLS 1.2+) and at rest (AES-256) | §164.312(a)(2)(iv) |
| Physical | Cloud infrastructure isolation (AWS HIPAA-eligible services) | §164.310(a)(1) |
| Audit | Immutable logs, timestamped access trails | §164.312(b) |
| Consent | Explicit consent before upload or sharing | §164.508 |

All PHI-handling workflows include:
- De-identification where possible.  
- Automatic redaction of patient names/IDs in analytics.  
- 180-day data retention (default).  
- BAA agreements with all data processors.

---

## 8. Success Metrics

| Metric Category | KPI | Target |
|-----------------|-----|--------|
| Reliability | API uptime | >=99.9% |
| Accuracy | OCR + normalization accuracy | >=95% |
| Engagement | Avg. session time | >=5 minutes |
| Financial Impact | Avg. user savings | >=15% per bill |
| Adoption | Active monthly users | >=10,000 |
| Compliance | Security audit pass rate | 100% |

---

## 9. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| OCR misreads codes or amounts | Incorrect comparisons | Continuous ML retraining, manual correction UI |
| CMS data structure changes | API inconsistency | Version locking and schema regression tests |
| Breach of PHI | Legal and trust loss | Full encryption, BAA, access logging |
| Low consumer trust | Poor adoption | Empathetic UX and transparent data provenance |
| Scalability under load | Performance degradation | Load testing and horizontal scaling via Kubernetes |

---

## 10. Roadmap

| Quarter | Phase | Milestone |
|---------|-------|-----------|
| Q1 2026 | Phase 1 - API Launch | Deploy CMS Pricing API (MPFS + OPPS + GPCI) |
| Q2 2026 | Phase 2 - App Alpha | Limited release for internal testers |
| Q3 2026 | Phase 3 - Public Beta | Consumer and advocate dashboard, negotiation generator |
| Q4 2026 | Phase 4 - General Launch | Public rollout, compliance certification, partner SDK |

---

## 11. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| 2025-10-17 | v1.0 (Draft) | Product Operations | Initial creation of ClearBill PRD with API and app definition, HIPAA framework, and roadmap. |

---

Registered in DOC-master-catalog-prd-v1.0.md under Product PRDs.

---

Governance Compliance Checks:
- Filename pattern `PRD-clearbill-prd-v1.0.md`.  
- Header metadata included.  
- Numbered structure with Overview through Change Log.  
- Cross-reference to upstream STD doc.  
- End-of-doc Master Catalog registration.  
- No normative RFC 2119 keywords (normative: false).

