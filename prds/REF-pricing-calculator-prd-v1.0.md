Perfect — here’s a complete prds/REF-pricing-calculator-prd-v1.0.md you can drop into your repo.
It’s written to complement the new MPFS PRD and clarify that the Pricing Calculator is a runtime service layer built on top of DIS-curated inputs.

⸻


# REF: Pricing Calculator (v1.0)

**Status:** Adopted v1.0 (2025-11-06)  
**Owners:** Pricing Platform Engineering  
**Consumers:** Pricing API, Analytics, Ops  
**Change Control:** ADR + PR review  

**Cross-References:**  
- **PRD-mpfs-prd-v1.0.md** — MPFS ingestion inputs  
- **STD-data-architecture-prd-v1.0.md** — DIS lifecycle & validation  
- **STD-api-security-and-auth-prd-v1.0.md** — API security controls  
- **STD-qa-testing-prd-v1.0.md** — Calculator QA parity tests  
- **DOC-master-catalog-prd-v1.0.md** — Catalog registration (Product Docs §4)  
- **REF-cms-pricing-source-map-prd-v1.0.md** — Source lineage for RVU, GPCI, CF  

---

## 1. Purpose
Define the runtime pricing calculator architecture used by the Pricing API to compute **Medicare Physician Fee Schedule (MPFS)** amounts on-request from authoritative input datasets.  
This reference ensures parity with CMS methodology while keeping the ingestion pipeline simple and maintenance-free.

---

## 2. Context
The **MPFS ingestor** stores authoritative datasets (`mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`).  
The **Pricing Calculator** consumes these datasets at runtime to produce dollar amounts when a user or downstream service requests a price.

Design goal:  
> **Compute on-request, not on-ingest.**  
> Keep ingestion lightweight and always up-to-date; let the calculator layer apply formulas dynamically with caching.

---

## 3. Inputs and Dependencies
| Source Dataset | Description | Key Fields |
|----------------|-------------|-------------|
| `mpfs_rvu` | Relative Value Units + status indicators | hcpcs, modifier, work_rvu, pe_rvu, mp_rvu |
| `mpfs_gpci` | Geographic Practice Cost Indices | locality_id, gpci_work, gpci_pe, gpci_mp |
| `mpfs_cf_vintage` | Conversion Factors by year | year, cf_physician, cf_anesthesia |
| `nearest_zip_locality` | Resolver mapping ZIP → locality | zip, locality_id |

All inputs originate from validated DIS snapshots with immutable digests.

---

## 4. Calculation Logic

### 4.1 Formula
```text
Payment = CF × [(Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP)]

  • CF → conversion factor for the applicable calendar year
  • Work/PE/MP RVUs → from MPFS RVU file
  • GPCI → from locality table based on ZIP resolver
  • Facility vs Non-Facility → chosen by place_of_service at runtime

4.2 Facility Logic

If place_of_service ∈ {office, home, ASC}: use non-facility PE_RVU
Else: use facility PE_RVU

4.3 Effective Dating
  • Use effective_from / effective_to from each dataset to select correct vintage.
  • Default fallback = most recent effective record ≤ request date.

⸻

5. Runtime Architecture
  1.  Input fetch — Retrieve RVU, GPCI, and CF records using snapshot digests.
  2.  Join & compute — Apply the formula dynamically per request.
  3.  Cache — Store computed result by (hcpcs, modifier, zip, year, pos) for 24 h.
  4.  Return payload — Include datasets_used, digests, and computed payment.

Diagram (text)

API Request → Calculator Engine → 
  ├─ Fetch mpfs_rvu
  ├─ Fetch mpfs_gpci via locality resolver
  ├─ Fetch mpfs_cf_vintage
  └─ Compute & cache → JSON response


⸻

6. Caching Strategy

Layer Cache Key TTL Invalidation
Memory / Redis  (hcpcs, modifier, zip, year, pos) 24 hours  Digest mismatch or dataset refresh
Local digest cache  dataset_name → manifest_digest  1 hour  Updated manifest in S3 / Render DB
API response cache  HTTP 304 via X-Dataset-Digest 1 hour  Client invalidation or new digest


⸻

7. API Contract Summary

Endpoint

GET /pricing/codes/price

Query Parameters

Name  Type  Description
code  string  HCPCS or CPT code
modifier  string  Optional modifier
zip string  Patient or facility ZIP
setting string  Facility / Non-Facility / POS code
year  integer Calendar year (defaults to current)

Response

{
  "code": "99213",
  "price": 93.45,
  "datasets_used": ["mpfs_rvu", "mpfs_gpci", "mpfs_cf_vintage"],
  "digests": {
    "mpfs_rvu": "sha256:...",
    "mpfs_gpci": "sha256:...",
    "mpfs_cf_vintage": "sha256:..."
  },
  "locality": "Los Angeles, CA",
  "metadata": {
    "cf_physician": 32.3465,
    "effective_from": "2025-01-01"
  }
}


⸻

8. Quality & Parity Assurance
  • API-level parity testing: Compare sampled API results against CMS Look-Up Tool for top 100 HCPCS codes per quarter.
  • Tolerance: ± $0.01 difference allowed (rounding).
  • Automation: QA pipeline tests/api/test_pricing_parity.py runs nightly.
  • Observability: Log datasets_used, latency, and parity drift metrics.

⸻

9. Error Handling

Condition Response  Notes
Missing input snapshot  503 Service Unavailable Dataset digests not ready
Invalid ZIP 400 Bad Request ZIP resolver failed
Missing HCPCS 404 Not Found No RVU entry for code
Internal error  500 Internal Server Error Logged with trace ID


⸻

10. Security & Compliance
  • Internal-only service, protected by token auth per STD-api-security-and-auth-prd-v1.0.md.
  • All external surfaces must mask or aggregate outputs per compliance guidance.
  • Dataset digests recorded in logs for traceability.

⸻

11. Operations & Monitoring
  • Metrics: latency, cache hit-rate, parity drift %, error count.
  • Logs: include batch_id, dataset_digests, and response time.
  • Alerts: trigger if parity drift > 1 % or dataset snapshot missing > 24 h.
  • Health endpoint: /health/pricing returns current dataset digests and last parity test timestamp.

⸻

12. Roadmap
  • Add anesthesia CF support (separate calculator path).
  • Support mid-year CF adjustments automatically.
  • Extend to OPPS / site-neutral comparisons.
  • Enable configurable rounding policies by payer type.
  • Introduce streaming cache warm-up for top codes.

⸻

End of Document

---

### ✅ Summary
- Fully matches your **on-request calculation** model.  
- Clearly distinguishes between *DIS ingestion* (inputs) and *runtime computation* (calculator).  
- Includes API behavior, caching, parity QA, and operations guidance.

Would you like me to generate a diff patch so this file is added automatically to your repo under `prds/REF-pricing-calculator-prd-v1.0.md`?
