# Issue 1: Deployment & Re-load Steps

## ✅ Completed
- [x] Fixed column name mismatch in RVU ingestor
- [x] Committed and pushed to GitHub
- [ ] Render auto-deploys (or trigger manual deploy)
- [ ] Re-run ingestion
- [ ] Load to simplified gpci table

---

## Step 1: Wait for Render Deployment

Check Render dashboard for deployment status. Once deployed, proceed to Step 2.

**Alternative:** If auto-deploy is disabled, trigger manually via Render dashboard or:

```bash
# Trigger via Render API (if configured)
curl -X POST "https://api.render.com/v1/services/{service_id}/deploys" \
  -H "Authorization: Bearer $RENDER_API_KEY"
```

---

## Step 2: Re-run GPCI Ingestion

Once deployed, run the ingestion script in Render:

**Render One-Off Job:**
```bash
python scripts/load_rvu_to_production.py
```

This will:
- Re-parse GPCI files with the fixed column mapping
- Load to `gpci_indices` with **actual GPCI values** (not NULL)
- Should see ~109 rows with populated work_gpci, pe_gpci, mp_gpci

---

## Step 3: Verify Data Loaded

Check that GPCI values are now populated:

```python
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex
db = SessionLocal()
indices = db.query(GPCIIndex).all()
print(f'Total rows: {len(indices)}')
sample = indices[0] if indices else None
if sample:
    print(f'Sample - work_gpci: {sample.work_gpci}, pe_gpci: {sample.pe_gpci}, mp_gpci: {sample.mp_gpci}')
non_null_count = sum(1 for idx in indices if idx.work_gpci and idx.pe_gpci and idx.mp_gpci)
print(f'Rows with GPCI values: {non_null_count}/{len(indices)}')
db.close()
"
```

Expected: All 109 rows should have non-NULL GPCI values.

---

## Step 4: Load to Simplified gpci Table

Once `gpci_indices` has real data, load to the simplified table:

**Render One-Off Job:**
```bash
python scripts/load_gpci_from_indices.py
```

Or use the inline script (if file not deployed yet):
```python
python << 'EOF'
# [paste the updated inline script from artifacts/issue1_render_load_gpci_inline.py]
EOF
```

---

## Step 5: Final Verification

Verify both tables have data:

```python
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex
from cms_pricing.models.fee_schedules import GPCI
db = SessionLocal()
print(f'gpci_indices: {db.query(GPCIIndex).count()} rows')
print(f'gpci (simplified): {db.query(GPCI).count()} rows')
# Check provenance
gpci_sample = db.query(GPCI).first()
if gpci_sample:
    print(f'Sample gpci - release_id: {gpci_sample.release_id}, batch_id: {gpci_sample.batch_id}')
db.close()
"
```

Expected:
- `gpci_indices`: 109 rows with GPCI values
- `gpci`: ~100-109 rows (after deduplication) with provenance

---

## Success Criteria

- [ ] Render deployment completed
- [ ] GPCI ingestion re-run successfully
- [ ] `gpci_indices` has 109 rows with non-NULL GPCI values
- [ ] `gpci` table has 100-109 rows with provenance
- [ ] Pricing engines can now use GPCI data

---

## Troubleshooting

If GPCI values are still NULL after re-running:
1. Check ingestion logs for errors
2. Verify source files are accessible
3. Check parser output column names match expected format
4. Review `cms_pricing/ingestion/ingestors/rvu_ingestor.py:4196-4198` for column mapping logic

