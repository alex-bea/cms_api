# Check Database Columns Directly

Run this SQL query in Render to see what columns actually exist:

```sql
-- Check actual column values in gpci_indices
SELECT 
  mac, 
  locality_id, 
  effective_start,
  work_gpci,
  pe_gpci,
  mp_gpci
FROM gpci_indices 
LIMIT 5;
```

Also check if there are alternative column names:

```sql
-- See all columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'gpci_indices'
ORDER BY ordinal_position;
```

If all GPCI values are NULL, we need to:
1. Fix the RVU ingestor bug (column name mismatch)
2. Re-run ingestion to reload with correct column mapping

