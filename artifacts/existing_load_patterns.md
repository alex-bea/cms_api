# Existing Database Load Patterns

## Summary
Found existing implementations in the codebase that show how to load DataFrames into Postgres.

## 1. Row-by-Row Insert Pattern (Most Common)
**Files:** 
- `cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py:315-342`
- `cms_pricing/ingestion/ingestors/cms_zip9_ingester.py:568-589`

**Pattern:**
```python
for _, row in df.iterrows():
    model_instance = ModelClass(
        field1=row['field1'],
        field2=row['field2'],
        # ...
    )
    db.add(model_instance)
    records_inserted += 1

db.commit()
```

**Pros:**
- Simple and straightforward
- Easy to debug (see exact row on error)
- Works for small datasets

**Cons:**
- SLOW for large datasets (10,000+ rows)
- Not efficient for bulk operations

## 2. Bulk Insert Pattern (Load Tester)
**File:** `cms_pricing/performance/load_tester.py:192-209`

**Pattern:**
```python
batch_size = 1000
for i in range(0, len(items), batch_size):
    batch = items[i:i + batch_size]
    for item in batch:
        db.add(item)
db.commit()
```

**Pros:**
- Better performance for large datasets
- Reduces memory usage

**Cons:**
- Still row-by-row, just batched
- Not the fastest possible

## 3. Raw SQL Insert (Metadata)
**File:** `cms_pricing/ingestion/metadata/ingestion_runs_manager.py:313-321`

**Pattern:**
```python
db_session.execute(text("""
    INSERT INTO table_name (
        col1, col2, col3
    ) VALUES (
        :col1, :col2, :col3
    )
"""), {"col1": val1, "col2": val2, "col3": val3})
```

**Pros:**
- Fastest for bulk inserts
- Direct SQL execution
- Best for performance

**Cons:**
- Requires manual column mapping
- More complex error handling

## Recommendation for RVU Pipeline

**Use Pattern #1 (row-by-row) with these modifications:**

1. **Add batch commits** (commit every 1000 rows)
2. **Add progress logging** (log every 10,000 rows)
3. **Handle errors gracefully** (log and continue)
4. **Add transaction rollback on failure**

**Why:**
- RVU datasets are moderate size (10,000-50,000 rows per dataset)
- Error handling is important (want to see which HCPCS code failed)
- Simpler to implement and maintain
- Can optimize later if needed

## Implementation Notes

**Column Mapping:**
- Need to map DataFrame columns to SQLAlchemy model fields
- Handle type conversions (dates, decimals, arrays)
- Add metadata fields (release_id, row_num, source_file)

**Error Handling:**
- Log failed rows with context
- Continue inserting remaining rows
- Return summary of successes/failures

**Performance:**
- Batch commits (every 1000 rows)
- Use db.flush() every 10000 rows to avoid memory issues
- Consider parallel inserts for multiple datasets

## PRD Guidance

**STD-data-architecture-impl-v1.0.md §7.4** shows ingestor examples but doesn't specify load patterns.

**Best practice from codebase:**
1. Use existing model classes (RVUItem, GPCIIndex, etc.)
2. Follow the row-by-row pattern with batching
3. Add comprehensive logging
4. Handle errors gracefully
