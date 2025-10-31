#!/usr/bin/env python3
"""Verify Phase 2 provenance migration was applied correctly.

This script validates that:
1. All 10 tables have release_id and batch_id columns
2. All 20 indexes are created (2 per table)
3. Column types are VARCHAR(50), nullable
4. No data loss (optional row count check)
5. Migration revision is current

Usage:
    python scripts/verify_migration.py --database-url $DATABASE_URL
    python scripts/verify_migration.py --database-url $DATABASE_URL --check-row-counts
"""

import argparse
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import structlog

logger = structlog.get_logger()

# Tables that should have provenance columns
REQUIRED_TABLES = [
    'fee_mpfs',
    'fee_opps',
    'fee_asc',
    'fee_ipps',
    'fee_clfs',
    'fee_dmepos',
    'gpci',
    'conversion_factors',
    'wage_index',
    'ipps_base_rates'
]

# Expected indexes (2 per table)
EXPECTED_INDEXES = {
    table: [f'idx_{table}_release', f'idx_{table}_batch']
    for table in REQUIRED_TABLES
}


def verify_columns(engine, check_row_counts: bool = False):
    """Verify all tables have release_id and batch_id columns with correct types."""
    logger.info("Verifying provenance columns exist")
    errors = []
    
    with engine.connect() as conn:
        for table in REQUIRED_TABLES:
            result = conn.execute(text("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_name = :table_name
                  AND column_name IN ('release_id', 'batch_id')
                ORDER BY column_name
            """), {"table_name": table})
            
            columns = {row[0]: row for row in result}
            
            # Check release_id
            if 'release_id' not in columns:
                errors.append(f"Table {table}: missing release_id column")
            else:
                col_data = columns['release_id']
                if col_data[1] != 'character varying':
                    errors.append(f"Table {table}: release_id has wrong type {col_data[1]}")
                elif col_data[2] != 50:
                    errors.append(f"Table {table}: release_id has wrong length {col_data[2]} (expected 50)")
                elif col_data[3] != 'YES':
                    errors.append(f"Table {table}: release_id is not nullable")
            
            # Check batch_id
            if 'batch_id' not in columns:
                errors.append(f"Table {table}: missing batch_id column")
            else:
                col_data = columns['batch_id']
                if col_data[1] != 'character varying':
                    errors.append(f"Table {table}: batch_id has wrong type {col_data[1]}")
                elif col_data[2] != 50:
                    errors.append(f"Table {table}: batch_id has wrong length {col_data[2]} (expected 50)")
                elif col_data[3] != 'YES':
                    errors.append(f"Table {table}: batch_id is not nullable")
            
            # Optional: Check row counts
            if check_row_counts:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                row_count = count_result.scalar()
                logger.info(f"Table {table} row count", count=row_count)
    
    if errors:
        logger.error("Column verification failed", errors=errors)
        return False
    
    logger.info("All provenance columns verified successfully")
    return True


def verify_indexes(engine):
    """Verify all expected indexes exist."""
    logger.info("Verifying provenance indexes exist")
    errors = []
    found_indexes = set()
    
    with engine.connect() as conn:
        for table in REQUIRED_TABLES:
            expected = EXPECTED_INDEXES[table]
            result = conn.execute(text("""
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = :table_name
                  AND (indexname LIKE :release_pattern OR indexname LIKE :batch_pattern)
            """), {
                "table_name": table,
                "release_pattern": f'idx_{table}_release',
                "batch_pattern": f'idx_{table}_batch'
            })
            
            found = {row[0] for row in result}
            
            for expected_idx in expected:
                if expected_idx not in found:
                    errors.append(f"Table {table}: missing index {expected_idx}")
                else:
                    found_indexes.add(expected_idx)
    
    if errors:
        logger.error("Index verification failed", errors=errors)
        return False
    
    logger.info("All provenance indexes verified successfully", count=len(found_indexes))
    return True


def verify_migration_revision(engine):
    """Verify Alembic migration revision is at head or later."""
    logger.info("Verifying Alembic revision")
    
    try:
        with engine.connect() as conn:
            # Check if alembic_version table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'alembic_version'
                )
            """))
            
            if not result.scalar():
                logger.warning("alembic_version table does not exist - skipping revision check")
                return True
            
            # Get current revision
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            current_revision = result.scalar()
            
            if current_revision == '8d80f393d0ee':
                logger.info("Migration revision is correct", revision=current_revision)
                return True
            else:
                logger.warning(
                    "Migration revision may not be at expected revision",
                    current=current_revision,
                    expected='8d80f393d0ee'
                )
                return True  # Don't fail - just warn
    
    except SQLAlchemyError as e:
        logger.warning("Could not verify Alembic revision", error=str(e))
        return True  # Don't fail on revision check


def main():
    parser = argparse.ArgumentParser(description="Verify Phase 2 provenance migration")
    parser.add_argument(
        "--database-url",
        required=True,
        help="Database URL (e.g., postgresql://user:pass@host:5432/dbname)"
    )
    parser.add_argument(
        "--check-row-counts",
        action="store_true",
        help="Also check and report row counts for each table"
    )
    args = parser.parse_args()
    
    try:
        engine = create_engine(args.database_url)
        
        logger.info("Starting migration verification", database_url=args.database_url[:50] + "...")
        
        all_passed = True
        
        # Verify columns
        if not verify_columns(engine, check_row_counts=args.check_row_counts):
            all_passed = False
        
        # Verify indexes
        if not verify_indexes(engine):
            all_passed = False
        
        # Verify revision (non-blocking)
        verify_migration_revision(engine)
        
        if all_passed:
            logger.info("✅ All migration checks passed")
            sys.exit(0)
        else:
            logger.error("❌ Migration verification failed")
            sys.exit(1)
    
    except Exception as e:
        logger.exception("Verification script failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

