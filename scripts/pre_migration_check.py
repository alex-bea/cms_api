#!/usr/bin/env python3
"""Pre-migration checks for Phase 2 provenance migration.

This script performs pre-migration validation:
1. Verify current Alembic revision
2. Document table row counts
3. Check for existing indexes that might conflict
4. Verify database connection

Usage:
    python scripts/pre_migration_check.py --database-url $DATABASE_URL
    python scripts/pre_migration_check.py --database-url $DATABASE_URL --output report.json
"""

import argparse
import json
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import structlog

logger = structlog.get_logger()

# Tables that will be modified
TARGET_TABLES = [
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

EXPECTED_REVISION = '6d0f0408be80'  # Current revision before Phase 2 migration
TARGET_REVISION = '8d80f393d0ee'   # Phase 2 migration revision


def check_alembic_revision(engine):
    """Check current Alembic revision matches expected."""
    logger.info("Checking Alembic revision")
    
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
                logger.warning("alembic_version table does not exist")
                return None, "No alembic_version table"
            
            # Get current revision
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            current_revision = result.scalar()
            
            if current_revision == EXPECTED_REVISION:
                logger.info("Current revision matches expected", revision=current_revision)
                return current_revision, "OK"
            elif current_revision == TARGET_REVISION:
                logger.warning("Migration already applied", revision=current_revision)
                return current_revision, "Already migrated"
            else:
                logger.warning(
                    "Current revision does not match expected",
                    current=current_revision,
                    expected=EXPECTED_REVISION
                )
                return current_revision, f"Mismatch: expected {EXPECTED_REVISION}, got {current_revision}"
    
    except SQLAlchemyError as e:
        logger.error("Failed to check Alembic revision", error=str(e))
        return None, f"Error: {str(e)}"


def get_table_row_counts(engine):
    """Get row counts for all target tables."""
    logger.info("Getting table row counts")
    counts = {}
    
    with engine.connect() as conn:
        for table in TARGET_TABLES:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                counts[table] = count
                logger.info(f"Table {table} row count", count=count)
            except SQLAlchemyError as e:
                logger.warning(f"Could not count rows for {table}", error=str(e))
                counts[table] = None
    
    return counts


def check_existing_indexes(engine):
    """Check for existing indexes that might conflict."""
    logger.info("Checking for existing indexes")
    existing_indexes = {}
    
    with engine.connect() as conn:
        for table in TARGET_TABLES:
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
            
            indexes = [row[0] for row in result]
            if indexes:
                existing_indexes[table] = indexes
                logger.warning(f"Found existing indexes for {table}", indexes=indexes)
            else:
                existing_indexes[table] = []
    
    return existing_indexes


def test_connection(engine):
    """Test database connection."""
    logger.info("Testing database connection")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info("Database connection successful", version=version[:50])
            return True, version
    except SQLAlchemyError as e:
        logger.error("Database connection failed", error=str(e))
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description="Pre-migration checks for Phase 2")
    parser.add_argument(
        "--database-url",
        required=True,
        help="Database URL"
    )
    parser.add_argument(
        "--output",
        help="Output file for JSON report (optional)"
    )
    args = parser.parse_args()
    
    report = {
        "database_url": args.database_url[:50] + "..." if len(args.database_url) > 50 else args.database_url,
        "checks": {}
    }
    
    try:
        engine = create_engine(args.database_url)
        
        # Test connection
        connected, connection_info = test_connection(engine)
        report["checks"]["connection"] = {
            "status": "OK" if connected else "FAILED",
            "info": connection_info
        }
        
        if not connected:
            logger.error("Cannot proceed - database connection failed")
            sys.exit(1)
        
        # Check Alembic revision
        current_rev, rev_status = check_alembic_revision(engine)
        report["checks"]["alembic_revision"] = {
            "current": current_rev,
            "status": rev_status,
            "expected": EXPECTED_REVISION,
            "target": TARGET_REVISION
        }
        
        # Get row counts
        row_counts = get_table_row_counts(engine)
        report["checks"]["row_counts"] = row_counts
        total_rows = sum(count for count in row_counts.values() if count is not None)
        report["checks"]["total_rows"] = total_rows
        
        # Check for existing indexes
        existing_indexes = check_existing_indexes(engine)
        report["checks"]["existing_indexes"] = existing_indexes
        has_conflicts = any(indexes for indexes in existing_indexes.values())
        report["checks"]["index_conflicts"] = has_conflicts
        
        # Summary
        all_ok = (
            connected and
            current_rev == EXPECTED_REVISION and
            not has_conflicts
        )
        
        report["summary"] = {
            "ready": all_ok,
            "total_rows": total_rows,
            "can_proceed": all_ok
        }
        
        # Output report
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info("Report saved", file=args.output)
        
        # Print summary
        print("\n" + "="*60)
        print("PRE-MIGRATION CHECK SUMMARY")
        print("="*60)
        print(f"Database Connection: {'✅ OK' if connected else '❌ FAILED'}")
        print(f"Alembic Revision: {current_rev or 'N/A'} ({rev_status})")
        print(f"Total Rows: {total_rows:,}")
        print(f"Index Conflicts: {'⚠️  Yes' if has_conflicts else '✅ None'}")
        print(f"\nReady to proceed: {'✅ YES' if all_ok else '❌ NO'}")
        print("="*60 + "\n")
        
        if all_ok:
            logger.info("✅ Pre-migration checks passed")
            sys.exit(0)
        else:
            logger.error("❌ Pre-migration checks failed - review report")
            sys.exit(1)
    
    except Exception as e:
        logger.exception("Pre-migration check failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

