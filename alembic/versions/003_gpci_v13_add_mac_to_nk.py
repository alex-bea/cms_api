"""GPCI v1.3: Add MAC to natural key unique index

Revision ID: 003_gpci_v13_add_mac_to_nk
Revises: 002_add_nber_centroids
Create Date: 2025-10-21 00:45:00.000000

Breaking Change: GPCI schema v1.2 → v1.3
- Natural key changed from (locality_code, effective_from) 
  to (mac, locality_code, effective_from)
- Rationale: locality_code='00' appears in multiple states (AL, AZ, AR, etc.)
- MAC column already exists in table (added earlier)
- This migration adds unique constraint to enforce new NK

Impact:
- Prevents false duplicates (63 of 112 rows affected in v1.2)
- Enables proper joins with locality/ANES data
- Requires backfill (re-parse GPCI data for correct hashes)

See: planning/parsers/gpci/v1.3_MIGRATION_NOTES.md
See: prds/SRC-gpci.md for full schema evolution

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '003_gpci_v13_add_mac_to_nk'
down_revision = '002_add_nber_centroids'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add unique index on GPCI v1.3 natural key: (mac, locality_id, effective_start).
    
    Note: Keeps surrogate UUID primary key for backwards compatibility.
    Database columns use locality_id and effective_start (not locality_code, effective_from).
    """
    # Drop old unique constraint if it exists (v1.2 NK)
    # Check first to avoid error if constraint/table doesn't exist
    op.execute("""
        DO $$ 
        BEGIN
            -- Check if table exists before querying constraints
            IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'gpci_indices') THEN
                -- Drop old v1.2 constraint if present
                IF EXISTS (
                    SELECT 1 
                    FROM pg_constraint 
                    WHERE conname = 'uq_gpci_locality_effective'
                      AND conrelid = 'gpci_indices'::regclass
                ) THEN
                    ALTER TABLE gpci_indices 
                    DROP CONSTRAINT uq_gpci_locality_effective;
                    
                    RAISE NOTICE 'Dropped old v1.2 unique constraint: uq_gpci_locality_effective';
                END IF;
                
                -- Also check for index-based constraint
                IF EXISTS (
                    SELECT 1 
                    FROM pg_indexes 
                    WHERE tablename = 'gpci_indices' 
                      AND indexname = 'uq_gpci_locality_effective'
                ) THEN
                    DROP INDEX IF EXISTS uq_gpci_locality_effective;
                    
                    RAISE NOTICE 'Dropped old v1.2 unique index: uq_gpci_locality_effective';
                END IF;
            ELSE
                RAISE NOTICE 'Table gpci_indices does not exist yet (fresh database) - skipping constraint cleanup';
            END IF;
        END $$;
    """)
    
    # Add new v1.3 unique index on 3-field natural key
    # Keep surrogate UUID primary key (id) for compatibility with existing foreign keys
    # Note: Table must exist before creating index (created by app or previous migrations)
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'gpci_indices') THEN
                -- Create unique index if table exists
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'gpci_indices' 
                      AND indexname = 'uq_gpci_mac_locality_effective'
                ) THEN
                    CREATE UNIQUE INDEX uq_gpci_mac_locality_effective
                    ON gpci_indices (mac, locality_id, effective_start);
                    RAISE NOTICE 'Created v1.3 unique index: uq_gpci_mac_locality_effective';
                ELSE
                    RAISE NOTICE 'Index uq_gpci_mac_locality_effective already exists';
                END IF;
            ELSE
                RAISE NOTICE 'Table gpci_indices does not exist - index will be created when table is created';
            END IF;
        END $$;
    """)
    
    # Add comment documenting the constraint (if index exists)
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = 'gpci_indices' 
                  AND indexname = 'uq_gpci_mac_locality_effective'
            ) THEN
                COMMENT ON INDEX uq_gpci_mac_locality_effective IS 
                'GPCI v1.3 natural key unique constraint.
                 Enforces uniqueness on (mac, locality_id, effective_start).
                 Prevents false duplicates where locality_code=00 appears in multiple states.
                 Migration date: 2025-10-21
                 See: prds/SRC-gpci.md for schema evolution details';
                RAISE NOTICE 'Added comment to index';
            END IF;
        END $$;
    """)
    
    # Log migration completion
    op.execute("""
        DO $$ 
        BEGIN
            RAISE NOTICE '✅ GPCI v1.3 migration complete';
            RAISE NOTICE '   Added unique index: uq_gpci_mac_locality_effective';
            RAISE NOTICE '   Natural key: (mac, locality_id, effective_start)';
            RAISE NOTICE '   Next step: Run backfill script to re-parse GPCI data';
        END $$;
    """)


def downgrade() -> None:
    """
    Revert to v1.2 (NOT RECOMMENDED - reintroduces false duplicate bug).
    
    This removes the v1.3 unique constraint but does NOT:
    - Fix existing false duplicates in data
    - Update row hashes
    - Restore v1.2 constraint (intentionally omitted)
    """
    # Remove v1.3 unique index
    op.drop_index('uq_gpci_mac_locality_effective', table_name='gpci_indices')
    
    # Log downgrade warning
    op.execute("""
        DO $$ 
        BEGIN
            RAISE WARNING '⚠️ GPCI v1.3 downgrade executed';
            RAISE WARNING '   Removed unique index: uq_gpci_mac_locality_effective';
            RAISE WARNING '   Data may now contain false duplicates (locality_code=00 across states)';
            RAISE WARNING '   v1.2 constraint NOT restored (intentionally)';
            RAISE WARNING '   Recommend: Re-apply v1.3 migration or investigate data quality';
        END $$;
    """)
    
    # Intentionally NOT recreating v1.2 constraint
    # v1.2 constraint would be:
    #   op.create_index('uq_gpci_locality_effective', 'gpci_indices',
    #                   ['locality_id', 'effective_start'], unique=True)
    # This is WRONG and causes false duplicates, so we don't restore it

