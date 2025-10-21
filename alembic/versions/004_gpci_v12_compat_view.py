"""GPCI v1.2 backwards compatibility view (optional)

Revision ID: 004_gpci_v12_compat_view
Revises: 003_gpci_v13_add_mac_to_nk
Create Date: 2025-10-21 01:00:00.000000

Optional: Creates a backwards compatibility view for v1.2 consumers.

Background:
- v1.2 allowed queries by (locality_code, effective_start) without MAC
- v1.3 requires MAC in all queries
- This view provides limited v1.2 compatibility via DISTINCT ON

Limitations:
- Ambiguous localities (e.g., locality_code='00' in multiple states) 
  return arbitrary MAC (first by sort order)
- NOT recommended for production use
- Provided only for transition period

Recommendation: Update consumers to use v1.3 NK (include MAC in queries)

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004_gpci_v12_compat_view'
down_revision = '003_gpci_v13_add_mac_to_nk'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Create v1.2 backwards compatibility view.
    
    Selects first MAC per (locality_id, effective_start) for ambiguous localities.
    
    WARNING: This is NOT accurate for ambiguous localities (e.g., locality '00').
    Consumers should migrate to v1.3 queries that include MAC.
    """
    op.execute("""
        CREATE OR REPLACE VIEW gpci_indices_v12_compat AS
        SELECT DISTINCT ON (locality_id, effective_start)
            id,
            release_id,
            mac,
            state,
            locality_id,
            locality_name,
            work_gpci,
            pe_gpci,
            mp_gpci,
            effective_start,
            effective_end,
            source_file,
            row_num
        FROM gpci_indices
        ORDER BY locality_id, effective_start, mac;
    """)
    
    # Add comment explaining limitations
    op.execute("""
        COMMENT ON VIEW gpci_indices_v12_compat IS 
        'GPCI v1.2 backwards compatibility view (DEPRECATED).
         
         Purpose: Allows legacy queries that do not include MAC.
         
         Limitations:
         - Ambiguous localities (e.g., locality_code=00 in AL, AZ, AR, etc.) 
           return ARBITRARY MAC (first alphabetically).
         - NOT suitable for production use - results may be incorrect.
         - Provided only for transition period (1-2 quarters).
         
         Recommendation: 
         Update consumers to v1.3 queries:
           WHERE mac = ? AND locality_id = ? AND effective_start = ?
         
         Example v1.2 query (WORKS but may be inaccurate):
           SELECT * FROM gpci_indices_v12_compat 
           WHERE locality_id = ''00'' AND effective_start = ''2025-01-01'';
         
         Example v1.3 query (CORRECT):
           SELECT * FROM gpci_indices 
           WHERE mac = ''01112'' AND locality_id = ''00'' AND effective_start = ''2025-01-01'';
         
         Migration date: 2025-10-21
         Sunset date: 2026-04-01 (expected)
         See: prds/SRC-gpci.md §6 for schema evolution details';
    """)
    
    # Log creation
    op.execute("""
        DO $$ 
        BEGIN
            RAISE NOTICE '✅ Created GPCI v1.2 compatibility view';
            RAISE NOTICE '   View: gpci_indices_v12_compat';
            RAISE NOTICE '   WARNING: Returns arbitrary MAC for ambiguous localities';
            RAISE NOTICE '   Recommend: Migrate consumers to v1.3 (include MAC in queries)';
        END $$;
    """)


def downgrade() -> None:
    """Drop v1.2 compatibility view."""
    op.execute("DROP VIEW IF EXISTS gpci_indices_v12_compat;")
    
    op.execute("""
        DO $$ 
        BEGIN
            RAISE NOTICE '✅ Dropped GPCI v1.2 compatibility view';
        END $$;
    """)

