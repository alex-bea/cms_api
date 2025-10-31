"""add_provenance_to_fee_tables

Revision ID: 8d80f393d0ee
Revises: 6d0f0408be80
Create Date: 2025-10-31 11:45:05.679288

Adds release_id and batch_id columns to all simplified fee schedule tables
to enable deterministic tracking of CMS data versions used in pricing calculations.
This is part of Phase 2 provenance implementation for ClearBill integration.

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d80f393d0ee'
down_revision = '6d0f0408be80'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Set safety timeouts per PRD requirements (STD-database-platform-prd-v1.0.md)
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    
    # Tables to modify (in dependency order)
    # These are the simplified fee schedule tables queried by pricing engines
    tables_to_modify = [
        'fee_mpfs',      # MPFS engine primary table
        'fee_opps',      # OPPS engine primary table
        'fee_asc',       # ASC engine primary table
        'fee_ipps',      # IPPS engine primary table
        'fee_clfs',      # CLFS engine primary table
        'fee_dmepos',    # DMEPOS engine primary table
        'gpci',          # MPFS supporting data (locality adjustments)
        'conversion_factors',  # MPFS/ASC supporting data
        'wage_index',    # OPPS/IPPS supporting data
        'ipps_base_rates'  # IPPS supporting data
    ]
    
    for table_name in tables_to_modify:
        # Add nullable columns (will be populated by future ingestion)
        # Using nullable=True for backward compatibility with existing data
        op.add_column(
            table_name,
            sa.Column('release_id', sa.String(length=50), nullable=True, comment='Release identifier from CMS data source')
        )
        op.add_column(
            table_name,
            sa.Column('batch_id', sa.String(length=50), nullable=True, comment='Batch identifier from ingestion run')
        )
        
        # Create indexes for efficient provenance queries
        # Note: Not using CONCURRENTLY for initial deploy (acceptable for new nullable columns)
        op.create_index(
            f'idx_{table_name}_release',
            table_name,
            ['release_id'],
            unique=False
        )
        op.create_index(
            f'idx_{table_name}_batch',
            table_name,
            ['batch_id'],
            unique=False
        )


def downgrade() -> None:
    # Remove in reverse order
    tables_to_modify = [
        'ipps_base_rates',
        'wage_index',
        'conversion_factors',
        'gpci',
        'fee_dmepos',
        'fee_clfs',
        'fee_ipps',
        'fee_asc',
        'fee_opps',
        'fee_mpfs'
    ]
    
    for table_name in tables_to_modify:
        # Drop indexes first
        op.drop_index(f'idx_{table_name}_batch', table_name=table_name)
        op.drop_index(f'idx_{table_name}_release', table_name=table_name)
        # Then drop columns
        op.drop_column(table_name, 'batch_id')
        op.drop_column(table_name, 'release_id')
