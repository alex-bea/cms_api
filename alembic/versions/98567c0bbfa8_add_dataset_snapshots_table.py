"""add_dataset_snapshots_table

Revision ID: 98567c0bbfa8
Revises: 8d80f393d0ee
Create Date: 2025-10-31 13:40:27.356869

Creates dataset_snapshots table to serve as a registry of available dataset versions.
This enables deterministic snapshot selection and completes the provenance tracking story
from Phase 2. Each snapshot represents a specific release of a dataset with its digest,
effective dates, and manifest URL.

Part of Quick Win #1 from post-Phase 2 workstream.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '98567c0bbfa8'
down_revision = '8d80f393d0ee'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Set safety timeouts per PRD requirements
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    
    # Create dataset_snapshots table
    op.create_table(
        'dataset_snapshots',
        sa.Column('dataset_id', sa.String(length=50), nullable=False, comment='Dataset identifier (e.g., MPFS, OPPS, ASC)'),
        sa.Column('release_id', sa.String(length=50), nullable=False, comment='Release identifier matching fee schedule tables'),
        sa.Column('digest', sa.String(length=64), nullable=False, comment='SHA256 digest of dataset content'),
        sa.Column('effective_from', sa.Date(), nullable=False, comment='Date when snapshot becomes effective'),
        sa.Column('effective_to', sa.Date(), nullable=True, comment='Date when snapshot expires (None for current)'),
        sa.Column('manifest_url', sa.String(length=500), nullable=True, comment='URL to dataset manifest/metadata'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()'), comment='Timestamp when snapshot was registered'),
        sa.PrimaryKeyConstraint('dataset_id', 'release_id', name='pk_dataset_snapshots'),
        comment='Registry of dataset snapshots for deterministic provenance selection'
    )
    
    # Create indexes for efficient queries
    # Use unique names to avoid conflict with existing snapshots table indexes
    op.create_index(
        'idx_dataset_snapshots_dataset_effective',
        'dataset_snapshots',
        ['dataset_id', 'effective_from', 'effective_to'],
        unique=False
    )
    op.create_index(
        'idx_dataset_snapshots_digest',
        'dataset_snapshots',
        ['digest'],
        unique=False
    )


def downgrade() -> None:
    # Remove indexes first
    op.drop_index('idx_dataset_snapshots_digest', table_name='dataset_snapshots')
    op.drop_index('idx_dataset_snapshots_dataset_effective', table_name='dataset_snapshots')
    
    # Drop table
    op.drop_table('dataset_snapshots')
