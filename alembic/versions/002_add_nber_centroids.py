"""Add NBER centroids table

Revision ID: 002_add_nber_centroids
Revises: 001_add_nearest_zip_tables
Create Date: 2025-09-29 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002_add_nber_centroids'
down_revision = '001_add_nearest_zip_tables'
branch_labels = None
depends_on = None


def upgrade():
    # Create NBER centroids table
    op.create_table('nber_centroids',
        sa.Column('zcta5', sa.CHAR(5), nullable=False),
        sa.Column('lat', sa.Float(), nullable=False),
        sa.Column('lon', sa.Float(), nullable=False),
        sa.Column('vintage', sa.String(10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.PrimaryKeyConstraint('zcta5')
    )
    
    # Create indexes
    op.create_index('idx_nber_centroids_vintage', 'nber_centroids', ['vintage'])


def downgrade():
    # Drop indexes
    op.drop_index('idx_nber_centroids_vintage', table_name='nber_centroids')
    
    # Drop table
    op.drop_table('nber_centroids')
