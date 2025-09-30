"""Add missing metadata fields to CMS tables

Revision ID: 003_add_metadata_fields
Revises: 002_add_nber_centroids
Create Date: 2025-09-29 20:08:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003_add_metadata_fields'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Add missing metadata fields to CMSZipLocality table
    op.add_column('cms_zip_locality', sa.Column('effective_to', sa.DATE(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('carrier_mac', sa.VARCHAR(10), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('rural_flag', sa.BOOLEAN(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('data_quality_score', sa.FLOAT(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('validation_results', postgresql.JSONB(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('processing_timestamp', sa.TIMESTAMP(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('file_checksum', sa.VARCHAR(64), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('record_count', sa.INTEGER(), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('schema_version', sa.VARCHAR(20), nullable=True))
    op.add_column('cms_zip_locality', sa.Column('business_rules_applied', postgresql.JSONB(), nullable=True))
    
    # Add missing metadata fields to ZIP9Overrides table
    op.add_column('zip9_overrides', sa.Column('effective_from', sa.DATE(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('effective_to', sa.DATE(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('data_quality_score', sa.FLOAT(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('validation_results', postgresql.JSONB(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('processing_timestamp', sa.TIMESTAMP(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('file_checksum', sa.VARCHAR(64), nullable=True))
    op.add_column('zip9_overrides', sa.Column('record_count', sa.INTEGER(), nullable=True))
    op.add_column('zip9_overrides', sa.Column('schema_version', sa.VARCHAR(20), nullable=True))
    op.add_column('zip9_overrides', sa.Column('business_rules_applied', postgresql.JSONB(), nullable=True))

def downgrade():
    # Remove added metadata fields from ZIP9Overrides table
    op.drop_column('zip9_overrides', 'business_rules_applied')
    op.drop_column('zip9_overrides', 'schema_version')
    op.drop_column('zip9_overrides', 'record_count')
    op.drop_column('zip9_overrides', 'file_checksum')
    op.drop_column('zip9_overrides', 'processing_timestamp')
    op.drop_column('zip9_overrides', 'validation_results')
    op.drop_column('zip9_overrides', 'data_quality_score')
    op.drop_column('zip9_overrides', 'effective_to')
    op.drop_column('zip9_overrides', 'effective_from')
    
    # Remove added metadata fields from CMSZipLocality table
    op.drop_column('cms_zip_locality', 'business_rules_applied')
    op.drop_column('cms_zip_locality', 'schema_version')
    op.drop_column('cms_zip_locality', 'record_count')
    op.drop_column('cms_zip_locality', 'file_checksum')
    op.drop_column('cms_zip_locality', 'processing_timestamp')
    op.drop_column('cms_zip_locality', 'validation_results')
    op.drop_column('cms_zip_locality', 'data_quality_score')
    op.drop_column('cms_zip_locality', 'rural_flag')
    op.drop_column('cms_zip_locality', 'carrier_mac')
    op.drop_column('cms_zip_locality', 'effective_to')
