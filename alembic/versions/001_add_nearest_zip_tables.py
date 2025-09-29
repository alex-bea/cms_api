"""Add nearest ZIP resolver tables

Revision ID: 001
Revises: 
Create Date: 2025-09-29 12:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create nearest ZIP resolver tables
    op.create_table('zcta_coords',
        sa.Column('zcta5', sa.CHAR(length=5), nullable=False),
        sa.Column('lat', sa.Float(), nullable=False),
        sa.Column('lon', sa.Float(), nullable=False),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('zcta5')
    )
    op.create_index('idx_zcta_coords_vintage', 'zcta_coords', ['vintage'], unique=False)
    op.create_index('idx_zcta_coords_coords', 'zcta_coords', ['lat', 'lon'], unique=False)

    op.create_table('zip_to_zcta',
        sa.Column('zip5', sa.CHAR(length=5), nullable=False),
        sa.Column('zcta5', sa.CHAR(length=5), nullable=False),
        sa.Column('relationship', sa.Text(), nullable=True),
        sa.Column('weight', sa.Numeric(), nullable=True),
        sa.Column('city', sa.Text(), nullable=True),
        sa.Column('state', sa.Text(), nullable=True),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('zip5')
    )
    op.create_index('idx_zip_to_zcta_zcta5', 'zip_to_zcta', ['zcta5'], unique=False)
    op.create_index('idx_zip_to_zcta_vintage', 'zip_to_zcta', ['vintage'], unique=False)
    op.create_index('idx_zip_to_zcta_relationship', 'zip_to_zcta', ['relationship'], unique=False)

    op.create_table('cms_zip_locality',
        sa.Column('zip5', sa.CHAR(length=5), nullable=False),
        sa.Column('state', sa.CHAR(length=2), nullable=False),
        sa.Column('locality', sa.String(length=10), nullable=False),
        sa.Column('carrier_mac', sa.String(length=10), nullable=True),
        sa.Column('rural_flag', sa.Boolean(), nullable=True),
        sa.Column('effective_from', sa.Date(), nullable=False),
        sa.Column('effective_to', sa.Date(), nullable=True),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('zip5')
    )
    op.create_index('idx_cms_zip_locality_state', 'cms_zip_locality', ['state'], unique=False)
    op.create_index('idx_cms_zip_locality_locality', 'cms_zip_locality', ['locality'], unique=False)
    op.create_index('idx_cms_zip_locality_effective', 'cms_zip_locality', ['effective_from', 'effective_to'], unique=False)
    op.create_index('idx_cms_zip_locality_vintage', 'cms_zip_locality', ['vintage'], unique=False)

    op.create_table('zip9_overrides',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('zip9_low', sa.CHAR(length=9), nullable=False),
        sa.Column('zip9_high', sa.CHAR(length=9), nullable=False),
        sa.Column('state', sa.CHAR(length=2), nullable=False),
        sa.Column('locality', sa.String(length=10), nullable=False),
        sa.Column('rural_flag', sa.Boolean(), nullable=True),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_zip9_overrides_state', 'zip9_overrides', ['state'], unique=False)
    op.create_index('idx_zip9_overrides_locality', 'zip9_overrides', ['locality'], unique=False)
    op.create_index('idx_zip9_overrides_vintage', 'zip9_overrides', ['vintage'], unique=False)
    op.create_index('idx_zip9_overrides_range', 'zip9_overrides', ['zip9_low', 'zip9_high'], unique=True)
    op.create_index('ix_zip9_overrides_zip9_low', 'zip9_overrides', ['zip9_low'], unique=False)
    op.create_index('ix_zip9_overrides_zip9_high', 'zip9_overrides', ['zip9_high'], unique=False)

    op.create_table('zcta_distances',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('zcta5_a', sa.CHAR(length=5), nullable=False),
        sa.Column('zcta5_b', sa.CHAR(length=5), nullable=False),
        sa.Column('miles', sa.Float(), nullable=False),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_zcta_distances_vintage', 'zcta_distances', ['vintage'], unique=False)
    op.create_index('idx_zcta_distances_miles', 'zcta_distances', ['miles'], unique=False)
    op.create_index('idx_zcta_distances_pair', 'zcta_distances', ['zcta5_a', 'zcta5_b'], unique=True)
    op.create_index('ix_zcta_distances_zcta5_a', 'zcta_distances', ['zcta5_a'], unique=False)
    op.create_index('ix_zcta_distances_zcta5_b', 'zcta_distances', ['zcta5_b'], unique=False)

    op.create_table('zip_metadata',
        sa.Column('zip5', sa.CHAR(length=5), nullable=False),
        sa.Column('zcta_bool', sa.Boolean(), nullable=True),
        sa.Column('parent_zcta', sa.CHAR(length=5), nullable=True),
        sa.Column('military_bool', sa.Boolean(), nullable=True),
        sa.Column('population', sa.Integer(), nullable=True),
        sa.Column('is_pobox', sa.Boolean(), nullable=False),
        sa.Column('vintage', sa.String(length=10), nullable=False),
        sa.Column('source_filename', sa.Text(), nullable=True),
        sa.Column('ingest_run_id', sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint('zip5')
    )
    op.create_index('idx_zip_metadata_is_pobox', 'zip_metadata', ['is_pobox'], unique=False)
    op.create_index('idx_zip_metadata_vintage', 'zip_metadata', ['vintage'], unique=False)
    op.create_index('idx_zip_metadata_population', 'zip_metadata', ['population'], unique=False)

    op.create_table('ingest_runs',
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('source_url', sa.Text(), nullable=False),
        sa.Column('filename', sa.Text(), nullable=True),
        sa.Column('sha256', sa.CHAR(length=64), nullable=True),
        sa.Column('bytes', sa.BIGINT(), nullable=True),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('finished_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('row_count', sa.BIGINT(), nullable=True),
        sa.Column('tool_version', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('run_id')
    )
    op.create_index('idx_ingest_runs_status', 'ingest_runs', ['status'], unique=False)
    op.create_index('idx_ingest_runs_started_at', 'ingest_runs', ['started_at'], unique=False)
    op.create_index('idx_ingest_runs_source_url', 'ingest_runs', ['source_url'], unique=False)

    op.create_table('nearest_zip_traces',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('input_zip', sa.String(length=20), nullable=False),
        sa.Column('input_zip5', sa.CHAR(length=5), nullable=False),
        sa.Column('input_zip9', sa.CHAR(length=9), nullable=True),
        sa.Column('result_zip', sa.CHAR(length=5), nullable=False),
        sa.Column('distance_miles', sa.Float(), nullable=False),
        sa.Column('trace_json', sa.Text(), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_nearest_zip_traces_input', 'nearest_zip_traces', ['input_zip5'], unique=False)
    op.create_index('idx_nearest_zip_traces_result', 'nearest_zip_traces', ['result_zip'], unique=False)
    op.create_index('idx_nearest_zip_traces_created_at', 'nearest_zip_traces', ['created_at'], unique=False)


def downgrade() -> None:
    # Drop nearest ZIP resolver tables
    op.drop_table('nearest_zip_traces')
    op.drop_table('ingest_runs')
    op.drop_table('zip_metadata')
    op.drop_table('zcta_distances')
    op.drop_table('zip9_overrides')
    op.drop_table('cms_zip_locality')
    op.drop_table('zip_to_zcta')
    op.drop_table('zcta_coords')
