"""Add plans and plan_components tables

Revision ID: 6d0f0408be80
Revises: 002_add_nber_centroids
Create Date: 2025-10-03 20:45:57.488263

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '6d0f0408be80'
down_revision = '002_add_nber_centroids'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create plans table
    op.create_table('plans',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('metadata', sa.Text(), nullable=True),
        sa.Column('created_by', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_plans_name', 'plans', ['name'], unique=False)
    op.create_index('idx_plans_created_at', 'plans', ['created_at'], unique=False)
    
    # Create plan_components table
    op.create_table('plan_components',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('plan_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('code', sa.String(length=5), nullable=False),
        sa.Column('setting', sa.String(length=20), nullable=False),
        sa.Column('units', sa.Float(), nullable=False),
        sa.Column('utilization_weight', sa.Float(), nullable=False),
        sa.Column('professional_component', sa.Boolean(), nullable=False),
        sa.Column('facility_component', sa.Boolean(), nullable=False),
        sa.Column('modifiers', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('pos', sa.String(length=2), nullable=True),
        sa.Column('ndc11', sa.String(length=11), nullable=True),
        sa.Column('wastage_units', sa.Float(), nullable=False),
        sa.Column('sequence', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(['plan_id'], ['plans.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_plan_components_plan_code', 'plan_components', ['plan_id', 'code'], unique=False)
    op.create_index('idx_plan_components_setting', 'plan_components', ['setting'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_plan_components_setting', table_name='plan_components')
    op.drop_index('idx_plan_components_plan_code', table_name='plan_components')
    op.drop_table('plan_components')
    op.drop_index('idx_plans_created_at', table_name='plans')
    op.drop_index('idx_plans_name', table_name='plans')
    op.drop_table('plans')
