"""Increase fee_schedule_area column size from 10 to 128 characters

Revision ID: c056bb717c6e
Revises: 98567c0bbfa8
Create Date: 2025-11-04 15:07:10.158885

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c056bb717c6e'
down_revision = '98567c0bbfa8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Increase fee_schedule_area column size to accommodate full locality descriptions
    # Source file field is 70 characters wide, so 128 provides adequate headroom
    op.alter_column(
        'locality_counties',
        'fee_schedule_area',
        existing_type=sa.VARCHAR(length=10),
        type_=sa.String(length=128),
        existing_nullable=True
    )


def downgrade() -> None:
    # Revert to original 10 character limit (WARNING: will truncate existing data)
    op.alter_column(
        'locality_counties',
        'fee_schedule_area',
        existing_type=sa.String(length=128),
        type_=sa.VARCHAR(length=10),
        existing_nullable=True
    )
