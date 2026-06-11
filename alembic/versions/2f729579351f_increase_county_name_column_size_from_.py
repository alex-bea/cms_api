"""Increase county_name column size from 100 to 128 characters

Revision ID: 2f729579351f
Revises: c056bb717c6e
Create Date: 2025-11-04 23:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2f729579351f"
down_revision = "c056bb717c6e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("locality_counties"):
        op.execute(
            """
            DO $$
            BEGIN
                RAISE NOTICE 'Table locality_counties does not exist yet (fresh database) - skipping county_name column resize';
            END $$;
        """
        )
        return

    # Increase county_name column size to match loader expectation (128 chars)
    # Loader uses max_len=128, so database should allow same size
    op.alter_column(
        "locality_counties",
        "county_name",
        existing_type=sa.VARCHAR(length=100),
        type_=sa.String(length=128),
        existing_nullable=True,
    )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("locality_counties"):
        return

    # Revert to original 100 character limit (WARNING: will truncate existing data)
    op.alter_column(
        "locality_counties",
        "county_name",
        existing_type=sa.String(length=128),
        type_=sa.VARCHAR(length=100),
        existing_nullable=True,
    )
