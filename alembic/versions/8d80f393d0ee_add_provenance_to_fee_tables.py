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
revision = "8d80f393d0ee"
down_revision = "6d0f0408be80"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Set safety timeouts per PRD requirements (STD-database-platform-prd-v1.0.md)
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")

    # Tables to modify (in dependency order)
    # These are the simplified fee schedule tables queried by pricing engines
    tables_to_modify = [
        "fee_mpfs",  # MPFS engine primary table
        "fee_opps",  # OPPS engine primary table
        "fee_asc",  # ASC engine primary table
        "fee_ipps",  # IPPS engine primary table
        "fee_clfs",  # CLFS engine primary table
        "fee_dmepos",  # DMEPOS engine primary table
        "gpci",  # MPFS supporting data (locality adjustments)
        "conversion_factors",  # MPFS/ASC supporting data
        "wage_index",  # OPPS/IPPS supporting data
        "ipps_base_rates",  # IPPS supporting data
    ]

    inspector = sa.inspect(op.get_bind())
    for table_name in tables_to_modify:
        if not inspector.has_table(table_name):
            op.execute(
                f"""
                DO $$
                BEGIN
                    RAISE NOTICE 'Table {table_name} does not exist yet (fresh database) - skipping provenance columns';
                END $$;
                """
            )
            continue

        column_names = {column["name"] for column in inspector.get_columns(table_name)}
        index_names = {index["name"] for index in inspector.get_indexes(table_name)}

        # Add nullable columns (will be populated by future ingestion)
        # Using nullable=True for backward compatibility with existing data
        if "release_id" not in column_names:
            op.add_column(
                table_name,
                sa.Column(
                    "release_id",
                    sa.String(length=50),
                    nullable=True,
                    comment="Release identifier from CMS data source",
                ),
            )
        if "batch_id" not in column_names:
            op.add_column(
                table_name,
                sa.Column(
                    "batch_id",
                    sa.String(length=50),
                    nullable=True,
                    comment="Batch identifier from ingestion run",
                ),
            )

        # Create indexes for efficient provenance queries
        # Note: Not using CONCURRENTLY for initial deploy (acceptable for new nullable columns)
        if f"idx_{table_name}_release" not in index_names:
            op.create_index(
                f"idx_{table_name}_release",
                table_name,
                ["release_id"],
                unique=False,
            )
        if f"idx_{table_name}_batch" not in index_names:
            op.create_index(
                f"idx_{table_name}_batch",
                table_name,
                ["batch_id"],
                unique=False,
            )


def downgrade() -> None:
    # Remove in reverse order
    tables_to_modify = [
        "ipps_base_rates",
        "wage_index",
        "conversion_factors",
        "gpci",
        "fee_dmepos",
        "fee_clfs",
        "fee_ipps",
        "fee_asc",
        "fee_opps",
        "fee_mpfs",
    ]

    inspector = sa.inspect(op.get_bind())
    for table_name in tables_to_modify:
        if not inspector.has_table(table_name):
            continue

        column_names = {column["name"] for column in inspector.get_columns(table_name)}
        index_names = {index["name"] for index in inspector.get_indexes(table_name)}

        # Drop indexes first
        if f"idx_{table_name}_batch" in index_names:
            op.drop_index(f"idx_{table_name}_batch", table_name=table_name)
        if f"idx_{table_name}_release" in index_names:
            op.drop_index(f"idx_{table_name}_release", table_name=table_name)
        # Then drop columns
        if "batch_id" in column_names:
            op.drop_column(table_name, "batch_id")
        if "release_id" in column_names:
            op.drop_column(table_name, "release_id")
