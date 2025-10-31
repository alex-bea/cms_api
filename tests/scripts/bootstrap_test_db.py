"""Bootstrap a Postgres test database for API suites.

This script is intentionally lightweight: it runs Alembic migrations
against the configured database URL and loads any required seed data so
pytest can execute against a real PostgreSQL dialect (JSONB/ARRAY, etc.).

Usage:
    python tests/scripts/bootstrap_test_db.py --database-url postgresql://...

If ``--database-url`` is omitted the script falls back to the
``TEST_DATABASE_URL`` environment variable, then ``DATABASE_URL``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import date
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import ProgrammingError

try:
    from alembic import command
    from alembic.config import Config as AlembicConfig
except ImportError as exc:  # pragma: no cover - defensive guard
    raise SystemExit(
        "Alembic is required to bootstrap the test database. Install dev dependencies."
    ) from exc

LOGGER = logging.getLogger("bootstrap_test_db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap Postgres test database")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        default=None,
        help="Database URL to use (overrides TEST_DATABASE_URL/DATABASE_URL)",
    )
    parser.add_argument(
        "--alembic-ini",
        dest="alembic_ini",
        default=str(Path("alembic.ini")),
        help="Path to Alembic configuration file (default: alembic.ini)",
    )
    parser.add_argument(
        "--revision",
        dest="revision",
        default="head",
        help="Alembic revision to upgrade to (default: head)",
    )
    return parser.parse_args()


def resolve_database_url(cmdline_url: str | None) -> str:
    candidates = [
        cmdline_url,
        os.getenv("TEST_DATABASE_URL"),
        os.getenv("DATABASE_URL"),
    ]
    for value in candidates:
        if value:
            return value
    raise SystemExit("Database URL not provided. Use --database-url or set TEST_DATABASE_URL.")


def run_migrations(database_url: str, alembic_ini: str, revision: str) -> None:
    LOGGER.info(f"Running Alembic migrations to revision: {revision}")
    ini_path = Path(alembic_ini)
    if not ini_path.exists():
        raise SystemExit(f"Alembic config not found at {ini_path}")

    config = AlembicConfig(str(ini_path))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, revision)


def seed_reference_data(database_url: str) -> None:
    """Insert minimal reference data required by API tests.

    Seeds deterministic provenance-aware rows so pricing endpoints
    return realistic data out of the box.
    """

    LOGGER.info("Seeding baseline reference data (if empty)")
    engine = create_engine(database_url)
    with Session(engine) as session:
        try:
            mpfs_count = session.execute("SELECT COUNT(*) FROM fee_mpfs").scalar()
        except ProgrammingError:
            # Tables may not exist yet (e.g., revision skipped) – rely on migrations
            session.rollback()
            LOGGER.warning("fee_mpfs table not available; skipping seed")
            return

        if mpfs_count and mpfs_count > 0:
            LOGGER.info("Reference data already present; skipping seed")
            return

        LOGGER.info("Inserting seed rows for fee schedule tables")

        # Shared provenance identifiers
        release_seed = "seed_release_2025"
        batch_seed = "seed_batch_001"

        # fee_mpfs
        session.execute(
            """
            INSERT INTO fee_mpfs (
                id, year, revision, hcpcs, work_rvu, pe_nf_rvu, pe_fac_rvu, mp_rvu,
                global_days, status_indicator, effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :revision, :hcpcs, :work_rvu, :pe_nf_rvu, :pe_fac_rvu, :mp_rvu,
                    :global_days, :status_indicator, :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "revision": "A",
                "hcpcs": "99213",
                "work_rvu": 1.5,
                "pe_nf_rvu": 1.2,
                "pe_fac_rvu": 0.9,
                "mp_rvu": 0.2,
                "global_days": 0,
                "status_indicator": "A",
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # gpci
        session.execute(
            """
            INSERT INTO gpci (
                id, year, locality_id, locality_name,
                gpci_work, gpci_pe, gpci_mp,
                effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :locality_id, :locality_name,
                    :gpci_work, :gpci_pe, :gpci_mp,
                    :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "locality_id": "01",
                "locality_name": "Seed Locality",
                "gpci_work": 1.05,
                "gpci_pe": 1.02,
                "gpci_mp": 0.99,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # conversion_factors
        session.execute(
            """
            INSERT INTO conversion_factors (
                id, year, cf, source, effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :cf, :source, :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "cf": 33.89,
                "source": "MPFS",
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # fee_opps
        session.execute(
            """
            INSERT INTO fee_opps (
                id, year, quarter, hcpcs, status_indicator, apc, national_unadj_rate,
                effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :quarter, :hcpcs, :status_indicator, :apc, :rate,
                    :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "quarter": "1",
                "hcpcs": "0633T",
                "status_indicator": "J1",
                "apc": "5153",
                "rate": 520.75,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # fee_asc
        session.execute(
            """
            INSERT INTO fee_asc (
                id, year, quarter, hcpcs, asc_rate, effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :quarter, :hcpcs, :asc_rate, :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "quarter": "1",
                "hcpcs": "0191T",
                "asc_rate": 1120.55,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # fee_ipps
        session.execute(
            """
            INSERT INTO fee_ipps (
                id, fy, drg, weight, effective_from, release_id, batch_id
            )
            VALUES (:id, :fy, :drg, :weight, :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "fy": 2025,
                "drg": "469",
                "weight": 3.567,
                "effective_from": date(2024, 10, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # ipps_base_rates
        session.execute(
            """
            INSERT INTO ipps_base_rates (
                id, fy, operating_base, capital_base,
                effective_from, release_id, batch_id
            )
            VALUES (:id, :fy, :operating_base, :capital_base,
                    :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "fy": 2025,
                "operating_base": 6000.12,
                "capital_base": 450.33,
                "effective_from": date(2024, 10, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # fee_clfs
        session.execute(
            """
            INSERT INTO fee_clfs (
                id, year, quarter, hcpcs, fee, effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :quarter, :hcpcs, :fee, :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "quarter": "1",
                "hcpcs": "80053",
                "fee": 15.75,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # fee_dmepos
        session.execute(
            """
            INSERT INTO fee_dmepos (
                id, year, quarter, code, rural_flag, fee,
                effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :quarter, :code, :rural_flag, :fee,
                    :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "quarter": "1",
                "code": "E0110",
                "rural_flag": False,
                "fee": 42.87,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        # wage_index
        session.execute(
            """
            INSERT INTO wage_index (
                id, year, quarter, cbsa, wage_index,
                effective_from, release_id, batch_id
            )
            VALUES (:id, :year, :quarter, :cbsa, :wage_index,
                    :effective_from, :release_id, :batch_id)
            """,
            {
                "id": uuid4(),
                "year": 2025,
                "quarter": "1",
                "cbsa": "41860",
                "wage_index": 1.12,
                "effective_from": date(2025, 1, 1),
                "release_id": release_seed,
                "batch_id": batch_seed,
            },
        )

        session.commit()
        LOGGER.info("Seed data inserted successfully")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()
    database_url = resolve_database_url(args.database_url)

    LOGGER.info(f"Bootstrapping Postgres test database: {database_url}")
    run_migrations(database_url, args.alembic_ini, args.revision)
    seed_reference_data(database_url)
    LOGGER.info("Bootstrap complete")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - top level guard
        LOGGER.exception("Bootstrap failed: %s", exc)
        sys.exit(1)
