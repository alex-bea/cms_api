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

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

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

    Currently this is a placeholder hook. Extend it as soon as tests
    require specific fee schedule or geography rows.
    """

    LOGGER.info("Seeding baseline reference data (noop placeholder)")
    engine = create_engine(database_url)
    with Session(engine) as session:
        # Intentionally empty – add inserts/fixtures as coverage expands.
        session.commit()


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
