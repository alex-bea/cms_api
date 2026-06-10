#!/usr/bin/env python3
"""Bootstrap a local/dev database from SQLAlchemy models.

This is intentionally a development helper. It creates the current model schema,
optionally stamps Alembic head, and can seed the tiny reference set needed for
local smoke pricing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session


LOGGER = logging.getLogger("bootstrap_local_db")
LOCAL_DB_HOSTS = {
    None,
    "",
    "localhost",
    "127.0.0.1",
    "::1",
    "db",
    "postgres",
    "host.docker.internal",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap a local CMS API database")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL.",
    )
    parser.add_argument(
        "--seed-smoke",
        action="store_true",
        help="Seed minimal MPFS/GPCI/CF rows for local smoke pricing.",
    )
    parser.add_argument(
        "--stamp-head",
        action="store_true",
        help="Stamp Alembic head after creating model tables.",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow bootstrapping a non-local database URL.",
    )
    parser.add_argument(
        "--alembic-ini",
        default=str(_project_root() / "alembic.ini"),
        help="Path to alembic.ini.",
    )
    return parser.parse_args()


def resolve_database_url(database_url: str | None = None) -> str:
    for value in (database_url, os.getenv("TEST_DATABASE_URL"), os.getenv("DATABASE_URL")):
        if value:
            return value
    raise SystemExit("Database URL not provided. Use --database-url or set TEST_DATABASE_URL/DATABASE_URL.")


def assert_local_database_url(database_url: str, allow_remote: bool = False) -> None:
    if allow_remote:
        return

    url = make_url(database_url)
    if url.drivername.startswith("sqlite"):
        return

    if url.host not in LOCAL_DB_HOSTS:
        safe_url = url.render_as_string(hide_password=True)
        raise SystemExit(
            f"Refusing to bootstrap non-local database {safe_url}. "
            "Pass --allow-remote only when this is intentional."
        )


def configure_database_url(database_url: str) -> None:
    os.environ["DATABASE_URL"] = database_url
    os.environ.setdefault("TEST_DATABASE_URL", database_url)

    project_root = str(_project_root())
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def create_model_schema(database_url: str) -> None:
    configure_database_url(database_url)

    from cms_pricing.database import Base, engine
    import cms_pricing.models  # noqa: F401 - register all declarative models

    LOGGER.info("Creating model schema with checkfirst=True")
    Base.metadata.create_all(bind=engine, checkfirst=True)


def stamp_alembic_head(database_url: str, alembic_ini: str) -> None:
    configure_database_url(database_url)

    from alembic import command
    from alembic.config import Config as AlembicConfig

    config_path = Path(alembic_ini)
    if not config_path.exists():
        raise SystemExit(f"Alembic config not found at {config_path}")

    LOGGER.info("Stamping Alembic head")
    config = AlembicConfig(str(config_path))
    config.set_main_option("sqlalchemy.url", database_url)
    command.stamp(config, "head")


def seed_smoke_data(database_url: str) -> None:
    """Seed minimal MPFS reference data for local smoke tests."""

    configure_database_url(database_url)

    from cms_pricing.models.fee_schedules import ConversionFactor, FeeMPFS, GPCI

    engine = create_engine(database_url)
    release_seed = "seed_release_2025"
    batch_seed = "seed_batch_001"

    with Session(engine) as session:
        try:
            existing_mpfs = (
                session.query(FeeMPFS)
                .filter(
                    FeeMPFS.year == 2025,
                    FeeMPFS.hcpcs == "99213",
                    FeeMPFS.revision == "A",
                )
                .first()
            )
        except ProgrammingError:
            session.rollback()
            raise SystemExit("fee_mpfs table is missing. Run schema bootstrap before seeding.")

        if not existing_mpfs:
            session.add(
                FeeMPFS(
                    id=uuid4(),
                    year=2025,
                    revision="A",
                    hcpcs="99213",
                    work_rvu=1.5,
                    pe_nf_rvu=1.2,
                    pe_fac_rvu=0.9,
                    mp_rvu=0.2,
                    global_days=0,
                    status_indicator="A",
                    effective_from=date(2025, 1, 1),
                    release_id=release_seed,
                    batch_id=batch_seed,
                )
            )

        existing_gpci = (
            session.query(GPCI)
            .filter(GPCI.year == 2025, GPCI.locality_id == "01")
            .first()
        )
        if not existing_gpci:
            session.add(
                GPCI(
                    id=uuid4(),
                    year=2025,
                    locality_id="01",
                    locality_name="Seed Locality",
                    gpci_work=1.05,
                    gpci_pe=1.02,
                    gpci_mp=0.99,
                    effective_from=date(2025, 1, 1),
                    release_id=release_seed,
                    batch_id=batch_seed,
                )
            )

        existing_cf = (
            session.query(ConversionFactor)
            .filter(ConversionFactor.year == 2025, ConversionFactor.source == "MPFS")
            .first()
        )
        if not existing_cf:
            session.add(
                ConversionFactor(
                    id=uuid4(),
                    year=2025,
                    cf=33.89,
                    source="MPFS",
                    effective_from=date(2025, 1, 1),
                    release_id=release_seed,
                    batch_id=batch_seed,
                )
            )

        session.commit()
        LOGGER.info("Smoke seed data is present")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()
    database_url = resolve_database_url(args.database_url)
    assert_local_database_url(database_url, allow_remote=args.allow_remote)

    create_model_schema(database_url)

    if args.stamp_head:
        stamp_alembic_head(database_url, args.alembic_ini)

    if args.seed_smoke:
        seed_smoke_data(database_url)

    LOGGER.info("Local database bootstrap complete")


if __name__ == "__main__":
    main()
