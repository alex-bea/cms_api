#!/usr/bin/env python3
"""Seed/register local post-RVU-load data needed by the API smoke.

The command is intentionally non-destructive:

- it refuses non-local database URLs unless --allow-remote is passed;
- it inserts the ZIP-locality row only when no active matching row exists;
- it registers missing dataset snapshot rows from already-loaded RVU/GPCI rows;
- it fails on conflicting active geography rows instead of overwriting them.

Example:
    python scripts/seed_post_rvu_load_local.py \
      --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bootstrap_local_db import (  # noqa: E402
    assert_local_database_url,
    resolve_database_url,
)
from cms_pricing.models.dataset_snapshots import DatasetSnapshot  # noqa: E402
from cms_pricing.models.geography import Geography  # noqa: E402
from cms_pricing.models.rvu import GPCIIndex, Release, RVUItem  # noqa: E402


@dataclass(frozen=True)
class SeedConfig:
    database_url: str
    zip5: str
    state: str
    locality_id: str
    locality_name: str
    carrier: str
    effective_from: date
    effective_to: date
    valuation_date: date
    rvu_release_id: str
    gpci_release_id: str
    manifest_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL.",
    )
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument("--zip", default="94110")
    parser.add_argument("--state", default="CA")
    parser.add_argument("--locality-id", default="05")
    parser.add_argument(
        "--locality-name",
        default="San Francisco-Oakland-Berkeley",
    )
    parser.add_argument("--carrier", default="01112")
    parser.add_argument("--effective-from", default="2026-01-01")
    parser.add_argument("--effective-to", default="2026-12-31")
    parser.add_argument("--valuation-date", default="2026-07-01")
    parser.add_argument("--rvu-release-id", default="rvu_2026_C")
    parser.add_argument("--gpci-release-id", default="gpci_2026_C")
    parser.add_argument(
        "--manifest-url",
        default="local://scripts/load_latest_cms_rvu_local.py",
    )
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> SeedConfig:
    database_url = resolve_database_url(args.database_url)
    assert_local_database_url(database_url, allow_remote=args.allow_remote)
    return SeedConfig(
        database_url=database_url,
        zip5=args.zip,
        state=args.state,
        locality_id=args.locality_id,
        locality_name=args.locality_name,
        carrier=args.carrier,
        effective_from=date.fromisoformat(args.effective_from),
        effective_to=date.fromisoformat(args.effective_to),
        valuation_date=date.fromisoformat(args.valuation_date),
        rvu_release_id=args.rvu_release_id,
        gpci_release_id=args.gpci_release_id,
        manifest_url=args.manifest_url,
    )


def geography_digest(config: SeedConfig) -> str:
    payload = "|".join(
        [
            "ZIP_LOCALITY",
            config.zip5,
            config.state,
            config.locality_id,
            config.carrier,
            config.effective_from.isoformat(),
            config.effective_to.isoformat(),
            "cms-public-local-seed",
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def snapshot_digest(
    *,
    dataset_id: str,
    release_id: str,
    release_uuid: Any,
    row_count: int,
) -> str:
    payload = f"{dataset_id}|{release_id}|{release_uuid}|{row_count}"
    return hashlib.sha256(payload.encode()).hexdigest()


def active_geography_rows(session: Session, config: SeedConfig) -> list[Geography]:
    return (
        session.query(Geography)
        .filter(
            Geography.zip5 == config.zip5,
            Geography.has_plus4 == 0,
            Geography.effective_from <= config.valuation_date,
            (Geography.effective_to.is_(None))
            | (Geography.effective_to >= config.valuation_date),
        )
        .order_by(Geography.effective_from.asc())
        .all()
    )


def ensure_geography_row(session: Session, config: SeedConfig) -> dict[str, Any]:
    active_rows = active_geography_rows(session, config)
    matching_rows = [
        row
        for row in active_rows
        if row.state == config.state
        and row.locality_id == config.locality_id
        and row.carrier == config.carrier
    ]
    if matching_rows:
        return {"inserted": False, "active_rows": len(active_rows)}

    if active_rows:
        conflicts = [
            {
                "zip5": row.zip5,
                "state": row.state,
                "locality_id": row.locality_id,
                "carrier": row.carrier,
                "effective_from": row.effective_from.isoformat(),
                "effective_to": row.effective_to.isoformat()
                if row.effective_to
                else None,
            }
            for row in active_rows
        ]
        raise SystemExit(
            "Refusing to overwrite conflicting active geography row: "
            f"{json.dumps(conflicts, sort_keys=True)}"
        )

    session.add(
        Geography(
            id=uuid4(),
            zip5=config.zip5,
            plus4=None,
            has_plus4=0,
            state=config.state,
            locality_id=config.locality_id,
            locality_name=config.locality_name,
            carrier=config.carrier,
            rural_flag=None,
            effective_from=config.effective_from,
            effective_to=config.effective_to,
            dataset_id="ZIP_LOCALITY",
            dataset_digest=geography_digest(config),
            created_at=date.today(),
        )
    )
    return {"inserted": True, "active_rows": 0}


def get_loaded_release(session: Session, config: SeedConfig) -> Release:
    release = (
        session.query(Release)
        .filter(
            Release.type == "RVU_FULL",
            Release.source_version == config.rvu_release_id,
        )
        .order_by(Release.imported_at.desc())
        .first()
    )
    if release is None:
        raise SystemExit(f"Missing loaded release {config.rvu_release_id}")
    return release


def ensure_snapshot(
    session: Session,
    *,
    dataset_id: str,
    release_id: str,
    digest: str,
    effective_from: date,
    manifest_url: str,
) -> dict[str, Any]:
    existing = session.get(DatasetSnapshot, (dataset_id, release_id))
    if existing is not None:
        if existing.effective_from > effective_from:
            raise SystemExit(
                f"Existing {dataset_id}:{release_id} snapshot starts after "
                f"{effective_from.isoformat()}: {existing.effective_from.isoformat()}"
            )
        return {
            "dataset_id": dataset_id,
            "release_id": release_id,
            "inserted": False,
            "effective_from": existing.effective_from.isoformat(),
        }

    session.add(
        DatasetSnapshot(
            dataset_id=dataset_id,
            release_id=release_id,
            digest=digest,
            effective_from=effective_from,
            effective_to=None,
            manifest_url=manifest_url,
        )
    )
    return {
        "dataset_id": dataset_id,
        "release_id": release_id,
        "inserted": True,
        "effective_from": effective_from.isoformat(),
    }


def ensure_snapshots(session: Session, config: SeedConfig) -> list[dict[str, Any]]:
    release = get_loaded_release(session, config)
    rvu_count = (
        session.query(RVUItem).filter(RVUItem.release_id == release.id).count()
    )
    gpci_count = (
        session.query(GPCIIndex).filter(GPCIIndex.release_id == release.id).count()
    )
    if rvu_count <= 0:
        raise SystemExit(f"No RVU rows found for release {release.id}")
    if gpci_count <= 0:
        raise SystemExit(f"No GPCI rows found for release {release.id}")

    return [
        ensure_snapshot(
            session,
            dataset_id="rvu_items",
            release_id=config.rvu_release_id,
            digest=snapshot_digest(
                dataset_id="rvu_items",
                release_id=config.rvu_release_id,
                release_uuid=release.id,
                row_count=rvu_count,
            ),
            effective_from=config.valuation_date,
            manifest_url=config.manifest_url,
        ),
        ensure_snapshot(
            session,
            dataset_id="gpci_indices",
            release_id=config.gpci_release_id,
            digest=snapshot_digest(
                dataset_id="gpci_indices",
                release_id=config.gpci_release_id,
                release_uuid=release.id,
                row_count=gpci_count,
            ),
            effective_from=config.valuation_date,
            manifest_url=config.manifest_url,
        ),
    ]


def main() -> None:
    config = build_config(parse_args())
    engine = create_engine(config.database_url)
    with Session(engine) as session:
        geography_result = ensure_geography_row(session, config)
        snapshot_results = ensure_snapshots(session, config)
        session.commit()

    print(
        json.dumps(
            {
                "status": "ok",
                "geography": {
                    "zip5": config.zip5,
                    "state": config.state,
                    "locality_id": config.locality_id,
                    "carrier": config.carrier,
                    **geography_result,
                },
                "snapshots": snapshot_results,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
