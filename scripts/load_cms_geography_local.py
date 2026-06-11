#!/usr/bin/env python3
"""Load public CMS ZIP-locality rows into a local/dev geography table.

This command parses the CMS ZIP Code to Carrier Locality package directly into
the runtime ``geography`` table used by pricing resolution. It is intentionally
conservative:

- dry-run mode needs no database and only reports source facts;
- database writes refuse non-local URLs unless --allow-remote is passed;
- existing overlapping geography rows are not overwritten unless
  --replace-existing is passed;
- 2025Q4 source rows are not silently treated as valid for later valuation
  dates unless --open-ended-latest is explicit.

Example:
    python scripts/load_cms_geography_local.py --dry-run
    python scripts/load_cms_geography_local.py \
      --database-url \
      postgresql://cms_user:cms_password@localhost:5432/cms_pricing
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import sys
from typing import Any, Sequence

from sqlalchemy import and_, create_engine, or_
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bootstrap_local_db import (  # noqa: E402
    assert_local_database_url,
    resolve_database_url,
)
from cms_pricing.ingestion.parsers.cms_geography import (  # noqa: E402
    CMS_SOURCE_URL,
    DEFAULT_DATASET_ID,
    DEFAULT_PROBE_EXPECTED_CARRIER,
    DEFAULT_PROBE_EXPECTED_LOCALITY,
    DEFAULT_PROBE_EXPECTED_STATE,
    DEFAULT_PROBE_ZIP,
    DEFAULT_VALUATION_DATE,
    GeographyLoadError,
    SourceStats,
    build_source_report,
    iter_source_rows,
    scan_source_zip,
    source_zip_digest,
)
from cms_pricing.ingestion.validators.cms_geography_readiness import (  # noqa: E402
    CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    validate_source_readiness,
)
from cms_pricing.models.dataset_snapshots import DatasetSnapshot  # noqa: E402
from cms_pricing.models.geography import Geography  # noqa: E402


DEFAULT_SOURCE_ZIP = (
    PROJECT_ROOT
    / "data"
    / "cms_raw"
    / "zip-code-carrier-locality-file-revised-08-14-2025.zip"
)


@dataclass(frozen=True)
class LoadConfig:
    source_zip: Path
    dataset_id: str
    database_url: str | None
    dry_run: bool
    replace_existing: bool
    allow_remote: bool
    batch_size: int
    release_id: str | None
    manifest_url: str
    report_json: Path | None
    probe_zip: str
    expected_probe_state: str
    expected_probe_locality: str
    expected_probe_carrier: str
    valuation_date: date | None
    require_valuation_date_coverage: bool
    open_ended_latest: bool
    production_readiness_gates: bool


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-zip", type=Path, default=DEFAULT_SOURCE_ZIP)
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL "
            "when not using --dry-run."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help=(
            "Delete overlapping local/dev geography rows for this dataset "
            "before loading."
        ),
    )
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--release-id", default=None)
    parser.add_argument("--manifest-url", default=CMS_SOURCE_URL)
    parser.add_argument("--report-json", type=Path, default=None)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--probe-zip", default=DEFAULT_PROBE_ZIP)
    parser.add_argument("--expected-probe-state", default=DEFAULT_PROBE_EXPECTED_STATE)
    parser.add_argument(
        "--expected-probe-locality", default=DEFAULT_PROBE_EXPECTED_LOCALITY
    )
    parser.add_argument(
        "--expected-probe-carrier", default=DEFAULT_PROBE_EXPECTED_CARRIER
    )
    parser.add_argument("--valuation-date", default=DEFAULT_VALUATION_DATE.isoformat())
    parser.add_argument(
        "--require-valuation-date-coverage",
        action="store_true",
        help=(
            "Exit non-zero when source effective dates do not cover "
            "--valuation-date."
        ),
    )
    parser.add_argument(
        "--open-ended-latest",
        action="store_true",
        help=(
            "Local/dev override: store source rows with effective_to=NULL "
            "so the latest CMS ZIP-locality package remains active after "
            "its source quarter."
        ),
    )
    parser.add_argument(
        "--production-readiness-gates",
        action="store_true",
        help=(
            "Add the CMS ZIP-locality production-readiness gate report and "
            "exit non-zero if any gate fails."
        ),
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> LoadConfig:
    source_zip = args.source_zip.resolve()
    if not source_zip.exists():
        raise SystemExit(f"CMS geography source ZIP not found: {source_zip}")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")

    database_url = None
    if not args.dry_run:
        database_url = resolve_database_url(args.database_url)
        assert_local_database_url(database_url, allow_remote=args.allow_remote)
    elif args.database_url:
        database_url = resolve_database_url(args.database_url)
        assert_local_database_url(database_url, allow_remote=args.allow_remote)

    if args.valuation_date:
        valuation_date = date.fromisoformat(args.valuation_date)
    else:
        valuation_date = None
    return LoadConfig(
        source_zip=source_zip,
        dataset_id=args.dataset_id,
        database_url=database_url,
        dry_run=args.dry_run,
        replace_existing=args.replace_existing,
        allow_remote=args.allow_remote,
        batch_size=args.batch_size,
        release_id=args.release_id,
        manifest_url=args.manifest_url,
        report_json=args.report_json,
        probe_zip=args.probe_zip,
        expected_probe_state=args.expected_probe_state,
        expected_probe_locality=args.expected_probe_locality,
        expected_probe_carrier=args.expected_probe_carrier,
        valuation_date=valuation_date,
        require_valuation_date_coverage=args.require_valuation_date_coverage,
        open_ended_latest=args.open_ended_latest,
        production_readiness_gates=args.production_readiness_gates,
    )


def build_report(
    stats: SourceStats,
    *,
    config: LoadConfig,
    load_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = build_source_report(
        stats,
        source_url=config.manifest_url,
        release_id=config.release_id,
        open_ended_latest=config.open_ended_latest,
        expected_probe_state=config.expected_probe_state,
        expected_probe_locality=config.expected_probe_locality,
        expected_probe_carrier=config.expected_probe_carrier,
        valuation_date=config.valuation_date,
        require_valuation_date_coverage=config.require_valuation_date_coverage,
        load_result=load_result,
    )
    if config.production_readiness_gates:
        readiness = validate_source_readiness(
            stats,
            thresholds=CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
            valuation_date=config.valuation_date,
            require_valuation_date_coverage=(config.require_valuation_date_coverage),
        )
        report["production_readiness_gates"] = readiness
        if readiness["status"] != "ok":
            report["status"] = "blocked"
            report["stop_condition"] = "production_readiness_gates_failed"
    return report


def overlapping_existing_filter(stats: SourceStats):
    if stats.effective_from is None:
        raise GeographyLoadError(
            "Cannot build existing-row filter without source effective_from"
        )
    if stats.effective_to is None:
        return and_(
            Geography.effective_from <= date.max,
            or_(
                Geography.effective_to.is_(None),
                Geography.effective_to >= stats.effective_from,
            ),
        )
    return and_(
        Geography.effective_from <= stats.effective_to,
        or_(
            Geography.effective_to.is_(None),
            Geography.effective_to >= stats.effective_from,
        ),
    )


def delete_existing_geography(
    session: Session, *, dataset_id: str, stats: SourceStats
) -> int:
    result = (
        session.query(Geography)
        .filter(
            Geography.dataset_id == dataset_id,
            overlapping_existing_filter(stats),
        )
        .delete(synchronize_session=False)
    )
    return int(result or 0)


def inspect_existing_geography(
    session: Session,
    *,
    config: LoadConfig,
    stats: SourceStats,
) -> dict[str, Any]:
    same_digest_count = (
        session.query(Geography)
        .filter(
            Geography.dataset_id == config.dataset_id,
            Geography.dataset_digest == stats.dataset_digest,
        )
        .count()
    )
    overlapping_count = (
        session.query(Geography)
        .filter(
            Geography.dataset_id == config.dataset_id,
            overlapping_existing_filter(stats),
        )
        .count()
    )
    different_digest_overlap_count = (
        session.query(Geography)
        .filter(
            Geography.dataset_id == config.dataset_id,
            Geography.dataset_digest != stats.dataset_digest,
            overlapping_existing_filter(stats),
        )
        .count()
    )

    if same_digest_count == stats.valid_rows:
        return {
            "action": "reuse_existing",
            "same_digest_count": same_digest_count,
            "overlapping_count": overlapping_count,
            "different_digest_overlap_count": different_digest_overlap_count,
        }
    if (
        same_digest_count or different_digest_overlap_count
    ) and not config.replace_existing:
        raise GeographyLoadError(
            "Existing geography rows overlap this load. Pass "
            "--replace-existing for a scoped local/dev reload. "
            f"same_digest_count={same_digest_count}, "
            f"different_digest_overlap_count={different_digest_overlap_count}"
        )
    return {
        "action": "insert",
        "same_digest_count": same_digest_count,
        "overlapping_count": overlapping_count,
        "different_digest_overlap_count": different_digest_overlap_count,
    }


def ensure_snapshot(
    session: Session, *, config: LoadConfig, stats: SourceStats
) -> dict[str, Any]:
    release_id = config.release_id or stats.release_id
    existing = session.get(DatasetSnapshot, (config.dataset_id, release_id))
    if existing is not None:
        if existing.digest == stats.dataset_digest:
            return {
                "dataset_id": config.dataset_id,
                "release_id": release_id,
                "action": "reuse_existing",
            }
        if not config.replace_existing:
            raise GeographyLoadError(
                f"Dataset snapshot {config.dataset_id}:{release_id} "
                "already exists with a different digest"
            )
        existing.digest = stats.dataset_digest
        existing.effective_from = stats.effective_from
        existing.effective_to = stats.effective_to
        existing.manifest_url = config.manifest_url
        return {
            "dataset_id": config.dataset_id,
            "release_id": release_id,
            "action": "updated",
        }

    session.add(
        DatasetSnapshot(
            dataset_id=config.dataset_id,
            release_id=release_id,
            digest=stats.dataset_digest,
            effective_from=stats.effective_from,
            effective_to=stats.effective_to,
            manifest_url=config.manifest_url,
        )
    )
    return {
        "dataset_id": config.dataset_id,
        "release_id": release_id,
        "action": "inserted",
    }


def insert_geography_rows(
    session: Session, *, config: LoadConfig, stats: SourceStats
) -> int:
    created_at = date.today()
    inserted = 0
    batch: list[dict[str, Any]] = []
    for row in iter_source_rows(
        config.source_zip, open_ended_latest=config.open_ended_latest
    ):
        batch.append(
            row.to_mapping(
                dataset_id=config.dataset_id,
                dataset_digest=stats.dataset_digest,
                created_at=created_at,
            )
        )
        if len(batch) >= config.batch_size:
            session.bulk_insert_mappings(Geography, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        session.bulk_insert_mappings(Geography, batch)
        inserted += len(batch)
    return inserted


def load_into_database(config: LoadConfig, stats: SourceStats) -> dict[str, Any]:
    if config.database_url is None:
        raise GeographyLoadError("Database URL is required unless --dry-run is used")
    engine = create_engine(config.database_url)
    with Session(engine) as session:
        existing = inspect_existing_geography(session, config=config, stats=stats)
        deleted_rows = 0
        inserted_rows = 0
        if existing["action"] == "insert":
            if config.replace_existing and existing["overlapping_count"]:
                deleted_rows = delete_existing_geography(
                    session,
                    dataset_id=config.dataset_id,
                    stats=stats,
                )
            inserted_rows = insert_geography_rows(session, config=config, stats=stats)
        snapshot = ensure_snapshot(session, config=config, stats=stats)
        session.commit()

    return {
        "dry_run": False,
        "action": existing["action"],
        "deleted_rows": deleted_rows,
        "inserted_rows": inserted_rows,
        "existing": existing,
        "snapshot": snapshot,
    }


def write_report(report: dict[str, Any], report_json: Path | None) -> None:
    output = json.dumps(report, indent=2, sort_keys=True)
    print(output, flush=True)
    if report_json is not None:
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(output + "\n")


def run(config: LoadConfig) -> dict[str, Any]:
    digest = source_zip_digest(config.source_zip)
    stats = scan_source_zip(
        config.source_zip,
        dataset_id=config.dataset_id,
        dataset_digest=digest,
        probe_zip=config.probe_zip,
        open_ended_latest=config.open_ended_latest,
    )
    load_result = None if config.dry_run else load_into_database(config, stats)
    return build_report(stats, config=config, load_result=load_result)


def main() -> None:
    config = build_config(parse_args())
    try:
        report = run(config)
    except GeographyLoadError as exc:
        raise SystemExit(str(exc)) from exc
    write_report(report, config.report_json)
    if report.get("status") == "blocked":
        raise SystemExit(str(report["stop_condition"]))


if __name__ == "__main__":
    main()
