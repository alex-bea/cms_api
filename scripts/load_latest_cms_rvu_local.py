#!/usr/bin/env python3
"""Load the latest CMS RVU release into a local/dev database.

This is the repeatable local/dev command for turning a live CMS RVU drop into
curated parquet artifacts, RVU database rows, dataset snapshot registrations,
and a verification report.

Examples:
    python scripts/load_latest_cms_rvu_local.py
    python scripts/load_latest_cms_rvu_local.py --release rvu_2026_C
    python scripts/load_latest_cms_rvu_local.py --start-year 2026 --end-year 2026
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bootstrap_local_db import (  # noqa: E402
    assert_local_database_url,
    configure_database_url,
    resolve_database_url,
)
from cms_pricing.ingestion.contracts.ingestor_spec import SourceFile  # noqa: E402
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import (  # noqa: E402
    CMSRVUScraper,
    RVUFileInfo,
)


LOGGER = logging.getLogger("load_latest_cms_rvu_local")
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "ingestion" / "local" / "rvu"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "ingestion" / "local" / "reports"
QUARTER_ORDER = {"A": 1, "B": 2, "C": 3, "D": 4}
REQUIRED_RVU_DATASETS = ("pprrvu", "gpci", "oppscap", "anescf", "localitycounty")
DB_TABLE_BY_DATASET = {
    "pprrvu": "rvu_items",
    "gpci": "gpci_indices",
    "oppscap": "opps_caps",
    "anescf": "anes_cfs",
    "localitycounty": "locality_counties",
}
SNAPSHOT_DATASET_IDS = ("rvu_items", "gpci_indices", "oppscap", "anescf", "localitycounty")


@dataclass(frozen=True)
class LoadConfig:
    database_url: str
    output_dir: Path
    report_json: Path
    start_year: int
    end_year: int
    release: str
    batch_id: str
    replace_existing: bool
    allow_remote: bool


class RVULocalLoadError(RuntimeError):
    """Raised when local RVU loading or verification fails."""


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    current_year = date.today().year
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--start-year", type=int, default=current_year)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument(
        "--release",
        default="latest",
        help="Release to load, e.g. latest, rvu_2026_C, rvu26c, or 26c.",
    )
    parser.add_argument("--batch-id")
    parser.add_argument(
        "--no-replace-existing",
        action="store_true",
        help="Do not delete an existing local RVU release/snapshot before loading.",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow a non-local database URL. Not recommended for this local/dev helper.",
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> LoadConfig:
    database_url = resolve_database_url(args.database_url)
    assert_local_database_url(database_url, allow_remote=args.allow_remote)
    started_at = datetime.now(timezone.utc)
    report_json = args.report_json or (
        DEFAULT_REPORT_DIR / f"cms_rvu_local_load_{started_at:%Y%m%d_%H%M%S}.json"
    )
    batch_id = args.batch_id or f"local_cms_rvu_{int(time.time())}"
    return LoadConfig(
        database_url=database_url,
        output_dir=args.output_dir,
        report_json=report_json,
        start_year=args.start_year,
        end_year=args.end_year,
        release=args.release,
        batch_id=batch_id,
        replace_existing=not args.no_replace_existing,
        allow_remote=args.allow_remote,
    )


def _release_sort_key(file_info: RVUFileInfo) -> Tuple[int, int, str]:
    return (
        int(file_info.year),
        QUARTER_ORDER.get(str(file_info.quarter or "").upper(), 0),
        str(file_info.revision or ""),
    )


def _release_id_for(file_info: RVUFileInfo) -> str:
    suffix = f"{str(file_info.quarter).upper()}{file_info.revision or ''}"
    return f"rvu_{file_info.year}_{suffix}"


def _compact_release_id_for(file_info: RVUFileInfo) -> str:
    suffix = f"{str(file_info.quarter).lower()}{str(file_info.revision or '').lower()}"
    return f"rvu{int(file_info.year) % 100:02d}{suffix}"


def _release_aliases(file_info: RVUFileInfo) -> set[str]:
    standard = _release_id_for(file_info)
    compact = _compact_release_id_for(file_info)
    return {
        standard.lower(),
        standard.replace("_", "").lower(),
        compact.lower(),
        compact.removeprefix("rvu").lower(),
    }


def select_release_files(files: Sequence[RVUFileInfo], release: str) -> List[RVUFileInfo]:
    if not files:
        raise RVULocalLoadError("CMS RVU discovery returned no files")

    if release.lower() == "latest":
        selected_key = max(_release_sort_key(file_info) for file_info in files)
        return [file_info for file_info in files if _release_sort_key(file_info) == selected_key]

    requested = release.lower().replace("-", "_")
    requested_compact = requested.replace("_", "")
    matches = [
        file_info
        for file_info in files
        if requested in _release_aliases(file_info)
        or requested_compact in _release_aliases(file_info)
    ]
    if not matches:
        available = sorted({_release_id_for(file_info) for file_info in files})
        raise RVULocalLoadError(
            f"Requested release {release!r} not found. Available releases: {available}"
        )
    return matches


def source_file_from_rvu_info(file_info: RVUFileInfo) -> SourceFile:
    metadata = {
        "detail_url": file_info.detail_url,
        "display_name": file_info.display_name,
        "posted_at": file_info.posted_at,
        "version": file_info.version,
        "year": file_info.year,
        "quarter": file_info.quarter,
        "revision": file_info.revision,
        **dict(file_info.metadata or {}),
    }
    return SourceFile(
        url=file_info.url,
        filename=file_info.filename,
        content_type=file_info.content_type or "application/octet-stream",
        expected_size_bytes=file_info.size_bytes,
        last_modified=file_info.last_modified,
        checksum=file_info.checksum,
        file_type=file_info.file_type,
        metadata=metadata,
    )


def source_version_for_release(release_id: str) -> str:
    return str(release_id or "")[:10]


def expected_dataset_release_ids(release_id: str) -> Dict[str, str]:
    from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

    return {
        dataset_id: RVUIngestor._dataset_release_id(dataset_id, release_id)
        for dataset_id in SNAPSHOT_DATASET_IDS
    }


def delete_existing_release(session: Session, release_id: str) -> Dict[str, int]:
    """Delete local rows for the same source_version and snapshot releases."""
    source_version = source_version_for_release(release_id)
    release_ids = [
        row[0]
        for row in session.execute(
            text("SELECT id FROM releases WHERE type = 'RVU_FULL' AND source_version = :source_version"),
            {"source_version": source_version},
        ).all()
    ]

    deleted: Dict[str, int] = {}
    for table_name in DB_TABLE_BY_DATASET.values():
        if release_ids:
            delete_rows = text(
                f"DELETE FROM {table_name} WHERE release_id IN :release_ids"
            ).bindparams(bindparam("release_ids", expanding=True))
            result = session.execute(
                delete_rows,
                {"release_ids": release_ids},
            )
            deleted[table_name] = int(result.rowcount or 0)
        else:
            deleted[table_name] = 0

    if release_ids:
        delete_releases = text(
            "DELETE FROM releases WHERE id IN :release_ids"
        ).bindparams(bindparam("release_ids", expanding=True))
        result = session.execute(
            delete_releases,
            {"release_ids": release_ids},
        )
        deleted["releases"] = int(result.rowcount or 0)
    else:
        deleted["releases"] = 0

    snapshot_release_ids = list(expected_dataset_release_ids(release_id).values())
    delete_snapshots = text(
        "DELETE FROM dataset_snapshots WHERE release_id IN :snapshot_release_ids"
    ).bindparams(bindparam("snapshot_release_ids", expanding=True))
    result = session.execute(
        delete_snapshots,
        {"snapshot_release_ids": snapshot_release_ids},
    )
    deleted["dataset_snapshots"] = int(result.rowcount or 0)
    session.commit()
    return deleted


async def run_rvu_load(
    config: LoadConfig,
    session: Session,
    source_files: List[SourceFile],
    release_id: str,
) -> Dict[str, Any]:
    from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

    ingestor = RVUIngestor(str(config.output_dir), db_session=session)

    land = await ingestor._land_stage(
        release_id=release_id,
        batch_id=config.batch_id,
        source_files=source_files,
    )
    _assert_stage_success("land", land)

    raw_batch = land["raw_batch"]
    validate = await ingestor._validate_stage(raw_batch)
    _assert_stage_success("validate", validate)

    normalize = await ingestor._normalize_stage(validate, raw_batch)
    _assert_stage_success("normalize", normalize)

    enrich = await ingestor._enrich_stage(normalize)
    _assert_stage_success("enrich", enrich)

    publish = await ingestor._publish_stage(enrich)
    _assert_stage_success("publish", publish)

    return {
        "land": land,
        "validate": validate,
        "normalize": normalize,
        "enrich": enrich,
        "publish": publish,
    }


def _assert_stage_success(stage_name: str, result: Dict[str, Any]) -> None:
    if result.get("status") != "success":
        raise RVULocalLoadError(f"{stage_name} stage failed: {result.get('error') or result}")


def verify_curated_parquet(publish_result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    import pandas as pd

    curated_tables = publish_result.get("curated_tables") or {}
    verified: Dict[str, Dict[str, Any]] = {}
    missing = []
    for dataset in REQUIRED_RVU_DATASETS:
        parquet_path = curated_tables.get(dataset)
        if not parquet_path:
            missing.append(dataset)
            continue
        path = Path(parquet_path)
        if not path.exists():
            missing.append(dataset)
            continue
        row_count = len(pd.read_parquet(path))
        verified[dataset] = {
            "path": str(path),
            "rows": row_count,
        }
    if missing:
        raise RVULocalLoadError(f"Missing curated parquet artifacts for: {missing}")
    zero = [dataset for dataset, payload in verified.items() if int(payload["rows"]) <= 0]
    if zero:
        raise RVULocalLoadError(f"Curated parquet artifacts have zero rows: {zero}")
    return verified


def verify_db_counts(session: Session, release_uuid: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for dataset, table_name in DB_TABLE_BY_DATASET.items():
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE release_id = :release_uuid"),
            {"release_uuid": release_uuid},
        ).scalar_one()
        counts[dataset] = int(count)

    zero = [dataset for dataset, count in counts.items() if count <= 0]
    if zero:
        raise RVULocalLoadError(f"DB tables have zero rows for this release: {zero}; counts={counts}")
    return counts


def verify_snapshot_rows(
    session: Session,
    release_id: str,
) -> Dict[str, Dict[str, Any]]:
    from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

    expected_effective = RVUIngestor._release_effective_from(release_id)
    if expected_effective is None:
        raise RVULocalLoadError(f"Could not infer expected effective date for {release_id}")

    expected_releases = expected_dataset_release_ids(release_id)
    verified: Dict[str, Dict[str, Any]] = {}
    for dataset_id, dataset_release_id in expected_releases.items():
        row = session.execute(
            text(
                """
                SELECT dataset_id, release_id, effective_from, manifest_url
                FROM dataset_snapshots
                WHERE dataset_id = :dataset_id AND release_id = :release_id
                """
            ),
            {"dataset_id": dataset_id, "release_id": dataset_release_id},
        ).mappings().first()
        if row is None:
            raise RVULocalLoadError(f"Missing dataset_snapshot row for {dataset_id}:{dataset_release_id}")
        if row["effective_from"] != expected_effective:
            raise RVULocalLoadError(
                f"{dataset_id}:{dataset_release_id} effective_from={row['effective_from']} "
                f"but expected {expected_effective}"
            )
        verified[dataset_id] = {
            "release_id": dataset_release_id,
            "effective_from": row["effective_from"].isoformat(),
            "manifest_url": row["manifest_url"],
        }
    return verified


def verify_snapshot_selection(
    session: Session,
    release_id: str,
) -> Dict[str, Dict[str, Any]]:
    from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
    from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

    expected_effective = RVUIngestor._release_effective_from(release_id)
    if expected_effective is None:
        raise RVULocalLoadError(f"Could not infer valuation date for {release_id}")

    expected_releases = expected_dataset_release_ids(release_id)
    service = DatasetSnapshotService(session)
    selected: Dict[str, Dict[str, Any]] = {}
    for dataset_id in ("rvu_items", "gpci_indices"):
        snapshot = service.select_snapshot(dataset_id, valuation_date=expected_effective)
        expected_release_id = expected_releases[dataset_id]
        if snapshot is None:
            raise RVULocalLoadError(f"No selected snapshot for {dataset_id} at {expected_effective}")
        if snapshot.release_id != expected_release_id:
            raise RVULocalLoadError(
                f"{dataset_id} selected {snapshot.release_id} at {expected_effective}; "
                f"expected {expected_release_id}"
            )

        before_snapshot = None
        if expected_effective > date(expected_effective.year, 1, 1):
            before_snapshot = service.select_snapshot(
                dataset_id,
                valuation_date=expected_effective - timedelta(days=1),
            )
            if before_snapshot and before_snapshot.release_id == expected_release_id:
                raise RVULocalLoadError(
                    f"{dataset_id} selected {expected_release_id} before its effective date "
                    f"{expected_effective}"
                )

        selected[dataset_id] = {
            "valuation_date": expected_effective.isoformat(),
            "selected_release_id": snapshot.release_id,
            "selected_effective_from": snapshot.effective_from.isoformat(),
            "previous_day_selected_release_id": before_snapshot.release_id if before_snapshot else None,
        }
    return selected


def write_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_paths(mapping: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        key: str(value) if value is not None else None
        for key, value in mapping.items()
    }


async def run(config: LoadConfig) -> Dict[str, Any]:
    configure_database_url(config.database_url)

    engine = create_engine(config.database_url)
    SessionFactory = sessionmaker(bind=engine)

    safe_database_url = engine.url.render_as_string(hide_password=True)
    report: Dict[str, Any] = {
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "database_url": safe_database_url,
            "output_dir": str(config.output_dir),
            "start_year": config.start_year,
            "end_year": config.end_year,
            "release": config.release,
            "batch_id": config.batch_id,
            "replace_existing": config.replace_existing,
        },
    }

    scraper = CMSRVUScraper(str(config.output_dir / "discovery"))
    discovered_files = await scraper.scrape_rvu_files(
        start_year=config.start_year,
        end_year=config.end_year,
    )
    selected_files = select_release_files(discovered_files, config.release)
    selected_release_id = _release_id_for(selected_files[0])
    source_files = [source_file_from_rvu_info(file_info) for file_info in selected_files]

    report["discovery"] = {
        "manifest_path": str(scraper.last_manifest_path) if scraper.last_manifest_path else None,
        "discovered_file_count": len(discovered_files),
        "available_releases": sorted({_release_id_for(file_info) for file_info in discovered_files}),
        "selected_release_id": selected_release_id,
        "selected_files": [
            {
                "filename": file_info.filename,
                "url": file_info.url,
                "year": file_info.year,
                "quarter": file_info.quarter,
                "revision": file_info.revision,
                "size_bytes": file_info.size_bytes,
            }
            for file_info in selected_files
        ],
    }

    with SessionFactory() as session:
        if config.replace_existing:
            report["deleted_existing"] = delete_existing_release(session, selected_release_id)

        stage_results = await run_rvu_load(config, session, source_files, selected_release_id)
        publish_result = stage_results["publish"]
        database_load = publish_result.get("database_load_results") or {}
        release_uuid = database_load.get("release_uuid")
        if not release_uuid:
            raise RVULocalLoadError(f"Publish result missing database release_uuid: {database_load}")

        report["stages"] = {
            "land": {
                "files_downloaded": stage_results["land"].get("files_downloaded"),
                "manifest_path": stage_results["land"].get("manifest_path"),
                "docs_manifest_path": stage_results["land"].get("docs_manifest_path"),
            },
            "validate": {
                "quality_score": stage_results["validate"].get("quality_score"),
                "total_records": stage_results["validate"].get("total_records"),
                "rejected_records": stage_results["validate"].get("rejected_records"),
            },
            "normalize": {
                "dataset_row_counts": stage_results["normalize"].get("dataset_row_counts"),
                "parser_rejects": (stage_results["normalize"].get("metadata") or {}).get("parser_rejects", {}),
            },
            "enrich": {
                "record_count": stage_results["enrich"].get("record_count"),
            },
            "publish": {
                "record_count": publish_result.get("record_count"),
                "curated_directory": publish_result.get("curated_directory"),
                "curated_tables": _normalize_paths(publish_result.get("curated_tables") or {}),
                "database_load_results": database_load,
            },
        }

        report["verification"] = {
            "curated_parquet": verify_curated_parquet(publish_result),
            "db_counts_by_release": verify_db_counts(session, release_uuid),
            "dataset_snapshots": verify_snapshot_rows(session, selected_release_id),
            "snapshot_selection": verify_snapshot_selection(session, selected_release_id),
        }

    report["status"] = "success"
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    report["report_path"] = str(config.report_json)
    write_report(config.report_json, report)
    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args(argv)
    config = build_config(args)
    try:
        report = asyncio.run(run(config))
    except Exception as exc:
        LOGGER.exception("CMS RVU local load failed")
        raise SystemExit(str(exc)) from exc

    print(json.dumps(report["verification"], indent=2, sort_keys=True))
    print(f"\nLoaded {report['discovery']['selected_release_id']} into local/dev DB.")
    print(f"Report: {report['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
