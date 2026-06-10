#!/usr/bin/env python3
"""Opt-in live CMS ingestion validation.

This harness intentionally avoids production database writes. It discovers the
latest live CMS RVU release, downloads the selected source artifact(s), runs the
RVU DIS stages through publish, and writes a compact JSON validation report.

Usage:
    ENABLE_LIVE_CMS=1 python scripts/validate_live_cms_ingest.py --dataset rvu
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cms_pricing.ingestion.contracts.ingestor_spec import SourceFile  # noqa: E402
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor  # noqa: E402
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import (  # noqa: E402
    CMSRVUScraper,
    RVUFileInfo,
)


QUARTER_ORDER = {"A": 1, "B": 2, "C": 3, "D": 4}
DEFAULT_REQUIRED_RVU_DATASETS = (
    "pprrvu",
    "gpci",
    "oppscap",
    "anescf",
    "localitycounty",
)


class LiveCMSValidationError(RuntimeError):
    """Raised when live CMS validation fails an acceptance invariant."""


@dataclass
class LiveCMSValidationConfig:
    dataset: str = "rvu"
    start_year: int = field(default_factory=lambda: date.today().year)
    end_year: int = field(default_factory=lambda: date.today().year)
    release: str = "latest"
    output_dir: Path = PROJECT_ROOT / "data" / "live_cms_validation"
    report_json: Optional[Path] = None
    min_total_records: int = 1
    required_datasets: Tuple[str, ...] = DEFAULT_REQUIRED_RVU_DATASETS


class RecordingSnapshotService:
    """Snapshot service stub that records registrations without DB writes."""

    def __init__(self) -> None:
        self.registered: List[Dict[str, Any]] = []
        self.db = _RecordingSnapshotDb()

    def register_snapshot(
        self,
        dataset_id: str,
        release_id: str,
        digest: str,
        effective_from: date,
        effective_to: Optional[date] = None,
        manifest_url: Optional[str] = None,
        curated_path: Optional[str] = None,
        autocommit: bool = True,
    ) -> None:
        self.registered.append(
            {
                "dataset_id": dataset_id,
                "release_id": release_id,
                "digest": digest,
                "effective_from": effective_from.isoformat(),
                "effective_to": effective_to.isoformat() if effective_to else None,
                "manifest_url": manifest_url,
                "curated_path": curated_path,
                "autocommit": autocommit,
            }
        )

    def close(self) -> None:
        pass


class _RecordingSnapshotDb:
    def begin(self) -> "_RecordingSnapshotDb":
        return self

    def __enter__(self) -> "_RecordingSnapshotDb":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        return False


def _release_sort_key(file_info: RVUFileInfo) -> Tuple[int, int, str]:
    return (
        file_info.year,
        QUARTER_ORDER.get((file_info.quarter or "").upper(), 0),
        (file_info.revision or ""),
    )


def _release_id_for(file_info: RVUFileInfo) -> str:
    suffix = f"{file_info.quarter.upper()}{file_info.revision or ''}"
    return f"rvu_{file_info.year}_{suffix}"


def _compact_release_id_for(file_info: RVUFileInfo) -> str:
    suffix = f"{file_info.quarter.lower()}{(file_info.revision or '').lower()}"
    return f"rvu{file_info.year % 100:02d}{suffix}"


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
    """Select all files belonging to a requested release or the latest release."""
    if not files:
        raise LiveCMSValidationError("CMS RVU discovery returned no files")

    if release.lower() == "latest":
        selected_key = max(_release_sort_key(file_info) for file_info in files)
        return [file_info for file_info in files if _release_sort_key(file_info) == selected_key]

    requested = release.lower().replace("-", "_")
    matches = [
        file_info
        for file_info in files
        if requested in _release_aliases(file_info)
        or requested.replace("_", "") in _release_aliases(file_info)
    ]
    if not matches:
        available = sorted({_release_id_for(file_info) for file_info in files})
        raise LiveCMSValidationError(
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


def _file_summary(file_info: RVUFileInfo) -> Dict[str, Any]:
    return {
        "url": file_info.url,
        "filename": file_info.filename,
        "content_type": file_info.content_type,
        "file_type": file_info.file_type,
        "size_bytes": file_info.size_bytes,
        "year": file_info.year,
        "quarter": file_info.quarter,
        "revision": file_info.revision,
        "posted_at": file_info.posted_at,
        "detail_url": file_info.detail_url,
        "display_name": file_info.display_name,
        "version": file_info.version,
    }


def _normalize_paths(mapping: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        key: str(value) if value is not None else None
        for key, value in mapping.items()
    }


def _write_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _assert_stage_success(stage_name: str, result: Dict[str, Any]) -> None:
    if result.get("status") != "success":
        raise LiveCMSValidationError(
            f"{stage_name} stage failed: {result.get('error') or result}"
        )


def _assert_normalize_invariants(
    normalize_result: Dict[str, Any],
    required_datasets: Iterable[str],
) -> None:
    row_counts = normalize_result.get("dataset_row_counts") or {}
    missing = [dataset for dataset in required_datasets if row_counts.get(dataset, 0) <= 0]
    if missing:
        raise LiveCMSValidationError(
            f"Normalize stage missing required positive datasets: {missing}; row_counts={row_counts}"
        )

    schema_validation = normalize_result.get("schema_validation") or {}
    errors = schema_validation.get("errors") or []
    if errors:
        raise LiveCMSValidationError(f"Schema validation errors: {errors}")

    validation_results = schema_validation.get("validation_results") or {}
    invalid = [
        dataset
        for dataset, result in validation_results.items()
        if not result.get("valid", False)
    ]
    if invalid:
        raise LiveCMSValidationError(f"Invalid schema validation results: {invalid}")


def _assert_publish_invariants(
    publish_result: Dict[str, Any],
    required_datasets: Iterable[str],
    min_total_records: int,
) -> None:
    record_count = int(publish_result.get("record_count") or 0)
    if record_count < min_total_records:
        raise LiveCMSValidationError(
            f"Published record count {record_count} is below minimum {min_total_records}"
        )

    curated_tables = publish_result.get("curated_tables") or {}
    missing_curated = [
        dataset for dataset in required_datasets if not curated_tables.get(dataset)
    ]
    if missing_curated:
        raise LiveCMSValidationError(
            f"Publish stage missing curated parquet paths for: {missing_curated}"
        )


async def run_live_rvu_validation(config: LiveCMSValidationConfig) -> Dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = config.report_json or (
        output_dir / f"live_cms_rvu_validation_{started_at:%Y%m%d_%H%M%S}.json"
    )

    report: Dict[str, Any] = {
        "status": "running",
        "dataset": config.dataset,
        "started_at": started_at.isoformat(),
        "config": {
            "start_year": config.start_year,
            "end_year": config.end_year,
            "release": config.release,
            "output_dir": str(output_dir),
            "required_datasets": list(config.required_datasets),
            "min_total_records": config.min_total_records,
            "db_writes_enabled": False,
        },
    }

    try:
        scraper = CMSRVUScraper(output_dir=str(output_dir / "discovery"))
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
            "selected_files": [_file_summary(file_info) for file_info in selected_files],
        }

        ingestor = RVUIngestor(str(output_dir / "ingested"))
        try:
            ingestor.snapshot_service.close()
        except Exception:
            pass
        snapshot_service = RecordingSnapshotService()
        ingestor.snapshot_service = snapshot_service
        ingestor._snapshot_service_managed_session = False

        batch_id = f"live-cms-rvu-{int(time.time())}"
        land = await ingestor._land_stage(
            release_id=selected_release_id,
            batch_id=batch_id,
            source_files=source_files,
        )
        _assert_stage_success("land", land)

        raw_batch = land["raw_batch"]
        validate = await ingestor._validate_stage(raw_batch)
        _assert_stage_success("validate", validate)

        normalize = await ingestor._normalize_stage(validate, raw_batch)
        _assert_stage_success("normalize", normalize)
        _assert_normalize_invariants(normalize, config.required_datasets)

        enrich = await ingestor._enrich_stage(normalize)
        _assert_stage_success("enrich", enrich)

        publish = await ingestor._publish_stage(enrich)
        _assert_stage_success("publish", publish)
        _assert_publish_invariants(
            publish,
            config.required_datasets,
            config.min_total_records,
        )

        schema_validation = normalize.get("schema_validation") or {}
        report["stages"] = {
            "land": {
                "files_downloaded": land.get("files_downloaded"),
                "manifest_path": land.get("manifest_path"),
                "docs_manifest_path": land.get("docs_manifest_path"),
                "total_size_bytes": land.get("total_size_bytes"),
            },
            "validate": {
                "quality_score": validate.get("quality_score"),
                "total_records": validate.get("total_records"),
                "valid_records": validate.get("valid_records"),
                "rejected_records": validate.get("rejected_records"),
            },
            "normalize": {
                "normalized_records": normalize.get("normalized_records"),
                "dataset_row_counts": normalize.get("dataset_row_counts"),
                "parser_rejects": (normalize.get("metadata") or {}).get("parser_rejects", {}),
                "schema_validation": {
                    "quality_score": schema_validation.get("quality_score"),
                    "total_records": schema_validation.get("total_records"),
                    "valid_records": schema_validation.get("valid_records"),
                    "rejected_records": schema_validation.get("rejected_records"),
                    "errors": schema_validation.get("errors", []),
                    "warnings": schema_validation.get("warnings", []),
                },
            },
            "enrich": {
                "record_count": enrich.get("record_count"),
                "mapping_confidence": enrich.get("mapping_confidence"),
                "reference_data_used": enrich.get("reference_data_used", []),
            },
            "publish": {
                "record_count": publish.get("record_count"),
                "curated_directory": publish.get("curated_directory"),
                "curated_tables": _normalize_paths(publish.get("curated_tables") or {}),
                "database_load_results": publish.get("database_load_results", {}),
                "snapshot_registrations": snapshot_service.registered,
            },
        }
        report["status"] = "success"
        return report
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise
    finally:
        completed_at = datetime.now(timezone.utc)
        report["completed_at"] = completed_at.isoformat()
        report["duration_seconds"] = round((completed_at - started_at).total_seconds(), 3)
        report["report_path"] = str(report_path)
        _write_report(report_path, report)


async def run_live_validation(config: LiveCMSValidationConfig) -> Dict[str, Any]:
    if config.dataset != "rvu":
        raise LiveCMSValidationError(
            f"Unsupported dataset {config.dataset!r}; only 'rvu' is implemented"
        )
    return await run_live_rvu_validation(config)


def _parse_required_datasets(value: str) -> Tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    current_year = date.today().year
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["rvu"], default="rvu")
    parser.add_argument("--start-year", type=int, default=current_year)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument(
        "--release",
        default="latest",
        help="Release to validate, e.g. latest, rvu_2025_A, rvu25a.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "live_cms_validation",
    )
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--min-total-records", type=int, default=1)
    parser.add_argument(
        "--required-datasets",
        default=",".join(DEFAULT_REQUIRED_RVU_DATASETS),
        help="Comma-separated normalized dataset keys that must be present.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run without ENABLE_LIVE_CMS=1. Intended for manual use only.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if os.getenv("ENABLE_LIVE_CMS") != "1" and not args.force:
        print(
            "Live CMS validation is disabled. Set ENABLE_LIVE_CMS=1 or pass --force.",
            file=sys.stderr,
        )
        return 2

    config = LiveCMSValidationConfig(
        dataset=args.dataset,
        start_year=args.start_year,
        end_year=args.end_year,
        release=args.release,
        output_dir=args.output_dir,
        report_json=args.report_json,
        min_total_records=args.min_total_records,
        required_datasets=_parse_required_datasets(args.required_datasets),
    )

    try:
        report = asyncio.run(run_live_validation(config))
    except Exception as exc:
        print(f"Live CMS validation failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"Report written to {report['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
