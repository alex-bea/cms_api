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
      --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterator, Sequence
from uuid import uuid4
import zipfile

from sqlalchemy import and_, create_engine, or_
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


DEFAULT_SOURCE_ZIP = (
    PROJECT_ROOT
    / "data"
    / "cms_raw"
    / "zip-code-carrier-locality-file-revised-08-14-2025.zip"
)
CMS_SOURCE_URL = "https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip"
DEFAULT_DATASET_ID = "ZIP_LOCALITY"
DEFAULT_PROBE_ZIP = "94110"
DEFAULT_PROBE_EXPECTED_STATE = "CA"
DEFAULT_PROBE_EXPECTED_LOCALITY = "05"
DEFAULT_PROBE_EXPECTED_CARRIER = "01112"
DEFAULT_VALUATION_DATE = date(2026, 7, 1)
ZIP5_MEMBER_PATTERN = re.compile(r"^ZIP5_.*\.txt$", re.IGNORECASE)
ZIP9_MEMBER_PATTERN = re.compile(r"^ZIP9_.*\.txt$", re.IGNORECASE)


class GeographyLoadError(RuntimeError):
    """Raised when geography source validation or loading cannot continue."""


@dataclass(frozen=True)
class ParsedGeographyRow:
    source_file: str
    source_line: int
    zip5: str
    plus4: str | None
    has_plus4: int
    state: str
    carrier: str
    locality_id: str
    rural_flag: str | None
    plus_four_flag: str | None
    part_b_payment_indicator: str | None
    year_quarter: str
    effective_from: date
    effective_to: date | None

    def active_key(self) -> tuple[str, str | None, date, date | None]:
        return (self.zip5, self.plus4, self.effective_from, self.effective_to)

    def to_mapping(self, *, dataset_id: str, dataset_digest: str, created_at: date) -> dict[str, Any]:
        return {
            "id": uuid4(),
            "zip5": self.zip5,
            "plus4": self.plus4,
            "has_plus4": self.has_plus4,
            "state": self.state,
            "locality_id": self.locality_id,
            "locality_name": None,
            "carrier": self.carrier,
            "rural_flag": self.rural_flag,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to,
            "dataset_id": dataset_id,
            "dataset_digest": dataset_digest,
            "created_at": created_at,
        }


@dataclass
class SourceStats:
    source_zip: Path
    dataset_id: str
    dataset_digest: str
    source_files: list[str]
    probe_zip: str
    zip5_rows: int = 0
    zip9_rows: int = 0
    rejected_rows: int = 0
    duplicate_source_keys: int = 0
    first_rejections: list[dict[str, Any]] = field(default_factory=list)
    duplicate_examples: list[dict[str, Any]] = field(default_factory=list)
    state_counts: Counter[str] = field(default_factory=Counter)
    locality_counts: Counter[str] = field(default_factory=Counter)
    year_quarter_counts: Counter[str] = field(default_factory=Counter)
    plus_four_flag_counts: Counter[str] = field(default_factory=Counter)
    zip5_key_count: int = 0
    zip9_key_count: int = 0
    effective_from: date | None = None
    effective_to: date | None = None
    probe_rows: list[ParsedGeographyRow] = field(default_factory=list)

    @property
    def valid_rows(self) -> int:
        return self.zip5_rows + self.zip9_rows

    @property
    def release_id(self) -> str:
        if not self.year_quarter_counts:
            return "zip_locality_unknown"
        latest = max(self.year_quarter_counts)
        year = latest[:4]
        quarter = latest[4]
        return f"zip_locality_{year}_Q{quarter}"

    def record(self, row: ParsedGeographyRow, *, probe_zip: str) -> None:
        if row.has_plus4:
            self.zip9_rows += 1
        else:
            self.zip5_rows += 1
        self.state_counts[row.state] += 1
        self.locality_counts[row.locality_id] += 1
        self.year_quarter_counts[row.year_quarter] += 1
        if row.plus_four_flag is not None:
            self.plus_four_flag_counts[row.plus_four_flag] += 1
        if self.effective_from is None or row.effective_from < self.effective_from:
            self.effective_from = row.effective_from
        if row.effective_to is None:
            self.effective_to = None
        elif self.effective_to is not None:
            self.effective_to = max(self.effective_to, row.effective_to)
        else:
            self.effective_to = row.effective_to
        if row.zip5 == probe_zip and row.has_plus4 == 0:
            self.probe_rows.append(row)

    def reject(self, *, source_file: str, source_line: int, error: str) -> None:
        self.rejected_rows += 1
        if len(self.first_rejections) < 20:
            self.first_rejections.append(
                {"source_file": source_file, "source_line": source_line, "error": error}
            )

    def duplicate(self, row: ParsedGeographyRow) -> None:
        self.duplicate_source_keys += 1
        if len(self.duplicate_examples) < 20:
            self.duplicate_examples.append(
                {
                    "source_file": row.source_file,
                    "source_line": row.source_line,
                    "zip5": row.zip5,
                    "plus4": row.plus4,
                    "effective_from": row.effective_from.isoformat(),
                    "effective_to": row.effective_to.isoformat()
                    if row.effective_to
                    else None,
                }
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-zip", type=Path, default=DEFAULT_SOURCE_ZIP)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL when not using --dry-run.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete overlapping local/dev geography rows for this dataset before loading.",
    )
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--release-id", default=None)
    parser.add_argument("--manifest-url", default=CMS_SOURCE_URL)
    parser.add_argument("--report-json", type=Path, default=None)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--probe-zip", default=DEFAULT_PROBE_ZIP)
    parser.add_argument("--expected-probe-state", default=DEFAULT_PROBE_EXPECTED_STATE)
    parser.add_argument("--expected-probe-locality", default=DEFAULT_PROBE_EXPECTED_LOCALITY)
    parser.add_argument("--expected-probe-carrier", default=DEFAULT_PROBE_EXPECTED_CARRIER)
    parser.add_argument("--valuation-date", default=DEFAULT_VALUATION_DATE.isoformat())
    parser.add_argument(
        "--require-valuation-date-coverage",
        action="store_true",
        help="Exit non-zero when source effective dates do not cover --valuation-date.",
    )
    parser.add_argument(
        "--open-ended-latest",
        action="store_true",
        help=(
            "Local/dev override: store source rows with effective_to=NULL so the "
            "latest CMS ZIP-locality package remains active after its source quarter."
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

    valuation_date = date.fromisoformat(args.valuation_date) if args.valuation_date else None
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
    )


def source_zip_digest(source_zip: Path) -> str:
    digest = hashlib.sha256()
    with source_zip.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def quarter_window(year_quarter: str) -> tuple[date, date]:
    if not re.fullmatch(r"\d{5}", year_quarter or ""):
        raise ValueError(f"Invalid year/quarter value {year_quarter!r}")
    year = int(year_quarter[:4])
    quarter = int(year_quarter[4])
    if quarter not in {1, 2, 3, 4}:
        raise ValueError(f"Invalid quarter in year/quarter value {year_quarter!r}")
    start_month = 1 + ((quarter - 1) * 3)
    start = date(year, start_month, 1)
    if quarter == 4:
        next_start = date(year + 1, 1, 1)
    else:
        next_start = date(year, start_month + 3, 1)
    return start, next_start - timedelta(days=1)


def _require_digits(value: str, *, length: int, field_name: str) -> None:
    if len(value) != length or not value.isdigit():
        raise ValueError(f"{field_name} must be {length} digits, got {value!r}")


def _require_locality(value: str) -> None:
    _require_digits(value, length=2, field_name="locality_id")


def parse_zip5_line(
    line: str,
    *,
    source_file: str,
    source_line: int,
    open_ended_latest: bool = False,
) -> ParsedGeographyRow:
    raw = line.rstrip("\r\n")
    if len(raw) < 80:
        raise ValueError(f"ZIP5 line shorter than 80 fixed-width characters: {len(raw)}")

    state = raw[0:2].strip().upper()
    zip5 = raw[2:7].strip()
    carrier = raw[7:12].strip()
    locality_id = raw[12:14].strip()
    rural_flag = raw[14:15].strip() or None
    plus_four_flag = raw[20:21].strip() or None
    part_b_payment_indicator = raw[22:23].strip() or None
    year_quarter = raw[75:80].strip()

    if len(state) != 2 or not state.isalpha():
        raise ValueError(f"state must be two letters, got {state!r}")
    _require_digits(zip5, length=5, field_name="zip5")
    _require_digits(carrier, length=5, field_name="carrier")
    _require_locality(locality_id)
    if plus_four_flag not in {None, "0", "1"}:
        raise ValueError(f"plus_four_flag must be 0 or 1, got {plus_four_flag!r}")
    effective_from, effective_to = quarter_window(year_quarter)

    return ParsedGeographyRow(
        source_file=source_file,
        source_line=source_line,
        zip5=zip5,
        plus4=None,
        has_plus4=0,
        state=state,
        carrier=carrier,
        locality_id=locality_id,
        rural_flag=rural_flag,
        plus_four_flag=plus_four_flag,
        part_b_payment_indicator=part_b_payment_indicator,
        year_quarter=year_quarter,
        effective_from=effective_from,
        effective_to=None if open_ended_latest else effective_to,
    )


def parse_zip9_line(
    line: str,
    *,
    source_file: str,
    source_line: int,
    open_ended_latest: bool = False,
) -> ParsedGeographyRow:
    raw = line.rstrip("\r\n")
    if len(raw) < 80:
        raise ValueError(f"ZIP9 line shorter than 80 fixed-width characters: {len(raw)}")

    state = raw[0:2].strip().upper()
    zip5 = raw[2:7].strip()
    carrier = raw[7:12].strip()
    locality_id = raw[12:14].strip()
    rural_flag = raw[14:15].strip() or None
    plus_four_flag = raw[20:21].strip() or None
    plus4 = raw[21:25].strip()
    part_b_payment_indicator = raw[31:32].strip() or None
    year_quarter = raw[75:80].strip()

    if len(state) != 2 or not state.isalpha():
        raise ValueError(f"state must be two letters, got {state!r}")
    _require_digits(zip5, length=5, field_name="zip5")
    _require_digits(carrier, length=5, field_name="carrier")
    _require_locality(locality_id)
    if plus_four_flag != "1":
        raise ValueError(f"ZIP9 plus_four_flag must be 1, got {plus_four_flag!r}")
    _require_digits(plus4, length=4, field_name="plus4")
    effective_from, effective_to = quarter_window(year_quarter)

    return ParsedGeographyRow(
        source_file=source_file,
        source_line=source_line,
        zip5=zip5,
        plus4=plus4,
        has_plus4=1,
        state=state,
        carrier=carrier,
        locality_id=locality_id,
        rural_flag=rural_flag,
        plus_four_flag=plus_four_flag,
        part_b_payment_indicator=part_b_payment_indicator,
        year_quarter=year_quarter,
        effective_from=effective_from,
        effective_to=None if open_ended_latest else effective_to,
    )


def _select_member(names: Sequence[str], pattern: re.Pattern[str], label: str) -> str:
    matches = sorted(name for name in names if pattern.match(Path(name).name))
    if len(matches) != 1:
        raise GeographyLoadError(f"Expected one {label} text file, found {matches}")
    return matches[0]


def source_members(source_zip: Path) -> tuple[str, str]:
    with zipfile.ZipFile(source_zip) as archive:
        names = archive.namelist()
    return (
        _select_member(names, ZIP5_MEMBER_PATTERN, "ZIP5"),
        _select_member(names, ZIP9_MEMBER_PATTERN, "ZIP9"),
    )


def _iter_member_lines(archive: zipfile.ZipFile, member_name: str) -> Iterator[tuple[int, str]]:
    with archive.open(member_name) as raw_handle:
        text_handle = io.TextIOWrapper(raw_handle, encoding="latin-1", newline="")
        for line_number, line in enumerate(text_handle, start=1):
            if line.strip():
                yield line_number, line


def iter_source_rows(
    source_zip: Path,
    *,
    open_ended_latest: bool = False,
) -> Iterator[ParsedGeographyRow]:
    zip5_member, zip9_member = source_members(source_zip)
    with zipfile.ZipFile(source_zip) as archive:
        for line_number, line in _iter_member_lines(archive, zip5_member):
            yield parse_zip5_line(
                line,
                source_file=zip5_member,
                source_line=line_number,
                open_ended_latest=open_ended_latest,
            )
        for line_number, line in _iter_member_lines(archive, zip9_member):
            yield parse_zip9_line(
                line,
                source_file=zip9_member,
                source_line=line_number,
                open_ended_latest=open_ended_latest,
            )


def scan_source_zip(
    source_zip: Path,
    *,
    dataset_id: str = DEFAULT_DATASET_ID,
    dataset_digest: str | None = None,
    probe_zip: str = DEFAULT_PROBE_ZIP,
    open_ended_latest: bool = False,
) -> SourceStats:
    digest = dataset_digest or source_zip_digest(source_zip)
    members = list(source_members(source_zip))
    stats = SourceStats(
        source_zip=source_zip,
        dataset_id=dataset_id,
        dataset_digest=digest,
        source_files=members,
        probe_zip=probe_zip,
    )
    seen_zip5: set[tuple[str, str | None, date, date | None]] = set()
    seen_zip9: set[tuple[str, str | None, date, date | None]] = set()

    for member_name in members:
        parser = parse_zip5_line if member_name == members[0] else parse_zip9_line
        seen = seen_zip5 if member_name == members[0] else seen_zip9
        with zipfile.ZipFile(source_zip) as archive:
            for line_number, line in _iter_member_lines(archive, member_name):
                try:
                    row = parser(
                        line,
                        source_file=member_name,
                        source_line=line_number,
                        open_ended_latest=open_ended_latest,
                    )
                except ValueError as exc:
                    stats.reject(
                        source_file=member_name,
                        source_line=line_number,
                        error=str(exc),
                    )
                    continue
                key = row.active_key()
                if key in seen:
                    stats.duplicate(row)
                    continue
                seen.add(key)
                stats.record(row, probe_zip=probe_zip)

    stats.zip5_key_count = len(seen_zip5)
    stats.zip9_key_count = len(seen_zip9)
    if stats.valid_rows == 0:
        raise GeographyLoadError("No valid CMS geography rows were found in the source ZIP")
    if stats.rejected_rows:
        raise GeographyLoadError(
            f"Rejected {stats.rejected_rows} source rows: {stats.first_rejections}"
        )
    if stats.duplicate_source_keys:
        raise GeographyLoadError(
            f"Found {stats.duplicate_source_keys} duplicate source keys: {stats.duplicate_examples}"
        )
    return stats


def _counter_report(counter: Counter[str], limit: int | None = None) -> dict[str, int]:
    items = counter.most_common(limit)
    return {key: int(value) for key, value in items}


def valuation_date_covered(stats: SourceStats, valuation_date: date | None) -> bool | None:
    if valuation_date is None:
        return None
    if stats.effective_from is None:
        return False
    if stats.effective_from > valuation_date:
        return False
    if stats.effective_to is not None and stats.effective_to < valuation_date:
        return False
    return True


def probe_report(
    stats: SourceStats,
    *,
    expected_state: str,
    expected_locality: str,
    expected_carrier: str,
) -> dict[str, Any]:
    rows = [
        {
            "zip5": row.zip5,
            "state": row.state,
            "locality_id": row.locality_id,
            "carrier": row.carrier,
            "effective_from": row.effective_from.isoformat(),
            "effective_to": row.effective_to.isoformat() if row.effective_to else None,
        }
        for row in stats.probe_rows
    ]
    expected_found = any(
        row["state"] == expected_state
        and row["locality_id"] == expected_locality
        and row["carrier"] == expected_carrier
        for row in rows
    )
    return {
        "zip5": stats.probe_zip,
        "expected": {
            "state": expected_state,
            "locality_id": expected_locality,
            "carrier": expected_carrier,
        },
        "expected_found": expected_found,
        "rows": rows,
    }


def build_report(
    stats: SourceStats,
    *,
    config: LoadConfig,
    load_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    coverage = valuation_date_covered(stats, config.valuation_date)
    report = {
        "status": "ok",
        "source": {
            "source_zip": str(stats.source_zip),
            "source_files": stats.source_files,
            "source_url": config.manifest_url,
            "dataset_id": stats.dataset_id,
            "dataset_digest": stats.dataset_digest,
            "release_id": config.release_id or stats.release_id,
            "open_ended_latest": config.open_ended_latest,
        },
        "effective_window": {
            "effective_from": stats.effective_from.isoformat()
            if stats.effective_from
            else None,
            "effective_to": stats.effective_to.isoformat() if stats.effective_to else None,
            "year_quarters": _counter_report(stats.year_quarter_counts),
        },
        "counts": {
            "rows_total": stats.valid_rows,
            "zip5_rows": stats.zip5_rows,
            "zip9_rows": stats.zip9_rows,
            "zip5_unique_keys": stats.zip5_key_count,
            "zip9_unique_keys": stats.zip9_key_count,
            "rejected_rows": stats.rejected_rows,
            "duplicate_source_keys": stats.duplicate_source_keys,
            "locality_00_rows": int(stats.locality_counts.get("00", 0)),
        },
        "coverage": {
            "state_counts": _counter_report(stats.state_counts),
            "locality_counts_top20": _counter_report(stats.locality_counts, limit=20),
            "plus_four_flag_counts": _counter_report(stats.plus_four_flag_counts),
        },
        "probe": probe_report(
            stats,
            expected_state=config.expected_probe_state,
            expected_locality=config.expected_probe_locality,
            expected_carrier=config.expected_probe_carrier,
        ),
        "valuation_date": config.valuation_date.isoformat()
        if config.valuation_date
        else None,
        "valuation_date_covered": coverage,
        "post_rvu_seedless_smoke_ready": bool(coverage),
        "load": load_result or {"dry_run": True, "inserted_rows": 0},
    }
    if config.require_valuation_date_coverage and coverage is False:
        report["status"] = "blocked"
        report["stop_condition"] = "source_effective_window_does_not_cover_valuation_date"
    return report


def overlapping_existing_filter(stats: SourceStats):
    if stats.effective_from is None:
        raise GeographyLoadError("Cannot build existing-row filter without source effective_from")
    if stats.effective_to is None:
        return and_(
            Geography.effective_from <= date.max,
            or_(Geography.effective_to.is_(None), Geography.effective_to >= stats.effective_from),
        )
    return and_(
        Geography.effective_from <= stats.effective_to,
        or_(Geography.effective_to.is_(None), Geography.effective_to >= stats.effective_from),
    )


def delete_existing_geography(session: Session, *, dataset_id: str, stats: SourceStats) -> int:
    result = (
        session.query(Geography)
        .filter(Geography.dataset_id == dataset_id, overlapping_existing_filter(stats))
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
    if (same_digest_count or different_digest_overlap_count) and not config.replace_existing:
        raise GeographyLoadError(
            "Existing geography rows overlap this load. Pass --replace-existing for a scoped local/dev reload. "
            f"same_digest_count={same_digest_count}, "
            f"different_digest_overlap_count={different_digest_overlap_count}"
        )
    return {
        "action": "insert",
        "same_digest_count": same_digest_count,
        "overlapping_count": overlapping_count,
        "different_digest_overlap_count": different_digest_overlap_count,
    }


def ensure_snapshot(session: Session, *, config: LoadConfig, stats: SourceStats) -> dict[str, Any]:
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
                f"Dataset snapshot {config.dataset_id}:{release_id} already exists with a different digest"
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


def insert_geography_rows(session: Session, *, config: LoadConfig, stats: SourceStats) -> int:
    created_at = date.today()
    inserted = 0
    batch: list[dict[str, Any]] = []
    for row in iter_source_rows(config.source_zip, open_ended_latest=config.open_ended_latest):
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
