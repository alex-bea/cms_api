"""Shared parser for CMS ZIP Code Carrier Locality source packages."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
import hashlib
import io
from pathlib import Path
import re
from typing import Any, Iterator, Sequence
from uuid import uuid4
import zipfile


CMS_SOURCE_URL = (
    "https://www.cms.gov/files/zip/"
    "zip-code-carrier-locality-file-revised-08/14/2025.zip"
)
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
    """Source-native CMS ZIP-locality row for runtime geography."""

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

    def to_mapping(
        self, *, dataset_id: str, dataset_digest: str, created_at: date
    ) -> dict[str, Any]:
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
    """Facts collected while scanning a CMS geography source package."""

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
        if (
            self.effective_from is None
            or row.effective_from < self.effective_from
        ):
            self.effective_from = row.effective_from
        if row.effective_to is None:
            self.effective_to = None
        elif self.effective_to is not None:
            self.effective_to = max(self.effective_to, row.effective_to)
        else:
            self.effective_to = row.effective_to
        if row.zip5 == probe_zip and row.has_plus4 == 0:
            self.probe_rows.append(row)

    def reject(
        self, *, source_file: str, source_line: int, error: str
    ) -> None:
        self.rejected_rows += 1
        if len(self.first_rejections) < 20:
            self.first_rejections.append(
                {
                    "source_file": source_file,
                    "source_line": source_line,
                    "error": error,
                }
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
        raise ValueError(
            f"Invalid quarter in year/quarter value {year_quarter!r}"
        )
    start_month = 1 + ((quarter - 1) * 3)
    start = date(year, start_month, 1)
    if quarter == 4:
        next_start = date(year + 1, 1, 1)
    else:
        next_start = date(year, start_month + 3, 1)
    return start, next_start - timedelta(days=1)


def _require_digits(value: str, *, length: int, field_name: str) -> None:
    if len(value) != length or not value.isdigit():
        raise ValueError(
            f"{field_name} must be {length} digits, got {value!r}"
        )


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
        raise ValueError(
            f"ZIP5 line shorter than 80 fixed-width characters: {len(raw)}"
        )

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
        raise ValueError(
            f"plus_four_flag must be 0 or 1, got {plus_four_flag!r}"
        )
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
        raise ValueError(
            f"ZIP9 line shorter than 80 fixed-width characters: {len(raw)}"
        )

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
        raise ValueError(
            f"ZIP9 plus_four_flag must be 1, got {plus_four_flag!r}"
        )
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


def _select_member(
    names: Sequence[str], pattern: re.Pattern[str], label: str
) -> str:
    matches = sorted(name for name in names if pattern.match(Path(name).name))
    if len(matches) != 1:
        raise GeographyLoadError(
            f"Expected one {label} text file, found {matches}"
        )
    return matches[0]


def source_members(source_zip: Path) -> tuple[str, str]:
    with zipfile.ZipFile(source_zip) as archive:
        names = archive.namelist()
    return (
        _select_member(names, ZIP5_MEMBER_PATTERN, "ZIP5"),
        _select_member(names, ZIP9_MEMBER_PATTERN, "ZIP9"),
    )


def _iter_member_lines(
    archive: zipfile.ZipFile, member_name: str
) -> Iterator[tuple[int, str]]:
    with archive.open(member_name) as raw_handle:
        text_handle = io.TextIOWrapper(
            raw_handle, encoding="latin-1", newline=""
        )
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
        parser = (
            parse_zip5_line
            if member_name == members[0]
            else parse_zip9_line
        )
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
        raise GeographyLoadError(
            "No valid CMS geography rows were found in the source ZIP"
        )
    if stats.rejected_rows:
        message = (
            f"Rejected {stats.rejected_rows} source rows: "
            f"{stats.first_rejections}"
        )
        raise GeographyLoadError(
            message
        )
    if stats.duplicate_source_keys:
        message = (
            f"Found {stats.duplicate_source_keys} duplicate source keys: "
            f"{stats.duplicate_examples}"
        )
        raise GeographyLoadError(
            message
        )
    return stats


def _counter_report(
    counter: Counter[str], limit: int | None = None
) -> dict[str, int]:
    items = counter.most_common(limit)
    return {key: int(value) for key, value in items}


def valuation_date_covered(
    stats: SourceStats, valuation_date: date | None
) -> bool | None:
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
            "effective_to": (
                row.effective_to.isoformat() if row.effective_to else None
            ),
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


def build_source_report(
    stats: SourceStats,
    *,
    source_url: str,
    release_id: str | None = None,
    open_ended_latest: bool = False,
    expected_probe_state: str = DEFAULT_PROBE_EXPECTED_STATE,
    expected_probe_locality: str = DEFAULT_PROBE_EXPECTED_LOCALITY,
    expected_probe_carrier: str = DEFAULT_PROBE_EXPECTED_CARRIER,
    valuation_date: date | None = None,
    require_valuation_date_coverage: bool = False,
    load_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    coverage = valuation_date_covered(stats, valuation_date)
    report = {
        "status": "ok",
        "source": {
            "source_zip": str(stats.source_zip),
            "source_files": stats.source_files,
            "source_url": source_url,
            "dataset_id": stats.dataset_id,
            "dataset_digest": stats.dataset_digest,
            "release_id": release_id or stats.release_id,
            "open_ended_latest": open_ended_latest,
        },
        "effective_window": {
            "effective_from": stats.effective_from.isoformat()
            if stats.effective_from
            else None,
            "effective_to": stats.effective_to.isoformat()
            if stats.effective_to
            else None,
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
            "locality_counts_top20": _counter_report(
                stats.locality_counts, limit=20
            ),
            "plus_four_flag_counts": _counter_report(
                stats.plus_four_flag_counts
            ),
        },
        "probe": probe_report(
            stats,
            expected_state=expected_probe_state,
            expected_locality=expected_probe_locality,
            expected_carrier=expected_probe_carrier,
        ),
        "valuation_date": (
            valuation_date.isoformat() if valuation_date else None
        ),
        "valuation_date_covered": coverage,
        "post_rvu_seedless_smoke_ready": bool(coverage),
        "load": load_result or {"dry_run": True, "inserted_rows": 0},
    }
    if require_valuation_date_coverage and coverage is False:
        report["status"] = "blocked"
        report[
            "stop_condition"
        ] = "source_effective_window_does_not_cover_valuation_date"
    return report
