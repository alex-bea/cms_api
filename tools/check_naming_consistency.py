#!/usr/bin/env python3
"""
Naming consistency auditor.

Compares schema contracts, parser modules, and database models to flag
column/natural-key mismatches across the RVU ingestion stack.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CONTRACTS_DIR = REPO_ROOT / "cms_pricing" / "ingestion" / "contracts"

# --------------------------------------------------------------------------- #
# Dataset configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DatasetConfig:
    parser_module: str
    natural_keys_attr: str = "NATURAL_KEYS"
    alias_attributes: Sequence[str] = ("ALIAS_MAP", "COLUMN_ALIASES")
    model_path: Optional[Tuple[str, str]] = None  # (module, class)

    # Schema columns to ignore when comparing against parser alias targets
    schema_columns_ignored_for_parser: Set[str] = field(default_factory=set)

    # Schema columns that intentionally do not land in the model (e.g. metadata-only)
    schema_columns_ignored_for_model: Set[str] = field(default_factory=set)

    # Mapping of schema column -> model column (for renamed fields)
    schema_to_model_overrides: Dict[str, str] = field(default_factory=dict)

    # Mapping of schema column -> parser column when parser exposes a synonym
    schema_to_parser_overrides: Dict[str, str] = field(default_factory=dict)


DATASET_CONFIG: Dict[str, DatasetConfig] = {
    # Geographic Practice Cost Indices
    "cms_gpci": DatasetConfig(
        parser_module="cms_pricing.ingestion.parsers.gpci_parser",
        model_path=("cms_pricing.models.rvu", "GPCIIndex"),
        schema_columns_ignored_for_parser={
            "row_content_hash",
            "source_release",
            "source_inner_file",
            "effective_to",
            "locality_name",
            "state",
        },
        schema_columns_ignored_for_model={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_to_model_overrides={
            "locality_code": "locality_id",
            "effective_from": "effective_start",
            "effective_to": "effective_end",
            "gpci_work": "work_gpci",
            "gpci_pe": "pe_gpci",
            "gpci_mp": "mp_gpci",
        },
        schema_to_parser_overrides={
            "effective_from": "effective_from",
            "effective_to": "effective_to",
        },
    ),
    # Physician Relative Value Units (PPRRVU)
    "cms_pprrvu": DatasetConfig(
        parser_module="cms_pricing.ingestion.parsers.pprrvu_parser",
        model_path=("cms_pricing.models.rvu", "RVUItem"),
        schema_columns_ignored_for_parser={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_columns_ignored_for_model={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_to_model_overrides={
            "hcpcs": "hcpcs_code",
            "modifier": "modifier_key",
            "rvu_work": "work_rvu",
            "rvu_pe_nonfac": "pe_rvu_nonfac",
            "rvu_pe_fac": "pe_rvu_fac",
            "rvu_malp": "mp_rvu",
            "effective_from": "effective_start",
            "effective_to": "effective_end",
        },
    ),
    # OPPS caps
    "cms_oppscap": DatasetConfig(
        parser_module="cms_pricing.ingestion.parsers.oppscap_parser",
        model_path=("cms_pricing.models.rvu", "OPPSCap"),
        schema_columns_ignored_for_parser={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_columns_ignored_for_model={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_to_model_overrides={
            "hcpcs": "hcpcs_code",
            "modifier": "modifier",  # direct pass-through if stored separately
            "status": "proc_status",
            "locality_code": "locality_id",
            "facility_price": "price_fac",
            "nonfacility_price": "price_nonfac",
            "effective_from": "effective_start",
            "effective_to": "effective_end",
        },
    ),
    # Anesthesia conversion factors
    "cms_anescf": DatasetConfig(
        parser_module="cms_pricing.ingestion.parsers.anes_parser",
        model_path=("cms_pricing.models.rvu", "AnesCF"),
        schema_columns_ignored_for_parser={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_columns_ignored_for_model={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_to_model_overrides={
            "locality_code": "locality_id",
            "anesthesia_cf_usd": "anesthesia_cf",
            "effective_from": "effective_start",
            "effective_to": "effective_end",
        },
    ),
    # Locality to county crosswalk
    "cms_localitycounty": DatasetConfig(
        parser_module="cms_pricing.ingestion.parsers.locality_parser",
        model_path=("cms_pricing.models.rvu", "LocalityCounty"),
        schema_columns_ignored_for_parser={
            "row_content_hash",
            "source_release",
            "source_inner_file",
        },
        schema_columns_ignored_for_model={
            "row_content_hash",
            "source_release",
            "source_inner_file",
            "state_fips",
            "county_fips",
            "locality_name",
        },
        schema_to_model_overrides={
            "locality_code": "locality_id",
            "effective_from": "effective_start",
            "effective_to": "effective_end",
        },
    ),
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def parse_version_from_filename(path: Path) -> Tuple[int, ...]:
    """
    Convert dataset filename suffix `_vX.Y.json` to tuple for sorting.

    Example: cms_gpci_v1.3.json -> (1, 3)
    """
    try:
        suffix = path.stem.split("_v", 1)[1]
    except IndexError:
        return (0,)
    parts = []
    for token in suffix.split("."):
        try:
            parts.append(int(token))
        except ValueError:
            # Fallback: strip non-digit prefix/suffix
            digits = "".join(ch for ch in token if ch.isdigit())
            parts.append(int(digits) if digits else 0)
    return tuple(parts)


def load_latest_schema(dataset_name: str, contracts_dir: Path) -> Tuple[Dict, Optional[str]]:
    """Load the most recent schema JSON for dataset."""
    candidates = sorted(
        contracts_dir.glob(f"{dataset_name}_v*.json"),
        key=parse_version_from_filename,
    )
    if not candidates:
        return {}, None
    latest = candidates[-1]
    with latest.open("r") as fh:
        return json.load(fh), latest.name


def import_object(module_path: str, attr: str):
    module = import_module(module_path)
    return getattr(module, attr)


def iter_alias_targets(module, attributes: Sequence[str]) -> Set[str]:
    targets: Set[str] = set()
    for attr in attributes:
        value = getattr(module, attr, None)
        if isinstance(value, dict):
            targets.update(str(v) for v in value.values())
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            # Some parsers expose aliases as list of tuples
            for item in value:
                if isinstance(item, (tuple, list)) and len(item) >= 2:
                    targets.add(str(item[1]))
    return targets


def resolve_schema_columns(schema_dict: Dict) -> Set[str]:
    if not schema_dict:
        return set()
    return set(schema_dict.get("columns", {}).keys())


def resolve_schema_natural_keys(schema_dict: Dict) -> List[str]:
    return list(schema_dict.get("natural_keys", []))


def map_schema_column(column: str, overrides: Dict[str, str]) -> str:
    return overrides.get(column, column)


def load_model_columns(model_module: str, model_name: str) -> Set[str]:
    model_cls = import_object(model_module, model_name)
    return {column.name for column in model_cls.__table__.columns}


@dataclass
class CheckIssue:
    check: str
    detail: str
    severity: str = "ERROR"


@dataclass
class DatasetReport:
    dataset_name: str
    schema_file: Optional[str]
    issues: List[CheckIssue] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(issue for issue in self.issues if issue.severity == "ERROR")

    def to_dict(self) -> Dict:
        return {
            "dataset": self.dataset_name,
            "ok": self.ok,
            "schema_file": self.schema_file,
            "issues": [
                {"check": issue.check, "detail": issue.detail, "severity": issue.severity}
                for issue in self.issues
            ],
            "notes": self.notes,
        }


# --------------------------------------------------------------------------- #
# Parser metadata collection (via subprocess to isolate segfaulting imports)
# --------------------------------------------------------------------------- #


@dataclass
class ParserMetadata:
    alias_targets: Set[str]
    natural_keys: Optional[List[str]]
    note: Optional[str] = None


def _parser_metadata_worker(
    module_path: str,
    alias_attrs: Sequence[str],
    natural_keys_attr: str,
    queue: mp.Queue,
) -> None:
    from importlib import import_module

    try:
        module = import_module(module_path)
        alias_targets = iter_alias_targets(module, alias_attrs)
        natural_keys = getattr(module, natural_keys_attr, None)
        if natural_keys is not None and not isinstance(natural_keys, (list, tuple)):
            natural_keys = list(natural_keys)
        queue.put(
            {
                "alias_targets": list(alias_targets),
                "natural_keys": list(natural_keys) if natural_keys is not None else None,
            }
        )
    except Exception as exc:  # pragma: no cover - defensive path
        queue.put({"error": repr(exc)})


def collect_parser_metadata(
    module_path: str,
    alias_attrs: Sequence[str],
    natural_keys_attr: str,
) -> ParserMetadata:
    """
    Import parser module in an isolated subprocess and extract alias/natural key metadata.

    Some parser modules pull in native dependencies that have segfaulted in certain
    environments. By importing within a subprocess we can avoid crashing the
    parent process and still gather the information when possible.
    """

    queue: mp.Queue = mp.Queue()

    proc = mp.Process(
        target=_parser_metadata_worker,
        args=(module_path, alias_attrs, natural_keys_attr, queue),
    )
    proc.start()
    proc.join()

    if proc.exitcode != 0:
        note = f"Parser module {module_path} exited with code {proc.exitcode}"
        if proc.exitcode and proc.exitcode < 0:
            note += f" (signal {-proc.exitcode})"
        return ParserMetadata(alias_targets=set(), natural_keys=None, note=note)

    if queue.empty():
        return ParserMetadata(alias_targets=set(), natural_keys=None, note="Parser metadata unavailable (empty queue).")

    payload = queue.get()
    if "error" in payload:
        return ParserMetadata(alias_targets=set(), natural_keys=None, note=f"Parser import error: {payload['error']}")

    return ParserMetadata(
        alias_targets=set(payload.get("alias_targets", [])),
        natural_keys=payload.get("natural_keys"),
        note=None,
    )


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #


def check_parser_aliases(
    report: DatasetReport,
    dataset_config: DatasetConfig,
    schema_columns: Set[str],
    schema_dict: Dict,
    parser_metadata: "ParserMetadata",
) -> None:
    alias_targets = parser_metadata.alias_targets
    if not alias_targets:
        if parser_metadata.note is not None:
            report.issues.append(
                CheckIssue(
                    check="parser_alias_columns",
                    detail=parser_metadata.note,
                    severity="WARN",
                )
            )
        else:
            report.notes.append("Parser alias map not found; skipping alias consistency check.")
        return

    ignored = dataset_config.schema_columns_ignored_for_parser
    expected_columns = {
        dataset_config.schema_to_parser_overrides.get(col, col)
        for col in schema_columns
        if col not in ignored
    }

    missing = sorted(expected_columns - alias_targets)
    if missing:
        report.issues.append(
            CheckIssue(
                check="parser_alias_columns",
                detail=f"Parser alias map missing canonical columns: {missing}",
            )
        )


def check_parser_natural_keys(
    report: DatasetReport,
    dataset_config: DatasetConfig,
    schema_natural_keys: List[str],
    parser_metadata: "ParserMetadata",
) -> None:
    parser_keys = parser_metadata.natural_keys
    if not parser_keys:
        if parser_metadata.note is not None:
            report.issues.append(
                CheckIssue(
                    check="parser_natural_keys",
                    detail=parser_metadata.note,
                    severity="WARN",
                )
            )
        else:
            report.notes.append("Parser natural key attribute missing; skipping natural key check.")
        return

    canonical_keys = list(schema_natural_keys)
    if not canonical_keys:
        report.notes.append("Schema natural keys not present; skipping natural key check.")
        return

    parser_keys_norm = [
        dataset_config.schema_to_parser_overrides.get(key, key) for key in parser_keys
    ]

    if canonical_keys != parser_keys_norm:
        report.issues.append(
            CheckIssue(
                check="parser_natural_keys",
                detail=f"Parser natural keys {parser_keys_norm} do not match schema {canonical_keys}",
            )
        )


def check_model_alignment(
    report: DatasetReport,
    dataset_config: DatasetConfig,
    schema_columns: Set[str],
    schema_dict: Dict,
) -> None:
    if not dataset_config.model_path:
        report.notes.append("No model mapping configured; skipping model alignment check.")
        return

    model_module, model_class = dataset_config.model_path
    try:
        model_columns = load_model_columns(model_module, model_class)
    except AttributeError as exc:
        report.issues.append(
            CheckIssue(
                check="model_import",
                detail=f"Could not load model {model_module}.{model_class}: {exc}",
            )
        )
        return

    ignored = dataset_config.schema_columns_ignored_for_model
    missing_in_model: List[str] = []
    for column in sorted(schema_columns):
        if column in ignored:
            continue
        mapped = map_schema_column(column, dataset_config.schema_to_model_overrides)
        if mapped not in model_columns:
            missing_in_model.append(f"{column}→{mapped}")

    if missing_in_model:
        report.issues.append(
            CheckIssue(
                check="model_column_coverage",
                detail=f"Model missing columns for schema fields: {missing_in_model}",
            )
        )

    # Optional: flag extra model columns not represented in schema
    reverse_overrides = {v: k for k, v in dataset_config.schema_to_model_overrides.items()}
    canonical_columns = {map_schema_column(col, dataset_config.schema_to_model_overrides) for col in schema_columns}
    extra_columns = sorted(
        col for col in model_columns if col not in canonical_columns and col not in dataset_config.schema_columns_ignored_for_model
    )
    if extra_columns:
        report.notes.append(
            f"Model columns not present in schema (may be expected): "
            f"{[reverse_overrides.get(col, col) for col in extra_columns]}"
        )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def run_checks(datasets: Sequence[str], output_json: Optional[Path]) -> int:
    contracts_dir = DEFAULT_CONTRACTS_DIR
    reports: List[DatasetReport] = []

    for dataset_name in datasets:
        config = DATASET_CONFIG.get(dataset_name)
        if not config:
            reports.append(
                DatasetReport(
                    dataset_name=dataset_name,
                    schema_file=None,
                    issues=[
                        CheckIssue(
                            check="configuration",
                            detail=f"No dataset configuration found for {dataset_name}",
                        )
                    ],
                )
            )
            continue

        schema_dict, schema_filename = load_latest_schema(dataset_name, contracts_dir)
        if not schema_dict:
            reports.append(
                DatasetReport(
                    dataset_name=dataset_name,
                    schema_file=None,
                    issues=[
                        CheckIssue(
                            check="schema_contract",
                            detail=f"No schema contract found for {dataset_name}",
                        )
                    ],
                )
            )
            continue

        report = DatasetReport(dataset_name=dataset_name, schema_file=schema_filename)
        schema_columns = resolve_schema_columns(schema_dict)
        schema_natural_keys = resolve_schema_natural_keys(schema_dict)

        parser_metadata = collect_parser_metadata(
            config.parser_module,
            config.alias_attributes,
            config.natural_keys_attr,
        )
        if parser_metadata.note and parser_metadata.alias_targets and parser_metadata.natural_keys:
            report.notes.append(parser_metadata.note)
        check_parser_aliases(report, config, schema_columns, schema_dict, parser_metadata)
        check_parser_natural_keys(report, config, schema_natural_keys, parser_metadata)
        check_model_alignment(report, config, schema_columns, schema_dict)

        reports.append(report)

    if output_json:
        payload = {"datasets": [report.to_dict() for report in reports]}
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2))

    # Human-readable output
    for report in reports:
        status = "OK" if report.ok else "FAIL"
        schema_info = f" ({report.schema_file})" if report.schema_file else ""
        print(f"[{status}] {report.dataset_name}{schema_info}")
        for issue in report.issues:
            print(f"  - {issue.severity}: {issue.check} -> {issue.detail}")
        for note in report.notes:
            print(f"    note: {note}")
        print()

    failures = sum(0 if report.ok else 1 for report in reports)
    return 0 if failures == 0 else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Check naming consistency across ingestion components.")
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=sorted(DATASET_CONFIG.keys()),
        help="Dataset names to check (default: all configured datasets)",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write JSON report",
    )
    args = parser.parse_args(argv)
    return run_checks(args.datasets, args.output_json)


if __name__ == "__main__":
    sys.exit(main())
