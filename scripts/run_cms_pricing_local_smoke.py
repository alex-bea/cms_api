#!/usr/bin/env python3
"""Run the production-style CMS pricing local/dev smoke sequence.

This command intentionally targets local/dev databases only. It wires the
proven sequence into one evidence-producing runner:

1. load/validate CMS ZIP-locality geography with readiness gates;
2. load the selected CMS RVU release;
3. run post-RVU API smoke for a normal ZIP and a special source-state ZIP.

Use ``--dry-run-plan`` to print the exact command plan without mutating a
database.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bootstrap_local_db import (  # noqa: E402
    assert_local_database_url,
    resolve_database_url,
)


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "ingestion" / "local" / "rvu"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "ingestion" / "local" / "reports"
DEFAULT_CONSOLIDATED_REPORT = (
    DEFAULT_REPORT_DIR / "cms_pricing_local_smoke_latest.json"
)
DEFAULT_GEOGRAPHY_REPORT = (
    DEFAULT_REPORT_DIR / "cms_geography_production_readiness_latest.json"
)
DEFAULT_RVU_REPORT = DEFAULT_REPORT_DIR / "cms_rvu_local_load_latest.json"
PROOF_PATH = "production_style_local_smoke"


@dataclass(frozen=True)
class SmokeConfig:
    database_url: str
    report_json: Path
    geography_report_json: Path
    rvu_report_json: Path
    output_dir: Path
    start_year: int
    end_year: int
    release: str
    valuation_date: str
    python_executable: str
    dry_run_plan: bool


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Local/dev database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL.",
    )
    parser.add_argument("--report-json", type=Path, default=DEFAULT_CONSOLIDATED_REPORT)
    parser.add_argument(
        "--geography-report-json",
        type=Path,
        default=DEFAULT_GEOGRAPHY_REPORT,
    )
    parser.add_argument("--rvu-report-json", type=Path, default=DEFAULT_RVU_REPORT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-year", type=int, default=2026)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--release", default="latest")
    parser.add_argument("--valuation-date", default="2026-07-01")
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument(
        "--dry-run-plan",
        action="store_true",
        help="Print command plan without running local/dev database mutations.",
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> SmokeConfig:
    database_url = resolve_database_url(args.database_url)
    assert_local_database_url(database_url, allow_remote=False)
    return SmokeConfig(
        database_url=database_url,
        report_json=args.report_json,
        geography_report_json=args.geography_report_json,
        rvu_report_json=args.rvu_report_json,
        output_dir=args.output_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        release=args.release,
        valuation_date=args.valuation_date,
        python_executable=args.python_executable,
        dry_run_plan=args.dry_run_plan,
    )


def build_step_plan(config: SmokeConfig) -> list[dict[str, Any]]:
    py = config.python_executable
    db = config.database_url
    return [
        {
            "name": "geography_readiness_load",
            "command": [
                py,
                "scripts/load_cms_geography_local.py",
                "--database-url",
                db,
                "--replace-existing",
                "--open-ended-latest",
                "--require-valuation-date-coverage",
                "--production-readiness-gates",
                "--report-json",
                str(config.geography_report_json),
            ],
            "report_json": str(config.geography_report_json),
        },
        {
            "name": "rvu_local_load",
            "command": [
                py,
                "scripts/load_latest_cms_rvu_local.py",
                "--database-url",
                db,
                "--start-year",
                str(config.start_year),
                "--end-year",
                str(config.end_year),
                "--release",
                config.release,
                "--output-dir",
                str(config.output_dir),
                "--report-json",
                str(config.rvu_report_json),
            ],
            "report_json": str(config.rvu_report_json),
        },
        {
            "name": "post_rvu_smoke_94110",
            "command": [
                py,
                "scripts/post_rvu_load_api_smoke.py",
                "--database-url",
                db,
                "--valuation-date",
                config.valuation_date,
                "--proof-path",
                PROOF_PATH,
            ],
        },
        {
            "name": "post_rvu_smoke_special_state_66012",
            "command": [
                py,
                "scripts/post_rvu_load_api_smoke.py",
                "--database-url",
                db,
                "--zip",
                "66012",
                "--valuation-date",
                config.valuation_date,
                "--expected-state",
                "EK",
                "--expected-locality",
                "00",
                "--expected-carrier",
                "05202",
                "--proof-path",
                PROOF_PATH,
            ],
        },
    ]


def safe_database_url(database_url: str) -> str:
    return make_url(database_url).render_as_string(hide_password=True)


def safe_command(command: Sequence[str]) -> list[str]:
    masked = list(command)
    for index, value in enumerate(masked[:-1]):
        if value == "--database-url":
            masked[index + 1] = safe_database_url(masked[index + 1])
    return masked


def read_json_file(path: str | None) -> Any:
    if not path:
        return None
    report_path = Path(path)
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def parse_json_stdout(stdout: str) -> Any:
    stripped = stdout.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def run_step(step: dict[str, Any]) -> dict[str, Any]:
    completed = subprocess.run(
        step["command"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    result = {
        "name": step["name"],
        "command": safe_command(step["command"]),
        "return_code": completed.returncode,
        "stdout_json": parse_json_stdout(completed.stdout),
        "stderr": completed.stderr,
        "report_json": step.get("report_json"),
        "report": read_json_file(step.get("report_json")),
    }
    if completed.returncode != 0:
        result["status"] = "failed"
        raise RuntimeError(json.dumps(result, default=str, sort_keys=True))
    result["status"] = "ok"
    return result


def build_report(config: SmokeConfig, steps: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "planned" if config.dry_run_plan else "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "database_url": safe_database_url(config.database_url),
        "proof_path": PROOF_PATH,
        "reports": {
            "consolidated": str(config.report_json),
            "geography": str(config.geography_report_json),
            "rvu": str(config.rvu_report_json),
        },
        "steps": [
            {
                **{key: value for key, value in step.items() if key != "command"},
                "command": safe_command(step["command"]),
            }
            for step in steps
        ],
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(config: SmokeConfig) -> dict[str, Any]:
    steps = build_step_plan(config)
    report = build_report(config, steps)
    if config.dry_run_plan:
        report["status"] = "planned"
        write_report(config.report_json, report)
        return report

    results = []
    try:
        for step in steps:
            results.append(run_step(step))
    except RuntimeError as exc:
        report["status"] = "failed"
        report["completed_steps"] = results
        report["error"] = str(exc)
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_report(config.report_json, report)
        raise

    report["status"] = "ok"
    report["completed_steps"] = results
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    write_report(config.report_json, report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    report = run(config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
