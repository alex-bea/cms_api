"""Preflight utility for reseeding + auditing RVU/GPCI snapshots."""

from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Sequence

from cms_pricing.ops.audit_snapshot_paths import audit_snapshots
from cms_pricing.ops.repair_snapshot_paths import audit_and_repair


def _run_with_capture(func, *args, **kwargs) -> tuple[int, str]:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        code = func(*args, **kwargs)
    output = buffer.getvalue()
    return code, output


def run_preflight(dataset_ids: Sequence[str], limit: int, repair_if_needed: bool, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    overall_rc = 0

    def append(text: str) -> None:
        with log_path.open("a") as handle:
            handle.write(text)
            if not text.endswith("\n"):
                handle.write("\n")
        print(text, end="" if text.endswith("\n") else "\n")

    for dataset in dataset_ids:
        append(f"=== Audit: {dataset} ===")
        rc, output = _run_with_capture(
            audit_snapshots,
            dataset_id=dataset,
            limit=limit,
            show_all=True,
        )
        append(output)
        if rc != 0:
            if repair_if_needed:
                append(f"-- Repair requested for {dataset} --")
                backup_path = log_path.parent / f"{dataset}_repair_{log_path.stem}.csv"
                repair_rc, repair_output = _run_with_capture(
                    audit_and_repair,
                    dataset_id=dataset,
                    release_id=None,
                    limit=None,
                    dry_run=False,
                    confirm=True,
                    backup_path=backup_path,
                )
                append(repair_output)
                rc = repair_rc
                if repair_rc == 0:
                    rc, post_repair_output = _run_with_capture(
                        audit_snapshots,
                        dataset_id=dataset,
                        limit=limit,
                        show_all=True,
                    )
                    append(post_repair_output)
        append(f"=== Completed {dataset} (rc={rc}) ===\n")
        if rc != 0:
            overall_rc = rc

    append(f"Preflight finished at {datetime.utcnow().isoformat()}Z (rc={overall_rc})")
    return overall_rc


def main() -> int:
    parser = argparse.ArgumentParser(description="Run RVU/GPCI snapshot preflight audit")
    parser.add_argument(
        "--dataset-id",
        action="append",
        dest="dataset_ids",
        help="Dataset IDs to audit (default: rvu_items + gpci_indices)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum snapshots per dataset (default: 50)",
    )
    parser.add_argument(
        "--repair-if-needed",
        action="store_true",
        help="Automatically invoke repair tool when audit fails",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        help="Optional log path (default: artifacts/preflight/<timestamp>.log)",
    )

    args = parser.parse_args()
    datasets = args.dataset_ids or ["rvu_items", "gpci_indices"]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = args.log_path or Path("artifacts/preflight") / f"snapshot_preflight_{timestamp}.log"

    return run_preflight(
        dataset_ids=datasets,
        limit=args.limit,
        repair_if_needed=args.repair_if_needed,
        log_path=log_path,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
