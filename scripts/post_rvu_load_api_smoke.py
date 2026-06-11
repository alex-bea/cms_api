#!/usr/bin/env python3
"""Run the post-RVU-load local API smoke checks.

This command uses FastAPI's in-process TestClient so it exercises the API
router/auth/database path without requiring a running uvicorn process.

Example:
    python scripts/post_rvu_load_api_smoke.py \
      --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bootstrap_local_db import (  # noqa: E402
    assert_local_database_url,
    configure_database_url,
    resolve_database_url,
)


EXPECTED_TRACE_REFS = {
    "RVU:release:rvu_2026_C",
    "GPCI:release:gpci_2026_C",
    "CF:release:rvu_2026_C",
    "CF:source:rvu_items.conversion_factor",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL. Defaults to TEST_DATABASE_URL, then DATABASE_URL.",
    )
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument("--api-key", default="dev-key-123")
    parser.add_argument("--zip", default="94110")
    parser.add_argument("--code", default="99213")
    parser.add_argument("--setting", default="MPFS")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--valuation-date", default="2026-07-01")
    parser.add_argument("--pos", default="11")
    parser.add_argument("--expected-locality", default="05")
    parser.add_argument("--expected-state", default="CA")
    parser.add_argument("--expected-carrier", default="01112")
    parser.add_argument("--expected-release", default="rvu_2026_C")
    parser.add_argument(
        "--proof-path",
        default="post_rvu_load_api_smoke",
        help=(
            "Name the evidence path for this smoke. Values that depend on "
            "scripts/seed_post_rvu_load_local.py are refused."
        ),
    )
    return parser.parse_args()


def require(condition: bool, message: str, payload: Any = None) -> None:
    if condition:
        return
    detail = f"{message}: {json.dumps(payload, default=str, sort_keys=True)}"
    raise SystemExit(detail)


def main() -> None:
    args = parse_args()
    database_url = resolve_database_url(args.database_url)
    assert_local_database_url(database_url, allow_remote=args.allow_remote)
    configure_database_url(database_url)

    from cms_pricing.ingestion.validators.cms_geography_readiness import (  # noqa: WPS433
        validate_smoke_proof_path,
    )

    proof_path = validate_smoke_proof_path(args.proof_path)
    require(
        proof_path["status"] == "ok",
        "Unacceptable post-RVU smoke proof path",
        proof_path,
    )

    from fastapi.testclient import TestClient  # noqa: WPS433
    from cms_pricing.main import app  # noqa: WPS433

    client = TestClient(app)
    headers = {"X-API-Key": args.api_key}

    health = client.get("/health", headers=headers)
    require(health.status_code == 200, "Health check failed", health.text)

    readiness = client.get("/readyz", headers=headers)
    require(readiness.status_code == 200, "Readiness check failed", readiness.text)

    geography = client.get(
        "/geography/resolve",
        params={
            "zip": args.zip,
            "valuation_year": args.year,
            "valuation_date": args.valuation_date,
            "expose_carrier": "true",
        },
        headers=headers,
    )
    require(
        geography.status_code == 200,
        "Geography resolve failed",
        geography.text,
    )
    geography_payload = geography.json()
    require(
        geography_payload.get("locality_id") == args.expected_locality,
        "Unexpected geography locality",
        geography_payload,
    )
    require(
        geography_payload.get("state") == args.expected_state,
        "Unexpected geography state",
        geography_payload,
    )
    require(
        geography_payload.get("carrier") == args.expected_carrier,
        "Unexpected geography carrier",
        geography_payload,
    )

    pricing = client.get(
        "/pricing/codes/price",
        params={
            "zip": args.zip,
            "code": args.code,
            "setting": args.setting,
            "year": args.year,
            "valuation_date": args.valuation_date,
            "pos": args.pos,
        },
        headers=headers,
    )
    require(pricing.status_code == 200, "Pricing smoke failed", pricing.text)
    pricing_payload = pricing.json()
    require(
        pricing_payload.get("allowed_cents", 0) > 0,
        "Pricing allowed amount is not positive",
        pricing_payload,
    )
    require(
        pricing_payload.get("release_id") == args.expected_release,
        "Unexpected pricing release",
        pricing_payload,
    )
    require(
        pricing_payload.get("geography", {}).get("locality_id")
        == args.expected_locality,
        "Unexpected pricing geography locality",
        pricing_payload.get("geography"),
    )

    trace_refs = set(pricing_payload.get("trace_refs") or [])
    missing_refs = sorted(EXPECTED_TRACE_REFS - trace_refs)
    require(not missing_refs, "Missing expected trace refs", missing_refs)

    print(
        json.dumps(
            {
                "status": "ok",
                "geography": {
                    "locality_id": geography_payload.get("locality_id"),
                    "state": geography_payload.get("state"),
                    "carrier": geography_payload.get("carrier"),
                    "match_level": geography_payload.get("match_level"),
                },
                "pricing": {
                    "allowed_cents": pricing_payload.get("allowed_cents"),
                    "release_id": pricing_payload.get("release_id"),
                    "dataset_id": pricing_payload.get("dataset_id"),
                    "trace_refs": pricing_payload.get("trace_refs"),
                },
                "proof_path": proof_path,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
