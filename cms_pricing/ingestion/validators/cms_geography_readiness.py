"""Production-readiness gates for CMS ZIP-locality ingestion evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Sequence

from cms_pricing.engines.mpfs import MPSFEngine
from cms_pricing.ingestion.parsers.cms_geography import (
    DEFAULT_PROBE_EXPECTED_CARRIER,
    DEFAULT_PROBE_EXPECTED_LOCALITY,
    DEFAULT_PROBE_EXPECTED_STATE,
    DEFAULT_PROBE_ZIP,
    DEFAULT_VALUATION_DATE,
    SourceStats,
    probe_report,
    valuation_date_covered,
)


@dataclass(frozen=True)
class GeographyReadinessThresholds:
    """Configurable expected facts for a CMS ZIP-locality source package."""

    rows_total_min: int
    zip5_rows_min: int
    zip9_rows_min: int
    rejected_rows_max: int = 0
    duplicate_source_keys_max: int = 0
    locality_00_rows_min: int = 0
    expected_probe_zip: str = DEFAULT_PROBE_ZIP
    expected_probe_state: str = DEFAULT_PROBE_EXPECTED_STATE
    expected_probe_locality: str = DEFAULT_PROBE_EXPECTED_LOCALITY
    expected_probe_carrier: str = DEFAULT_PROBE_EXPECTED_CARRIER


CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS = GeographyReadinessThresholds(
    rows_total_min=1_118_970,
    zip5_rows_min=42_956,
    zip9_rows_min=1_076_014,
    rejected_rows_max=0,
    duplicate_source_keys_max=0,
    locality_00_rows_min=39_476,
)

DISALLOWED_SMOKE_PROOF_PATHS = {
    "seed_post_rvu_load_local.py",
    "scripts/seed_post_rvu_load_local.py",
    "one_row_seed_helper",
    "seed-helper",
}


def _gate(
    name: str,
    *,
    passed: bool,
    message: str,
    observed: dict[str, Any],
    expected: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "message": message,
        "observed": observed,
        "expected": expected,
    }


def validate_source_readiness(
    stats: SourceStats,
    *,
    thresholds: GeographyReadinessThresholds = CMS_ZIP_LOCALITY_2025Q4_THRESHOLDS,
    valuation_date: date | None = DEFAULT_VALUATION_DATE,
    require_valuation_date_coverage: bool = True,
) -> dict[str, Any]:
    """Validate source scan facts against production-readiness gates."""

    source_clean = (
        stats.valid_rows > 0
        and stats.rejected_rows <= thresholds.rejected_rows_max
        and stats.duplicate_source_keys <= thresholds.duplicate_source_keys_max
    )
    row_counts_pass = (
        stats.valid_rows >= thresholds.rows_total_min
        and stats.zip5_rows >= thresholds.zip5_rows_min
        and stats.zip9_rows >= thresholds.zip9_rows_min
    )
    locality_00_rows = int(stats.locality_counts.get("00", 0))
    locality_00_pass = locality_00_rows >= thresholds.locality_00_rows_min

    probe = probe_report(
        stats,
        expected_state=thresholds.expected_probe_state,
        expected_locality=thresholds.expected_probe_locality,
        expected_carrier=thresholds.expected_probe_carrier,
    )
    probe_pass = (
        stats.probe_zip == thresholds.expected_probe_zip and probe["expected_found"]
    )

    coverage = valuation_date_covered(stats, valuation_date)
    coverage_pass = True
    if require_valuation_date_coverage:
        coverage_pass = coverage is True

    gates = [
        _gate(
            "source_package_clean",
            passed=source_clean,
            message="Source package has valid rows, no rejects, and no duplicate active keys.",
            observed={
                "rows_total": stats.valid_rows,
                "rejected_rows": stats.rejected_rows,
                "duplicate_source_keys": stats.duplicate_source_keys,
            },
            expected={
                "rows_total_min": 1,
                "rejected_rows_max": thresholds.rejected_rows_max,
                "duplicate_source_keys_max": thresholds.duplicate_source_keys_max,
            },
        ),
        _gate(
            "row_count_regression",
            passed=row_counts_pass,
            message="Source row counts meet or exceed the configured package floor.",
            observed={
                "rows_total": stats.valid_rows,
                "zip5_rows": stats.zip5_rows,
                "zip9_rows": stats.zip9_rows,
            },
            expected={
                "rows_total_min": thresholds.rows_total_min,
                "zip5_rows_min": thresholds.zip5_rows_min,
                "zip9_rows_min": thresholds.zip9_rows_min,
            },
        ),
        _gate(
            "locality_00_preserved",
            passed=locality_00_pass,
            message="CMS source locality 00 remains present and is not normalized away.",
            observed={"locality_00_rows": locality_00_rows},
            expected={"locality_00_rows_min": thresholds.locality_00_rows_min},
        ),
        _gate(
            "probe_zip_match",
            passed=probe_pass,
            message="Configured probe ZIP resolves to the expected state/locality/carrier.",
            observed=probe,
            expected={
                "zip5": thresholds.expected_probe_zip,
                "state": thresholds.expected_probe_state,
                "locality_id": thresholds.expected_probe_locality,
                "carrier": thresholds.expected_probe_carrier,
            },
        ),
        _gate(
            "valuation_date_coverage",
            passed=coverage_pass,
            message="Source effective window covers the requested valuation date.",
            observed={
                "valuation_date": valuation_date.isoformat()
                if valuation_date
                else None,
                "effective_from": stats.effective_from.isoformat()
                if stats.effective_from
                else None,
                "effective_to": stats.effective_to.isoformat()
                if stats.effective_to
                else None,
                "valuation_date_covered": coverage,
                "coverage_required": require_valuation_date_coverage,
            },
            expected={"valuation_date_covered": True},
        ),
    ]
    failed = [gate["name"] for gate in gates if gate["status"] != "pass"]
    return {
        "status": "ok" if not failed else "blocked",
        "profile": "cms_zip_locality_2025q4",
        "failed_gates": failed,
        "gates": gates,
    }


@dataclass(frozen=True)
class StateLocalityPair:
    """Minimal key used to validate geography-to-GPCI readiness."""

    state: str
    locality_id: str

    @classmethod
    def from_values(cls, state: Any, locality_id: Any) -> "StateLocalityPair":
        return cls(
            state=str(state or "").strip().upper(),
            locality_id=str(locality_id or "").strip(),
        )


def validate_gpci_join_readiness(
    geography_pairs: Iterable[StateLocalityPair],
    gpci_pairs: Iterable[StateLocalityPair],
    *,
    sample_limit: int = 20,
) -> dict[str, Any]:
    """Validate active geography state/locality pairs can join to GPCI rows."""

    gpci_keys = {
        StateLocalityPair.from_values(pair.state, locality)
        for pair in gpci_pairs
        for locality in MPSFEngine._locality_candidates(pair.locality_id)
    }
    direct = 0
    mapped = 0
    misses: list[dict[str, Any]] = []

    for geography_pair in geography_pairs:
        pair = StateLocalityPair.from_values(
            geography_pair.state, geography_pair.locality_id
        )
        locality_candidates = MPSFEngine._locality_candidates(pair.locality_id)
        state_candidates = MPSFEngine._gpci_state_candidates(
            pair.state, pair.locality_id
        )
        matched_index: int | None = None
        for index, candidate_state in enumerate(state_candidates):
            if any(
                StateLocalityPair(candidate_state, locality) in gpci_keys
                for locality in locality_candidates
            ):
                matched_index = index
                break

        if matched_index == 0:
            direct += 1
        elif matched_index is not None:
            mapped += 1
        else:
            misses.append(
                {
                    "state": pair.state,
                    "locality_id": pair.locality_id,
                    "state_candidates": state_candidates,
                    "locality_candidates": locality_candidates,
                }
            )

    return {
        "status": "ok" if not misses else "blocked",
        "geography_pair_count": direct + mapped + len(misses),
        "direct_join_count": direct,
        "mapped_join_count": mapped,
        "missing_join_count": len(misses),
        "missing_examples": misses[:sample_limit],
    }


def validate_smoke_proof_path(proof_path: str) -> dict[str, Any]:
    """Refuse smoke evidence that depends on the one-row local seed helper."""

    normalized = str(proof_path or "").strip()
    script_name = normalized.rsplit("/", 1)[-1]
    disallowed = (
        normalized in DISALLOWED_SMOKE_PROOF_PATHS
        or script_name in DISALLOWED_SMOKE_PROOF_PATHS
    )
    return {
        "status": "blocked" if disallowed else "ok",
        "proof_path": normalized,
        "accepted": not disallowed,
        "stop_condition": "seed_helper_proof_path_refused" if disallowed else None,
    }
