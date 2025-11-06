"""Utilities for building MPFS curated datasets from RVU, GPCI and CF inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, Optional, Sequence

import pandas as pd
import structlog

logger = structlog.get_logger()


@dataclass
class MPFSNormalizedInputs:
    """Container for normalized MPFS inputs used by the builder."""

    rvu: pd.DataFrame
    gpci: pd.DataFrame
    conversion_factor: pd.DataFrame
    release_id: str
    vintage_date: date


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return copy with snake_cased columns and duplicate suffixes handled."""
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    counts: Dict[str, int] = {}
    new_columns: list[str] = []

    for column in normalized.columns:
        base = (
            str(column)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("%", "pct")
            .replace("(", "")
            .replace(")", "")
        )
        count = counts.get(base, 0)
        if count:
            sanitized = f"{base}_{count + 1}"
        else:
            sanitized = base
        counts[base] = count + 1
        new_columns.append(sanitized)

    normalized.columns = new_columns
    return normalized


def _rename_first(df: pd.DataFrame, candidates: Sequence[str], target: str) -> pd.DataFrame:
    """Rename the first matching candidate column to target if target absent."""
    if target in df.columns:
        return df
    for candidate in candidates:
        if candidate in df.columns:
            return df.rename(columns={candidate: target})
    return df


def _ensure_columns(df: pd.DataFrame, defaults: Dict[str, object]) -> pd.DataFrame:
    """Ensure columns exist with default values."""
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default
    return df


def _coerce_numeric(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Coerce columns to numeric floats when present."""
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def normalize_rvu(df: pd.DataFrame, release_id: str) -> pd.DataFrame:
    """Normalize RVU dataframe to schema-aligned columns."""
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "hcpcs_code",
                "modifier",
                "description",
                "status_code",
                "global_days",
                "work_rvu",
                "pe_rvu_nonfac",
                "pe_rvu_fac",
                "mp_rvu",
                "na_indicator",
                "opps_cap_applicable",
                "bilateral_ind",
                "multiple_proc_ind",
                "assistant_surg_ind",
                "co_surg_ind",
                "team_surg_ind",
                "total_nonfac",
                "total_fac",
                "effective_start",
                "effective_end",
                "release_id",
            ]
        )

    normalized = _sanitize_columns(df)

    alias_map = {
        "hcpcs_code": ("hcpcs_code", "hcpcs", "cpt", "cpt_hcpcs"),
        "modifier": ("modifier", "modifier_key", "mod"),
        "description": ("description", "short_descriptor"),
        "status_code": ("status_code", "status", "code", "status_indicator"),
        "global_days": ("global_days", "global"),
        "work_rvu": ("work_rvu", "rvu_work", "rvu"),
        "pe_rvu_nonfac": ("pe_rvu_nonfac", "rvu_pe_nonfac", "pe_rvu", "pe_nf_rvu"),
        "pe_rvu_fac": ("pe_rvu_fac", "rvu_pe_fac", "pe_rvu_2", "pe_fac_rvu"),
        "mp_rvu": ("mp_rvu", "rvu_malp", "malpractice_rvu", "rvu_2"),
        "na_indicator": ("na_indicator", "na_ind"),
        "opps_cap_applicable": ("opps_cap_applicable", "opps_cap_ind"),
        "bilateral_ind": ("bilateral_ind", "bilateral_indicator", "bilat"),
        "multiple_proc_ind": ("multiple_proc_ind", "multiple_proc_indicator", "multi"),
        "assistant_surg_ind": ("assistant_surg_ind", "assistant_indicator", "assistant"),
        "co_surg_ind": ("co_surg_ind", "co_surgeon_indicator", "co_"),
        "team_surg_ind": ("team_surg_ind", "team_surgeon_indicator", "team"),
        "total_nonfac": ("total_nonfac", "total_rvu_nonfac", "non_fac_total", "total"),
        "total_fac": ("total_fac", "total_rvu_fac", "facility_total", "total_2"),
        "effective_start": ("effective_start", "effective_from"),
        "effective_end": ("effective_end", "effective_to"),
    }

    for target, candidates in alias_map.items():
        normalized = _rename_first(normalized, candidates, target)

    normalized = _ensure_columns(
        normalized,
        {
            "modifier": "",
            "description": "",
            "status_code": "",
            "global_days": "",
            "na_indicator": "",
            "opps_cap_applicable": "",
            "bilateral_ind": "",
            "multiple_proc_ind": "",
            "assistant_surg_ind": "",
            "co_surg_ind": "",
            "team_surg_ind": "",
            "total_nonfac": 0.0,
            "total_fac": 0.0,
            "effective_start": pd.NaT,
            "effective_end": pd.NaT,
        },
    )

    numeric_columns = [
        "work_rvu",
        "pe_rvu_nonfac",
        "pe_rvu_fac",
        "mp_rvu",
        "total_nonfac",
        "total_fac",
    ]
    normalized = _coerce_numeric(normalized, numeric_columns)

    normalized["hcpcs_code"] = normalized["hcpcs_code"].astype(str).str.strip().str.upper()
    normalized["modifier"] = normalized["modifier"].fillna("").astype(str).str.strip().str.upper()
    normalized["status_code"] = normalized["status_code"].astype(str).str.strip().str.upper()
    normalized["global_days"] = normalized["global_days"].fillna("").astype(str).str.strip()

    if "effective_start" in normalized.columns:
        normalized["effective_start"] = pd.to_datetime(normalized["effective_start"], errors="coerce").dt.date
    if "effective_end" in normalized.columns:
        normalized["effective_end"] = pd.to_datetime(normalized["effective_end"], errors="coerce").dt.date

    normalized["release_id"] = release_id

    ordered_columns = [
        "hcpcs_code",
        "modifier",
        "description",
        "status_code",
        "global_days",
        "work_rvu",
        "pe_rvu_nonfac",
        "pe_rvu_fac",
        "mp_rvu",
        "na_indicator",
        "opps_cap_applicable",
        "bilateral_ind",
        "multiple_proc_ind",
        "assistant_surg_ind",
        "co_surg_ind",
        "team_surg_ind",
        "total_nonfac",
        "total_fac",
        "effective_start",
        "effective_end",
        "release_id",
    ]

    return normalized[[col for col in ordered_columns if col in normalized.columns]].reset_index(drop=True)


def normalize_gpci(df: pd.DataFrame, release_id: str) -> pd.DataFrame:
    """Normalize GPCI dataframe to schema-aligned columns."""
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "mac",
                "state",
                "locality_id",
                "locality_name",
                "work_gpci",
                "pe_gpci",
                "mp_gpci",
                "effective_start",
                "effective_end",
                "release_id",
            ]
        )

    normalized = _sanitize_columns(df)

    alias_map = {
        "mac": ("mac", "medicare_administrative_contractor"),
        "state": ("state",),
        "locality_id": ("locality_id", "locality_code", "locality_number"),
        "locality_name": ("locality_name", "fee_schedule_area"),
        "work_gpci": ("work_gpci", "gpci_work", "pw_gpci", "pw_gpci_with_1_0_floor"),
        "pe_gpci": ("pe_gpci", "gpci_pe"),
        "mp_gpci": ("mp_gpci", "gpci_mp", "gpci_malp"),
        "effective_start": ("effective_start", "effective_from", "gpci_effective_start"),
        "effective_end": ("effective_end", "effective_to", "gpci_effective_end"),
    }

    for target, candidates in alias_map.items():
        normalized = _rename_first(normalized, candidates, target)

    normalized = _ensure_columns(
        normalized,
        {
            "mac": "",
            "state": "",
            "locality_id": "",
            "locality_name": "",
            "work_gpci": 1.0,
            "pe_gpci": 0.0,
            "mp_gpci": 0.0,
            "effective_start": pd.NaT,
            "effective_end": pd.NaT,
        },
    )

    numeric_columns = ["work_gpci", "pe_gpci", "mp_gpci"]
    normalized = _coerce_numeric(normalized, numeric_columns)

    normalized["mac"] = normalized["mac"].astype(str).str.strip()
    normalized["state"] = normalized["state"].astype(str).str.strip().str.upper()
    normalized["locality_id"] = (
        normalized["locality_id"].astype(str).str.strip().str.zfill(2)
    )
    normalized["locality_name"] = normalized["locality_name"].astype(str).str.strip()

    if "effective_start" in normalized.columns:
        normalized["effective_start"] = pd.to_datetime(
            normalized["effective_start"], errors="coerce"
        ).dt.date
    if "effective_end" in normalized.columns:
        normalized["effective_end"] = pd.to_datetime(
            normalized["effective_end"], errors="coerce"
        ).dt.date

    normalized["release_id"] = release_id

    ordered_columns = [
        "mac",
        "state",
        "locality_id",
        "locality_name",
        "work_gpci",
        "pe_gpci",
        "mp_gpci",
        "effective_start",
        "effective_end",
        "release_id",
    ]

    return normalized[[col for col in ordered_columns if col in normalized.columns]].reset_index(drop=True)


def normalize_conversion_factor(df: pd.DataFrame, year: int, release_id: str) -> pd.DataFrame:
    """Normalize conversion factor dataframe to canonical columns."""
    if df is None or df.empty:
        return pd.DataFrame(
            [
                {
                    "year": year,
                    "cf_type": "physician",
                    "cf_value": float("nan"),
                    "effective_start": date(year, 1, 1),
                    "effective_end": date(year, 12, 31),
                    "release_id": release_id,
                }
            ]
        )

    normalized = _sanitize_columns(df)

    alias_map = {
        "cf_type": ("cf_type", "type", "conversion_factor_type"),
        "cf_value": ("cf_value", "conversion_factor", "cf", "factor"),
        "effective_start": ("effective_start", "effective_from", "start_date"),
        "effective_end": ("effective_end", "effective_to", "end_date"),
        "year": ("year", "cf_year", "calendar_year"),
    }

    for target, candidates in alias_map.items():
        normalized = _rename_first(normalized, candidates, target)

    numeric_candidates = [col for col in normalized.columns if normalized[col].dtype.kind in {"i", "u", "f"}]
    if "cf_value" not in normalized.columns or normalized["cf_value"].isna().all():
        for candidate in numeric_candidates:
            series = pd.to_numeric(normalized[candidate], errors="coerce").dropna()
            if not series.empty:
                normalized["cf_value"] = series
                break
    if ("cf_value" not in normalized.columns or normalized["cf_value"].isna().all()):
        for candidate in normalized.columns:
            if candidate in {"cf_type", "effective_start", "effective_end", "year"}:
                continue
            series = pd.to_numeric(normalized[candidate], errors="coerce").dropna()
            if not series.empty:
                normalized["cf_value"] = series
                break

    normalized = _ensure_columns(
        normalized,
        {
            "cf_type": "physician",
            "cf_value": float("nan"),
            "effective_start": date(year, 1, 1),
            "effective_end": date(year, 12, 31),
            "year": year,
        },
    )

    normalized["cf_value"] = pd.to_numeric(normalized["cf_value"], errors="coerce")
    normalized["effective_start"] = pd.to_datetime(
        normalized["effective_start"], errors="coerce"
    ).dt.date.fillna(date(year, 1, 1))
    normalized["effective_end"] = pd.to_datetime(
        normalized["effective_end"], errors="coerce"
    ).dt.date.fillna(date(year, 12, 31))
    normalized["year"] = normalized["year"].fillna(year).astype(int)
    normalized["cf_type"] = normalized["cf_type"].astype(str).str.strip().str.lower()
    normalized["release_id"] = release_id

    # Detect extra numeric columns that are not being used (WARN logging for governance)
    # MVP scope is physician-factor only; extra columns (e.g., anesthesia_cf, midyear_cf) are detected but unused
    expected_columns = {"year", "cf_type", "cf_value", "effective_start", "effective_end", "release_id"}
    all_numeric_cols = [col for col in normalized.columns if normalized[col].dtype.kind in {"i", "u", "f"}]
    extra_numeric_cols = [
        col for col in all_numeric_cols 
        if col not in expected_columns and col != "cf_value"
    ]
    
    # Also check for non-numeric columns that might represent additional CF types
    extra_cols = [col for col in normalized.columns if col not in expected_columns]
    
    if extra_numeric_cols or (extra_cols and any("anesthesia" in str(col).lower() or "midyear" in str(col).lower() for col in extra_cols)):
        all_extra_cols = list(set(extra_numeric_cols + extra_cols))
        logger.warning(
            "Additional CF columns present but unused",
            release_id=release_id,
            extra_columns=all_extra_cols,
            message=(
                "Additional CF columns detected but not persisted. "
                "MVP scope is physician-factor only. "
                "To extend functionality (e.g., anesthesia CF, midyear adjustments), "
                "update governance approval and extend builder logic."
            )
        )

    ordered_columns = [
        "year",
        "cf_type",
        "cf_value",
        "effective_start",
        "effective_end",
        "release_id",
    ]

    # Filter to physician factor only (MVP scope)
    result = normalized[ordered_columns].copy()
    if "cf_type" in result.columns:
        result = result[result["cf_type"] == "physician"].copy()
    
    return result.drop_duplicates().reset_index(drop=True)


def build_curated_views(
    inputs: MPFSNormalizedInputs, valuation_date: Optional[date] = None
) -> Dict[str, pd.DataFrame]:
    """Build curated MPFS datasets for a given valuation date."""
    valuation_ts = pd.Timestamp(valuation_date or inputs.vintage_date)

    rvu_df = normalize_rvu(inputs.rvu, inputs.release_id)
    gpci_df = normalize_gpci(inputs.gpci, inputs.release_id)

    cf_year = inputs.vintage_date.year
    if not inputs.conversion_factor.empty and "year" in inputs.conversion_factor.columns:
        year_series = pd.to_numeric(inputs.conversion_factor["year"], errors="coerce").dropna()
        if not year_series.empty:
            cf_year = int(year_series.iloc[0])

    cf_df = normalize_conversion_factor(inputs.conversion_factor, cf_year, inputs.release_id)
    cf_record = cf_df.iloc[0] if not cf_df.empty else None
    cf_value = float(cf_record["cf_value"]) if cf_record is not None else float("nan")
    cf_type = str(cf_record["cf_type"]) if cf_record is not None else "physician"

    # Filter RVU/GPCI by valuation date
    rvu_filtered = rvu_df.copy()
    if "effective_start" in rvu_filtered.columns:
        start = pd.to_datetime(rvu_filtered["effective_start"], errors="coerce")
        end = pd.to_datetime(rvu_filtered.get("effective_end"), errors="coerce")
        mask = (start.isna() | (start <= valuation_ts)) & (end.isna() | (end >= valuation_ts))
        rvu_filtered = rvu_filtered[mask].copy()

    gpci_filtered = gpci_df.copy()
    if "effective_start" in gpci_filtered.columns:
        start = pd.to_datetime(gpci_filtered["effective_start"], errors="coerce")
        end = pd.to_datetime(gpci_filtered.get("effective_end"), errors="coerce")
        mask = (start.isna() | (start <= valuation_ts)) & (end.isna() | (end >= valuation_ts))
        gpci_filtered = gpci_filtered[mask].copy()

    rvu_filtered = rvu_filtered.rename(
        columns={
            "effective_start": "rvu_effective_start",
            "effective_end": "rvu_effective_end",
        }
    )
    gpci_filtered = gpci_filtered.rename(
        columns={
            "effective_start": "gpci_effective_start",
            "effective_end": "gpci_effective_end",
        }
    )

    if rvu_filtered.empty or gpci_filtered.empty:
        # Return empty structures but consistent keys
        return {
            "mpfs_payment_curated": pd.DataFrame(),
            "mpfs_rvu": rvu_filtered.rename(
                columns={
                    "rvu_effective_start": "effective_start",
                    "rvu_effective_end": "effective_end",
                }
            ),
            "mpfs_gpci": gpci_filtered.rename(
                columns={
                    "gpci_effective_start": "effective_start",
                    "gpci_effective_end": "effective_end",
                }
            ),
            "mpfs_cf_vintage": cf_df,
            "mpfs_indicators_all": pd.DataFrame(columns=["hcpcs_code", "modifier", "indicator_name", "indicator_value"]),
            "mpfs_locality": pd.DataFrame(),
            "mpfs_link_keys": pd.DataFrame(columns=["hcpcs_code", "modifier", "locality_id", "cf_year", "release_id"]),
        }

    rvu_for_join = rvu_filtered.drop(columns=["release_id"], errors="ignore").assign(__cross_key=1)
    gpci_for_join = gpci_filtered.drop(columns=["release_id"], errors="ignore").assign(__cross_key=1)

    payment_df = rvu_for_join.merge(gpci_for_join, on="__cross_key", how="outer").drop(columns="__cross_key")

    numeric_columns = [
        "work_rvu",
        "pe_rvu_nonfac",
        "pe_rvu_fac",
        "mp_rvu",
        "work_gpci",
        "pe_gpci",
        "mp_gpci",
    ]
    payment_df = _coerce_numeric(payment_df, numeric_columns)
    payment_df[numeric_columns] = payment_df[numeric_columns].fillna(0.0)

    payment_df["conversion_factor"] = cf_value
    payment_df["cf_year"] = cf_year
    payment_df["cf_type"] = cf_type
    payment_df["release_id"] = inputs.release_id
    payment_df["valuation_date"] = valuation_ts.date()
    payment_df["vintage_date"] = inputs.vintage_date

    payment_df["payment_nonfacility"] = (
        payment_df["work_rvu"] * payment_df["work_gpci"]
        + payment_df["pe_rvu_nonfac"] * payment_df["pe_gpci"]
        + payment_df["mp_rvu"] * payment_df["mp_gpci"]
    ) * cf_value

    payment_df["payment_facility"] = (
        payment_df["work_rvu"] * payment_df["work_gpci"]
        + payment_df["pe_rvu_fac"] * payment_df["pe_gpci"]
        + payment_df["mp_rvu"] * payment_df["mp_gpci"]
    ) * cf_value

    payment_df["effective_start"] = payment_df.get("rvu_effective_start")
    payment_df["effective_end"] = payment_df.get("rvu_effective_end")

    payment_columns = [
        "hcpcs_code",
        "modifier",
        "mac",
        "state",
        "locality_id",
        "locality_name",
        "work_rvu",
        "pe_rvu_nonfac",
        "pe_rvu_fac",
        "mp_rvu",
        "work_gpci",
        "pe_gpci",
        "mp_gpci",
        "conversion_factor",
        "payment_nonfacility",
        "payment_facility",
        "cf_year",
        "cf_type",
        "rvu_effective_start",
        "rvu_effective_end",
        "gpci_effective_start",
        "gpci_effective_end",
        "effective_start",
        "effective_end",
        "release_id",
        "valuation_date",
        "vintage_date",
    ]

    payment_view = payment_df[[col for col in payment_columns if col in payment_df.columns]].reset_index(drop=True)

    mpfs_rvu = rvu_filtered.rename(
        columns={
            "rvu_effective_start": "effective_start",
            "rvu_effective_end": "effective_end",
        }
    ).reset_index(drop=True)
    mpfs_rvu["vintage_date"] = inputs.vintage_date
    mpfs_rvu["valuation_date"] = valuation_ts.date()
    mpfs_rvu["release_id"] = inputs.release_id

    mpfs_gpci = gpci_filtered.rename(
        columns={
            "gpci_effective_start": "effective_start",
            "gpci_effective_end": "effective_end",
        }
    ).reset_index(drop=True)
    mpfs_gpci["vintage_date"] = inputs.vintage_date
    mpfs_gpci["valuation_date"] = valuation_ts.date()
    mpfs_gpci["release_id"] = inputs.release_id

    mpfs_cf = cf_df.copy()
    mpfs_cf["valuation_date"] = valuation_ts.date()
    mpfs_cf["vintage_date"] = inputs.vintage_date

    indicator_columns = [
        col
        for col in rvu_df.columns
        if col.endswith("_ind") or col.endswith("_indicator")
    ]

    indicator_frames = []
    for column in indicator_columns:
        indicator_df = rvu_df[["hcpcs_code", "modifier", column, "effective_start", "effective_end"]].copy()
        indicator_df = indicator_df.rename(columns={column: "indicator_value"})
        indicator_df["indicator_name"] = column
        indicator_frames.append(indicator_df)

    if indicator_frames:
        mpfs_indicators = pd.concat(indicator_frames, ignore_index=True)
        mpfs_indicators = mpfs_indicators[
            ["hcpcs_code", "modifier", "indicator_name", "indicator_value", "effective_start", "effective_end"]
        ]
        mpfs_indicators["release_id"] = inputs.release_id
    else:
        mpfs_indicators = pd.DataFrame(
            columns=["hcpcs_code", "modifier", "indicator_name", "indicator_value", "effective_start", "effective_end", "release_id"]
        )

    locality_cols = [
        col
        for col in ["locality_id", "locality_name", "state", "mac", "gpci_effective_start", "gpci_effective_end"]
        if col in gpci_filtered.columns
    ]
    mpfs_locality = gpci_filtered[locality_cols].drop_duplicates().rename(
        columns={
            "gpci_effective_start": "effective_start",
            "gpci_effective_end": "effective_end",
        }
    )
    mpfs_locality["release_id"] = inputs.release_id

    link_cols = ["hcpcs_code", "modifier", "locality_id", "cf_year", "release_id"]
    mpfs_link_keys = payment_view[link_cols].drop_duplicates().reset_index(drop=True)

    return {
        "mpfs_payment_curated": payment_view,
        "mpfs_rvu": mpfs_rvu,
        "mpfs_gpci": mpfs_gpci,
        "mpfs_cf_vintage": mpfs_cf,
        "mpfs_indicators_all": mpfs_indicators.reset_index(drop=True),
        "mpfs_locality": mpfs_locality.reset_index(drop=True),
        "mpfs_link_keys": mpfs_link_keys,
    }
