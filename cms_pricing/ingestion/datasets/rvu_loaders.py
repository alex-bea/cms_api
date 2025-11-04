"""
RVU dataset loader utilities.

Phase 2 Refactoring Context:
    - Step 2: Database loader extraction
      • Plan: artifacts/phase2_completion_plan.md (§Step 2)
      • Verification: docs/ingestion_verification.md (loader checks)

This module centralizes the database loading logic that was previously embedded
in `RVUIngestor`. Functions operate on pandas DataFrames produced by the
normalize stage and persist them into the RVU schema tables.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import delete

from cms_pricing.models.rvu import (
    Release,
    RVUItem,
    GPCIIndex,
    OPPSCap,
    AnesCF,
    LocalityCounty,
)

logger = structlog.get_logger()

BULK_INSERT_CHUNK_SIZE = 5000

# State name to USPS 2-character abbreviation mapping
_STATE_NAME_TO_ABBR = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'GUAM': 'GU', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN',
    'IOWA': 'IA', 'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME',
    'MARYLAND': 'MD', 'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH',
    'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND',
    'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI',
    'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
    'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI',
    'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC',
    # Handle compound names
    'HAWAII/GUAM': 'HI',  # Default to first state for compound names
    'ALASKA': 'AK',
}

def _state_name_to_abbr(state_name: str) -> str:
    """Convert full state name to 2-character USPS abbreviation."""
    if not state_name or pd.isna(state_name):
        return ''
    
    # Normalize: uppercase, strip whitespace
    state_upper = str(state_name).strip().upper()
    
    # Direct lookup
    if state_upper in _STATE_NAME_TO_ABBR:
        return _STATE_NAME_TO_ABBR[state_upper]
    
    # Handle compound names (e.g., "HAWAII/GUAM") - use first part
    if '/' in state_upper:
        first_part = state_upper.split('/')[0].strip()
        if first_part in _STATE_NAME_TO_ABBR:
            return _STATE_NAME_TO_ABBR[first_part]
    
    # Fallback: try to match by starting with state name
    for state, abbr in _STATE_NAME_TO_ABBR.items():
        if state_upper.startswith(state) or state.startswith(state_upper):
            return abbr
    
    # Last resort: truncate to 2 characters (might be invalid, but prevents DB error)
    logger.warning("Could not map state name to abbreviation", state_name=state_name, truncated=state_upper[:2])
    return state_upper[:2] if len(state_upper) >= 2 else state_upper


# Phase 2 Step 2: Database loader extraction
# See: artifacts/phase2_completion_plan.md (§Step 2)
def load_rvu_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    release_id: str,
    batch_id: str,
    vintage_date: str,
    db_session: Any,
) -> Dict[str, Any]:
    """
    Persist all RVU dataset DataFrames to the database using dataset-specific loaders.
    """
    if not db_session:
        logger.info("No database session configured; skipping DB load")
        return {"total_records": 0, "datasets": {}}

    release_uuid = uuid.uuid4()
    source_version = (release_id if release_id else _derive_source_version(vintage_date))[:10]
    release_record = Release(
        id=release_uuid,
        type="RVU_FULL",
        source_version=source_version,
        imported_at=datetime.utcnow().date(),
        notes=str(batch_id)[:10],
    )

    db_session.add(release_record)
    db_session.flush()

    natural_keys = {
        "pprrvu": ["hcpcs_code", "modifier_key", "effective_start"],
        "gpci": ["mac", "locality_id", "effective_start"],
        "oppscap": ["hcpcs_code", "modifier", "mac", "locality_id", "effective_start"],
        "anescf": ["mac", "locality_id", "effective_start"],
        "localitycounty": ["mac", "locality_id", "state", "effective_start"],
    }

    processed: Dict[str, pd.DataFrame] = {}
    for key, df in dataframes.items():
        if df is None or df.empty:
            processed[key] = df
            continue
        df_copy = df.copy()
        nk = natural_keys.get(key)
        if nk:
            subset = [col for col in nk if col in df_copy.columns]
            if subset:
                before = len(df_copy)
                for col in subset:
                    if df_copy[col].dtype == "O":
                        df_copy[col] = df_copy[col].fillna("")
                df_copy = (
                    df_copy.sort_values(subset)
                    .drop_duplicates(subset=subset, keep="first")
                    .reset_index(drop=True)
                )
                after = len(df_copy)
                if before != after:
                    logger.info(
                        "Dropped duplicate rows before DB load",
                        dataset=key,
                        duplicates_removed=before - after,
                        natural_key=subset,
                    )
        processed[key] = df_copy

    dataset_results: Dict[str, int] = {}
    total_records = 0

    for key, df in processed.items():
        if df is None or df.empty:
            dataset_results[key] = 0
            continue

        loader = RVU_DATASET_LOADERS.get(key)
        if not loader:
            logger.warning("No loader registered for dataset", dataset=key)
            dataset_results[key] = 0
            continue

        try:
            inserted = loader(df, release_uuid, batch_id, db_session)
            total_records += inserted
            dataset_results[key] = inserted
            logger.info(
                "Dataset loaded into database",
                dataset=key,
                records_inserted=inserted,
            )
        except Exception:
            logger.exception("Failed to load dataset", dataset=key)
            dataset_results[key] = 0

    db_session.commit()

    return {
        "total_records": total_records,
        "datasets": dataset_results,
        "release_uuid": str(release_uuid),
    }


def load_pprrvu_data(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    db_session: Any,
) -> int:
    if df is None or df.empty:
        return 0

    alias_pairs = [
        ("hcpcs", "hcpcs_code"),
        ("status", "status_code"),
    ]
    for source_col, target_col in alias_pairs:
        if source_col in df.columns and target_col not in df.columns:
            df[target_col] = df[source_col]

    df = _prepare_base_dataframe(df, release_uuid, batch_id)
    df["hcpcs_code"] = _string_column(df, "hcpcs_code", max_len=5)

    df["modifier_key"] = _string_column(df, "modifier", max_len=10)
    df["modifiers"] = df["modifier_key"].apply(
        lambda value: [value] if (pd.notna(value) and str(value).strip() != "") else None
    )

    df["description"] = _string_column(df, "description")
    df["status_code"] = _string_column(df, "status_code", max_len=2)
    df["na_indicator"] = _string_column(df, "na_indicator", max_len=1)
    df["global_days"] = _string_column(df, "global_days", max_len=3)
    df["bilateral_ind"] = _string_column(df, "bilateral_ind", max_len=1)
    df["multiple_proc_ind"] = _string_column(df, "multiple_proc_ind", max_len=1)
    df["assistant_surg_ind"] = _string_column(df, "assistant_surg_ind", max_len=1)
    df["co_surg_ind"] = _string_column(df, "co_surg_ind", max_len=1)
    df["team_surg_ind"] = _string_column(df, "team_surg_ind", max_len=1)
    df["endoscopic_base"] = _string_column(df, "endoscopic_base", max_len=1)
    df["physician_supervision"] = _string_column(df, "physician_supervision", max_len=2)
    df["diag_imaging_family"] = _string_column(df, "diag_imaging_family", max_len=10)

    df["work_rvu"] = _numeric_column(df, "work_rvu")
    df["pe_rvu_nonfac"] = _numeric_column(df, "pe_rvu_nonfac")
    df["pe_rvu_fac"] = _numeric_column(df, "pe_rvu_fac")
    df["mp_rvu"] = _numeric_column(df, "mp_rvu")
    df["conversion_factor"] = _numeric_column(df, "conversion_factor")
    df["total_nonfac"] = _numeric_column(df, "total_nonfac")
    df["total_fac"] = _numeric_column(df, "total_fac")

    df["effective_start"] = _date_column(df, "effective_start", fallback="vintage_date")
    df["effective_end"] = _date_column(df, "effective_end")

    insert_columns = [
        "id",
        "release_id",
        "hcpcs_code",
        "modifiers",
        "modifier_key",
        "description",
        "status_code",
        "work_rvu",
        "pe_rvu_nonfac",
        "pe_rvu_fac",
        "mp_rvu",
        "na_indicator",
        "global_days",
        "bilateral_ind",
        "multiple_proc_ind",
        "assistant_surg_ind",
        "co_surg_ind",
        "team_surg_ind",
        "endoscopic_base",
        "conversion_factor",
        "physician_supervision",
        "diag_imaging_family",
        "total_nonfac",
        "total_fac",
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    prepared = _replace_null_like(df[insert_columns])
    records = prepared.to_dict("records")
    return _bulk_replace_records(db_session, RVUItem, release_uuid, records, "PPRRVU")


def load_gpci_data(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    db_session: Any,
) -> int:
    if df is None or df.empty:
        return 0

    alias_pairs = [
        ("gpci_work", "work_gpci"),
        ("gpci_pe", "pe_gpci"),
        ("gpci_mp", "mp_gpci"),
    ]
    for source_col, target_col in alias_pairs:
        if source_col in df.columns and target_col not in df.columns:
            df[target_col] = df[source_col]

    logger.info(
        "Loading GPCI data",
        df_columns=list(df.columns),
        row_count=len(df),
        sample_columns_with_gpci=[col for col in df.columns if "gpci" in col.lower()],
    )
    if len(df) > 0:
        first_row = df.iloc[0]
        logger.debug(
            "Sample row columns",
            gpci_work=first_row.get("gpci_work"),
            work_gpci=first_row.get("work_gpci"),
            gpci_pe=first_row.get("gpci_pe"),
            pe_gpci=first_row.get("pe_gpci"),
            gpci_mp=first_row.get("gpci_mp"),
            gpci_malp=first_row.get("gpci_malp"),
            mp_gpci=first_row.get("mp_gpci"),
        )

    df = _prepare_base_dataframe(df, release_uuid, batch_id)

    df["mac"] = _string_column(df, "mac", max_len=10)
    df["state"] = _string_column(df, "state", max_len=2, uppercase=True)

    locality_series = _series_from_df(df, "locality_code")
    locality_series = locality_series.where(
        locality_series.notna(), _series_from_df(df, "locality_id")
    )
    df["locality_id"] = locality_series
    df["locality_id"] = _string_column(df, "locality_id", max_len=10)

    df["locality_name"] = _string_column(df, "locality_name", max_len=100)

    work_series = _series_from_df(df, "gpci_work")
    work_series = work_series.where(
        work_series.notna(), _series_from_df(df, "work_gpci")
    )
    df["work_gpci"] = pd.to_numeric(work_series, errors="coerce").astype(float)

    pe_series = _series_from_df(df, "gpci_pe")
    pe_series = pe_series.where(
        pe_series.notna(), _series_from_df(df, "pe_gpci")
    )
    df["pe_gpci"] = pd.to_numeric(pe_series, errors="coerce").astype(float)

    mp_series = _series_from_df(df, "gpci_mp")
    mp_series = mp_series.where(
        mp_series.notna(), _series_from_df(df, "gpci_malp")
    )
    mp_series = mp_series.where(
        mp_series.notna(), _series_from_df(df, "mp_gpci")
    )
    df["mp_gpci"] = pd.to_numeric(mp_series, errors="coerce").astype(float)

    df["effective_start"] = _date_column(df, "effective_start", fallback="vintage_date")
    df["effective_end"] = _date_column(df, "effective_end")

    insert_columns = [
        "id",
        "release_id",
        "mac",
        "state",
        "locality_id",
        "locality_name",
        "work_gpci",
        "pe_gpci",
        "mp_gpci",
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    prepared = _replace_null_like(df[insert_columns])
    records = prepared.to_dict("records")
    return _bulk_replace_records(db_session, GPCIIndex, release_uuid, records, "GPCI")


def load_oppscap_data(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    db_session: Any,
) -> int:
    if df is None or df.empty:
        return 0

    alias_pairs = [
        ("hcpcs", "hcpcs_code"),
        ("status", "proc_status"),
        ("facility_price", "price_fac"),
        ("nonfacility_price", "price_nonfac"),
    ]
    for source_col, target_col in alias_pairs:
        if source_col in df.columns and target_col not in df.columns:
            df[target_col] = df[source_col]

    df = _prepare_base_dataframe(df, release_uuid, batch_id)

    df["hcpcs_code"] = _string_column(df, "hcpcs_code", max_len=5)
    df["modifier"] = _string_column(df, "modifier", max_len=2)
    df["proc_status"] = _string_column(df, "proc_status", max_len=2)
    df["mac"] = _string_column(df, "mac", max_len=10)

    locality_series = _series_from_df(df, "locality_code")
    locality_series = locality_series.where(
        locality_series.notna(), _series_from_df(df, "locality_id")
    )
    df["locality_id"] = locality_series
    df["locality_id"] = _string_column(df, "locality_id", max_len=10)

    df["price_fac"] = _numeric_column(df, "price_fac")
    df["price_nonfac"] = _numeric_column(df, "price_nonfac")

    df["effective_start"] = _date_column(df, "effective_start", fallback="vintage_date")
    df["effective_end"] = _date_column(df, "effective_end")

    insert_columns = [
        "id",
        "release_id",
        "hcpcs_code",
        "modifier",
        "proc_status",
        "mac",
        "locality_id",
        "price_fac",
        "price_nonfac",
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    prepared = _replace_null_like(df[insert_columns])
    records = prepared.to_dict("records")
    return _bulk_replace_records(db_session, OPPSCap, release_uuid, records, "OPPSCap")


def load_anes_data(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    db_session: Any,
) -> int:
    if df is None or df.empty:
        return 0

    df = _prepare_base_dataframe(df, release_uuid, batch_id)

    df["mac"] = _string_column(df, "mac", max_len=10)

    locality_series = _series_from_df(df, "locality_code")
    locality_series = locality_series.where(
        locality_series.notna(), _series_from_df(df, "locality_id")
    )
    df["locality_id"] = locality_series
    df["locality_id"] = _string_column(df, "locality_id", max_len=10)

    df["locality_name"] = _string_column(df, "locality_name", max_len=100)
    df["anesthesia_cf"] = _numeric_column(df, "anesthesia_cf")

    df["effective_start"] = _date_column(df, "effective_start", fallback="vintage_date")
    df["effective_end"] = _date_column(df, "effective_end")

    insert_columns = [
        "id",
        "release_id",
        "mac",
        "locality_id",
        "locality_name",
        "anesthesia_cf",
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    prepared = _replace_null_like(df[insert_columns])
    records = prepared.to_dict("records")
    return _bulk_replace_records(db_session, AnesCF, release_uuid, records, "ANES")


def load_locality_data(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    db_session: Any,
) -> int:
    if df is None or df.empty:
        return 0

    alias_pairs = [
        ("locality_code", "locality_id"),
        ("state_name", "state"),
        ("fee_area", "fee_schedule_area"),
        ("county_names", "county_name"),
    ]
    for source_col, target_col in alias_pairs:
        if source_col in df.columns and target_col not in df.columns:
            df[target_col] = df[source_col]

    df = _prepare_base_dataframe(df, release_uuid, batch_id)

    df["mac"] = _string_column(df, "mac", max_len=10)

    locality_series = _series_from_df(df, "locality_code")
    locality_series = locality_series.where(
        locality_series.notna(), _series_from_df(df, "locality_id")
    )
    df["locality_id"] = locality_series
    df["locality_id"] = _string_column(df, "locality_id", max_len=10)

    # Convert state_name to 2-character USPS abbreviation (required by DB schema)
    if "state_name" in df.columns and "state" not in df.columns:
        df["state"] = df["state_name"].apply(_state_name_to_abbr)
    elif "state" in df.columns:
        # If state already exists but contains full names, convert them
        df["state"] = df["state"].apply(_state_name_to_abbr)
    
    # Ensure state is exactly 2 characters (enforce DB constraint)
    df["state"] = df["state"].str[:2].str.upper()
    
    df["fee_schedule_area"] = _string_column(df, "fee_schedule_area", max_len=128)
    df["county_name"] = _string_column(df, "county_name", max_len=128)

    df = df[
        df["state"].notna() & (df["state"].str.len() > 0)
    ]
    df = df[
        df["county_name"].notna() & (df["county_name"].str.len() > 0)
    ]

    df["effective_start"] = _date_column(df, "effective_start", fallback="vintage_date")
    df["effective_end"] = _date_column(df, "effective_end")

    insert_columns = [
        "id",
        "release_id",
        "mac",
        "locality_id",
        "state",
        "fee_schedule_area",
        "county_name",
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    prepared = _replace_null_like(df[insert_columns])
    records = prepared.to_dict("records")
    return _bulk_replace_records(db_session, LocalityCounty, release_uuid, records, "Locality")


def _derive_source_version(vintage_date: str) -> str:
    try:
        year = str(pd.to_datetime(vintage_date).year)[-2:]
        return f"{year}D"
    except Exception:
        return "2025D"


def _prepare_base_dataframe(
    df: pd.DataFrame,
    release_uuid: Any,
    batch_id: str,
    source_column: str = "source_filename",
    source_max_len: int = 100,
) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    normalized = df.copy().reset_index(drop=True)
    if not normalized.empty:
        normalized["id"] = [uuid.uuid4() for _ in range(len(normalized))]
    else:
        normalized["id"] = []

    normalized["source_file"] = _string_column(
        normalized,
        source_column,
        max_len=source_max_len,
        default=batch_id,
    )
    normalized["row_num"] = normalized.index.astype(int)
    normalized["release_id"] = release_uuid
    return normalized


def _string_column(
    df: pd.DataFrame,
    column: str,
    max_len: Optional[int] = None,
    default: Any = None,
    uppercase: bool = False,
) -> pd.Series:
    series = _series_from_df(df, column, default)
    cleaned = series.astype("string").str.strip()
    if uppercase:
        cleaned = cleaned.str.upper()
    if max_len is not None:
        cleaned = cleaned.str.slice(0, max_len)
    return cleaned.where(cleaned.notna() & (cleaned != ""), None)


def _numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    series = _series_from_df(df, column)
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype(float)


def _series_from_df(
    df: pd.DataFrame,
    column: str,
    default: Any = None,
) -> pd.Series:
    if column in df.columns:
        return df[column]
    if callable(default):
        return default()
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _date_column(
    df: pd.DataFrame,
    column: str,
    fallback: Optional[str] = None,
) -> pd.Series:
    series = _series_from_df(df, column)
    if fallback:
        fallback_series = _series_from_df(df, fallback)
        series = series.where(series.notna(), fallback_series)
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.date


def _replace_null_like(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({pd.NA: None, np.nan: None, pd.NaT: None})


def _bulk_replace_records(
    db_session: Any,
    model,
    release_uuid: Any,
    records: List[Dict[str, Any]],
    dataset_label: str,
) -> int:
    if not records:
        return 0

    try:
        with db_session.begin_nested():
            db_session.execute(delete(model).where(model.release_id == release_uuid))
            _bulk_insert_chunked(db_session, model, records)
    except Exception:  # pragma: no cover - logging only
        logger.exception("%s bulk load failed", dataset_label, release_id=str(release_uuid))
        raise

    return len(records)


def _bulk_insert_chunked(db_session: Any, model, records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    for start in range(0, len(records), BULK_INSERT_CHUNK_SIZE):
        chunk = records[start : start + BULK_INSERT_CHUNK_SIZE]
        db_session.bulk_insert_mappings(model, chunk)


RVU_DATASET_LOADERS = {
    "pprrvu": load_pprrvu_data,
    "gpci": load_gpci_data,
    "oppscap": load_oppscap_data,
    "anescf": load_anes_data,
    "localitycounty": load_locality_data,
}
