"""
RVU Adapter Module
------------------

Phase 2 Refactoring Context:
    - Step 3: Adapter extraction
      • Plan: artifacts/phase2_step3_detailed_plan.md
      • Verification: artifacts/phase2_regression_test_results.md

Parses raw RVU archives into AdaptedBatch objects using DatasetSpec routing.
This is an extraction of the `_adapt_raw_data_sync` logic from `RVUIngestor`
so stage modules can reuse the adapter outside the ingestor class.
"""

from __future__ import annotations

import io
import os
import uuid
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog
from datetime import datetime

from ..contracts.ingestor_spec import RawBatch, AdaptedBatch, SourceFile
from ..metadata.vintage_extractor import extract_vintage_metadata
from .rvu_spec import RVU_DATASETS, DatasetSpec

logger = structlog.get_logger()

DEFAULT_CATCHALL_PATTERN = r".*(rvu|pprrvu|gpci|opps|anes|locality|locco).*\.(txt|csv|xlsx|xls)$"


# Phase 2 Step 3: Adapter extraction
# See: artifacts/phase2_step3_detailed_plan.md
def adapt_rvu_raw_data(
    raw_batch: RawBatch,
    *,
    dataset_specs: Optional[Dict[str, DatasetSpec]] = None,
    schema_registry: Optional[Any] = None,
    release_id_override: Optional[str] = None,
    batch_id_override: Optional[str] = None,
    natural_keys_override: Optional[Dict[str, List[str]]] = None,
    catchall_pattern: str = DEFAULT_CATCHALL_PATTERN,
    observability_collector: Optional[Any] = None,
    output_dir: Optional[str] = None,
    dataset_name: str = "cms_rvu",
    derive_release_context: Optional[Any] = None,
) -> AdaptedBatch:
    """
    Parse raw RVU archives into canonical DataFrames using DatasetSpec routing.
    """
    dataset_specs = dataset_specs or RVU_DATASETS
    natural_keys_map = natural_keys_override or {
        dataset_id: spec.natural_keys for dataset_id, spec in dataset_specs.items()
    }

    metadata = raw_batch.metadata or {}
    release_id = release_id_override or metadata.get("release_id", "unknown")
    batch_id = batch_id_override or metadata.get("batch_id") or getattr(raw_batch, "batch_id", None) or "unknown"

    logger.info("Adapting raw RVU data", release_id=release_id)

    source_lookup = {
        sf.filename: sf for sf in (raw_batch.source_files or [])
    }

    raw_content = raw_batch.raw_content or {}
    if isinstance(raw_content, (bytes, bytearray)):
        raw_content = {"rvu_payload.zip": raw_content}

    dataset_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
    schema_contracts: Dict[str, Any] = {}
    parser_metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    rejects_summary: Dict[str, int] = defaultdict(int)
    unexpected_files_by_archive: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    aggregate_warnings = os.getenv("AGGREGATE_ZIP_WARNINGS", "true").lower() == "true"

    for filename, content in raw_content.items():
        if content is None:
            continue
        if not isinstance(content, (bytes, bytearray)):
            logger.debug("Skipping non-bytes content", filename=filename)
            continue

        content_bytes = bytes(content)
        buffer = io.BytesIO(content_bytes)
        buffer.seek(0)

        if zipfile.is_zipfile(buffer):
            with zipfile.ZipFile(buffer) as zf:
                members = [name for name in zf.namelist() if not name.endswith("/")]
                dataset_suffixes: Dict[str, set] = defaultdict(set)
                unclassified_members: List[str] = []

                for member in members:
                    suffix = Path(member).suffix.lower()
                    if suffix == ".pdf":
                        continue
                    spec = _route_with_specs(member, dataset_specs)
                    if not spec:
                        unclassified_members.append(member)
                        continue
                    dataset_suffixes[spec.dataset_id].add(suffix)

                recognized: List[Tuple[str, DatasetSpec]] = []
                for member in members:
                    suffix = Path(member).suffix.lower()
                    if suffix == ".pdf":
                        continue
                    spec = _route_with_specs(member, dataset_specs)
                    if not spec:
                        continue
                    dataset_key = spec.dataset_id
                    if (
                        dataset_key == "anescf"
                        and suffix in {".csv", ".xlsx", ".xls"}
                        and ".txt" in dataset_suffixes.get(dataset_key, set())
                    ):
                        logger.info(
                            "Skipping ANES CSV/XLSX variant (TXT available in archive)",
                            archive=filename,
                            filename=member,
                        )
                        continue
                    recognized.append((member, spec))

                for raw_member in unclassified_members:
                    if _matches_catchall(raw_member, catchall_pattern):
                        logger.warning(
                            "Possible new dataset type detected",
                            archive=filename,
                            filename=raw_member,
                            note="File matches RVU naming patterns but wasn't classified. May indicate new dataset type or classification gap.",
                        )
                        unexpected_files_by_archive[filename].append(
                            {
                                "filename": raw_member,
                                "dataset": None,
                                "reason": "Unclassified but matches RVU catch-all pattern",
                            }
                        )
                        if observability_collector:
                            try:
                                observability_collector.record_metric(
                                    "inner_file_unexpected_count",
                                    1,
                                    tags={"archive": filename, "release_id": release_id},
                                )
                            except Exception as metric_err:
                                logger.debug("Failed to record unexpected files metric", error=str(metric_err))
                    else:
                        logger.warning(
                            "Skipping unclassified inner file (may be CSV/XLSX variant)",
                            filename=raw_member,
                            archive=filename,
                            suggestion="Verify file format matches expected patterns. If this is a valid data file, update DatasetSpec.route_file() to recognize it.",
                        )

                for inner_name, spec in recognized:
                    inner_bytes = zf.read(inner_name)
                    metadata_payload = _build_parser_metadata(
                        dataset_key=spec.dataset_id,
                        spec=spec,
                        release_id=release_id,
                        source_file=source_lookup.get(filename),
                        inner_filename=Path(inner_name).name,
                        file_bytes=inner_bytes,
                        batch_id=batch_id,
                        derive_release_context=derive_release_context,
                    )

                    try:
                        logger.info(
                            "invoking_parser",
                            dataset=spec.dataset_id,
                            filename=inner_name,
                            size_bytes=len(inner_bytes),
                            parser_func=getattr(spec.parser, "__name__", str(spec.parser)),
                        )

                        result = _invoke_parser(spec, metadata_payload, Path(inner_name).name, inner_bytes)

                        logger.info(
                            "parser_result",
                            dataset=spec.dataset_id,
                            filename=inner_name,
                            rows_parsed=len(result.data),
                            rows_rejected=len(result.rejects),
                            metrics=result.metrics,
                            has_real_data=not result.data.empty,
                        )

                    except Exception as parse_error:
                        logger.error(
                            "parser_failure",
                            dataset=spec.dataset_id,
                            filename=inner_name,
                            error=str(parse_error),
                            error_type=type(parse_error).__name__,
                        )
                        continue

                    _accumulate_parser_outputs(
                        dataset_key=spec.dataset_id,
                        result=result,
                        dataset_frames=dataset_frames,
                        rejects_summary=rejects_summary,
                        parser_metrics=parser_metrics,
                        release_id=release_id,
                        output_dir=output_dir,
                        dataset_name=dataset_name,
                        archive_filename=filename,
                    )
        else:
            suffix = Path(filename).suffix.lower()
            if suffix == ".pdf":
                logger.info("Skipping guidance PDF", filename=filename)
                continue
            spec = _route_with_specs(filename, dataset_specs)
            if not spec:
                logger.warning(
                    "Skipping unclassified file (may be CSV/XLSX variant)",
                    filename=filename,
                    suggestion="Verify file format matches expected patterns. If this is a valid data file, update DatasetSpec.route_file() to recognize it.",
                )
                continue

            metadata_payload = _build_parser_metadata(
                dataset_key=spec.dataset_id,
                spec=spec,
                release_id=release_id,
                source_file=source_lookup.get(filename),
                inner_filename=Path(filename).name,
                file_bytes=content_bytes,
                batch_id=batch_id,
                derive_release_context=derive_release_context,
            )

            try:
                logger.info(
                    "invoking_parser",
                    dataset=spec.dataset_id,
                    filename=filename,
                    size_bytes=len(content_bytes),
                    parser_func=getattr(spec.parser, "__name__", str(spec.parser)),
                )

                result = _invoke_parser(spec, metadata_payload, Path(filename).name, content_bytes)

                logger.info(
                    "parser_result",
                    dataset=spec.dataset_id,
                    filename=filename,
                    rows_parsed=len(result.data),
                    rows_rejected=len(result.rejects),
                    metrics=result.metrics,
                    has_real_data=not result.data.empty,
                )

            except Exception as parse_error:
                logger.error(
                    "parser_failure",
                    dataset=spec.dataset_id,
                    filename=filename,
                    error=str(parse_error),
                    error_type=type(parse_error).__name__,
                )
                continue

            _accumulate_parser_outputs(
                dataset_key=spec.dataset_id,
                result=result,
                dataset_frames=dataset_frames,
                rejects_summary=rejects_summary,
                parser_metrics=parser_metrics,
                release_id=release_id,
                output_dir=output_dir,
                dataset_name=dataset_name,
                archive_filename=filename,
            )

    # Row limiting for fast testing/debugging (per QA Testing Standard §3.3)
    # Set MAX_INGESTION_ROWS env var to limit rows (e.g., "1000" for fast testing)
    max_rows = os.getenv("MAX_INGESTION_ROWS")
    max_rows_int = int(max_rows) if max_rows and max_rows.isdigit() else None
    
    final_dataframes: Dict[str, pd.DataFrame] = {}
    for dataset_key, frames in dataset_frames.items():
        if not frames:
            continue

        combined = (
            pd.concat(frames, ignore_index=True)
            if len(frames) > 1
            else frames[0].copy()
        )
        
        # Apply row limiting if enabled (for fast testing/debugging)
        if max_rows_int and len(combined) > max_rows_int:
            original_count = len(combined)
            combined = combined.head(max_rows_int)
            logger.info(
                "Row limiting applied for testing",
                dataset=dataset_key,
                original_rows=original_count,
                limited_rows=len(combined),
                max_rows=max_rows_int,
                note="Set MAX_INGESTION_ROWS env var to enable fast testing mode"
            )

        natural_keys = natural_keys_map.get(dataset_key, [])
        if natural_keys:
            missing_keys = [col for col in natural_keys if col not in combined.columns]
            if missing_keys:
                logger.debug(
                    "Natural key columns missing on combined dataframe",
                    dataset=dataset_key,
                    missing_columns=missing_keys,
                )
            else:
                before = len(combined)
                combined = combined.drop_duplicates(subset=natural_keys, keep="first")
                dropped = before - len(combined)
                if dropped > 0:
                    logger.warning(
                        "Duplicate natural keys trimmed post-adaptation",
                        dataset=dataset_key,
                        duplicates_removed=dropped,
                        natural_keys=natural_keys,
                    )

        combined = combined.reset_index(drop=True)
        final_dataframes[dataset_key] = combined

        spec = dataset_specs.get(dataset_key)
        if schema_registry and spec:
            contract = schema_registry.get_contract(spec.schema_id)
            if contract:
                schema_contracts[dataset_key] = contract

    metadata_out = dict(raw_batch.metadata or {})
    metadata_out.setdefault("release_id", release_id)
    metadata_out["parser_metrics"] = {k: v for k, v in parser_metrics.items()}
    metadata_out["parser_rejects"] = dict(rejects_summary)

    total_rows = sum(len(df) for df in final_dataframes.values())
    logger.info(
        "adapter_completed",
        datasets=list(final_dataframes.keys()),
        total_rows=total_rows,
        release_id=release_id,
        rejects_summary=dict(rejects_summary),
        parser_files_processed=len(raw_content),
    )

    if aggregate_warnings and unexpected_files_by_archive:
        for archive, entries in unexpected_files_by_archive.items():
            logger.warning(
                "rvu.ingestor.zip_unexpected_files_summary",
                archive=archive,
                unexpected_count=len(entries),
                unexpected_files=entries[:10],
            )

    for dataset_key, df in final_dataframes.items():
        spec = dataset_specs.get(dataset_key)
        logger.info(
            "dataset_parsed",
            dataset=dataset_key,
            rows=len(df),
            columns=list(df.columns)[:10],
            schema_name=spec.schema_id if spec else "unknown",
            natural_keys=str(natural_keys_map.get(dataset_key, [])),
            data_types=df.dtypes.astype(str).to_dict(),
        )

    return AdaptedBatch(
        dataframes=final_dataframes,
        schema_contract=schema_contracts,
        metadata=metadata_out,
    )


def _route_with_specs(filename: str, dataset_specs: Dict[str, DatasetSpec]) -> Optional[DatasetSpec]:
    for spec in dataset_specs.values():
        if spec.route_file(filename):
            return spec
    return None


def _matches_catchall(filename: str, pattern: str) -> bool:
    import re
    return bool(re.match(pattern, filename.lower()))


def _build_parser_metadata(
    dataset_key: str,
    spec: DatasetSpec,
    release_id: str,
    source_file: Optional[SourceFile],
    inner_filename: str,
    file_bytes: bytes,
    batch_id: Optional[str],
    derive_release_context: Optional[Any],
) -> Dict[str, Any]:
    context = None
    if callable(derive_release_context):
        try:
            context = derive_release_context(inner_filename, release_id)
        except Exception as err:
            logger.debug(
                "derive_release_context_failed",
                dataset=dataset_key,
                filename=inner_filename,
                error=str(err),
            )
    context = dict(context) if context else {}

    year_guess = (
        Path(inner_filename).stem[:4]
        if inner_filename[:4].isdigit()
        else str(datetime.utcnow().year)
    )

    def _infer_from_filename() -> Dict[str, Any]:
        try:
            return extract_vintage_metadata(
                filename=inner_filename,
                release_id=release_id
            )
        except Exception as err:
            logger.debug(
                "extract_vintage_metadata_failed",
                dataset=dataset_key,
                filename=inner_filename,
                error=str(err),
            )
            return {
                "product_year": str(year_guess),
                "quarter_vintage": "",
                "vintage_date": datetime.utcnow(),
                "revision": None,
            }

    vintage_context = _infer_from_filename()

    context.setdefault("product_year", vintage_context.get("product_year", str(year_guess)))
    context.setdefault("quarter_vintage", vintage_context.get("quarter_vintage", ""))
    context.setdefault("vintage_date", vintage_context.get("vintage_date", datetime.utcnow()))

    if context["quarter_vintage"] and "_annual" in context["quarter_vintage"]:
        fallback_context = _infer_from_filename()
        context["quarter_vintage"] = fallback_context.get("quarter_vintage", "2025Q4")
        context["product_year"] = fallback_context.get("product_year", context["product_year"])
        context["vintage_date"] = fallback_context.get("vintage_date", context.get("vintage_date", datetime.utcnow()))
        context["release_letter"] = fallback_context.get("revision") or "D"
    else:
        context.setdefault("release_letter", vintage_context.get("revision") or "D")

    context.setdefault("source_release", f"RVU{context['product_year']}{context.get('release_letter', '')}")

    metadata = {
        "release_id": release_id,
        "product_year": context.get("product_year"),
        "quarter_vintage": context.get("quarter_vintage"),
        "vintage_date": context.get("vintage_date"),
        "file_sha256": _sha256(file_bytes),
        "source_uri": source_file.url if source_file else "",
        "schema_id": spec.schema_id,
        "source_release": context.get("source_release"),
    }
    if batch_id:
        metadata["batch_id"] = batch_id
    product_year = context.get("product_year", "")
    release_letter = context.get("release_letter", "D")
    metadata["layout_version"] = f"v{product_year}{release_letter}.0"
    return metadata


def _invoke_parser(
    spec: DatasetSpec,
    metadata: Dict[str, Any],
    filename: str,
    file_bytes: bytes,
):
    file_obj = io.BytesIO(file_bytes)
    return spec.parser(file_obj, filename, metadata)


def _accumulate_parser_outputs(
    *,
    dataset_key: str,
    result: Any,
    dataset_frames: Dict[str, List[pd.DataFrame]],
    rejects_summary: Dict[str, int],
    parser_metrics: Dict[str, List[Dict[str, Any]]],
    release_id: str,
    output_dir: Optional[str],
    dataset_name: str,
    archive_filename: str,
) -> None:
    if not result.data.columns.is_unique:
        logger.warning(
            "Duplicate column names detected after parsing; dropping later duplicates",
            dataset=dataset_key,
            filename=archive_filename,
            duplicate_columns=[
                col for col in result.data.columns[result.data.columns.duplicated()]
            ],
        )
        deduped_df = result.data.loc[:, ~result.data.columns.duplicated()].copy()
        result = result._replace(data=deduped_df)
        if not result.rejects.empty:
            result = result._replace(
                rejects=result.rejects.loc[:, ~result.rejects.columns.duplicated()].copy()
            )

    if not result.data.empty:
        dataset_frames[dataset_key].append(result.data)
        logger.info(
            "dataframe_added",
            dataset=dataset_key,
            rows=len(result.data),
            columns=list(result.data.columns),
            first_row_preview=result.data.iloc[0].to_dict() if len(result.data) > 0 else {},
        )
    if not result.rejects.empty:
        rejects_summary[dataset_key] += len(result.rejects)
        logger.warning(
            "parser_rejects_detected",
            dataset=dataset_key,
            filename=archive_filename,
            rejects=len(result.rejects),
            sample_reject=str(result.rejects.iloc[0].to_dict()) if len(result.rejects) > 0 else None,
        )
        if output_dir:
            try:
                reject_dir = Path(output_dir) / "stage" / dataset_name / release_id / "reject"
                reject_dir.mkdir(parents=True, exist_ok=True)
                rejects_df = result.rejects.copy()
                rejects_df["_source_filename"] = Path(archive_filename).name
                rejects_df["_dataset"] = dataset_key
                rejects_df["_release_id"] = release_id
                reject_file = reject_dir / f"{dataset_key}_rejects_{uuid.uuid4().hex[:8]}.parquet"
                rejects_df.to_parquet(reject_file)
                logger.info(
                    "rejects_persisted",
                    dataset=dataset_key,
                    filename=str(reject_file),
                    rows=len(rejects_df),
                )
            except Exception as persist_err:
                logger.error(
                    "rejects_persist_failed",
                    dataset=dataset_key,
                    filename=archive_filename,
                    error=str(persist_err),
                )
    parser_metrics[dataset_key].append(result.metrics)


def _sha256(file_bytes: bytes) -> str:
    import hashlib

    return hashlib.sha256(file_bytes).hexdigest()
