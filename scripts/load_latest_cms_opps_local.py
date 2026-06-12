#!/usr/bin/env python3
"""Load the pinned public CMS OPPS release into a local/dev database."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cms_pricing.database import Base
from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSBatchInfo, OPPSIngestor
from cms_pricing.ingestion.scrapers.cms_opps_scraper import ScrapedFileInfo
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.models.opps import OPPSAPCPayment, OPPSHCPCSCrosswalk, RefSILookup


RELEASE_ID = "opps_2026q2_r1"
SOURCE_PAGE = (
    "https://www.cms.gov/medicare/payment/prospective-payment-systems/"
    "hospital-outpatient-pps/quarterly-addenda-updates"
)
PINNED_SOURCES = {
    "cy-2026-april-opps-addendum.zip": {
        "url": "https://www.cms.gov/files/zip/cy-2026-april-opps-addendum.zip",
        "sha256": "32bee7d5e146074c2c5ee4586eaf1fc1b5bda56ccc7fa1737594179cb815f04e",
    },
    "cy-2026-april-opps-addendum-b.zip": {
        "url": "https://www.cms.gov/files/zip/cy-2026-april-opps-addendum-b.zip",
        "sha256": "127644907d9cc4026d7b8349400c7c3787290bffc28e40fecc96c32935b0b181",
    },
}


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_file_info(source_dir: Path) -> list[ScrapedFileInfo]:
    files = []
    for filename, metadata in PINNED_SOURCES.items():
        path = source_dir / filename
        if not path.exists():
            raise FileNotFoundError(
                f"Missing {path}. Download from {metadata['url']} before running the local OPPS load."
            )
        digest = _sha256_file(path)
        if digest != metadata["sha256"]:
            raise ValueError(f"{path} SHA-256 mismatch: expected {metadata['sha256']}, got {digest}")
        files.append(
            ScrapedFileInfo(
                url=metadata["url"],
                filename=filename,
                file_type="addendum_zip",
                batch_id=RELEASE_ID,
                discovered_at=datetime.utcnow(),
                source_page=SOURCE_PAGE,
                metadata={"year": 2026, "quarter": 2, "sha256": digest},
                local_path=path,
                checksum=digest,
            )
        )
    return files


def build_batch_info(source_dir: Path) -> OPPSBatchInfo:
    ingestor = OPPSIngestor(output_dir=source_dir.parent / "opps_loader_tmp")
    files = _source_file_info(source_dir)
    return OPPSBatchInfo(
        batch_id=RELEASE_ID,
        year=2026,
        quarter=2,
        release_number=1,
        effective_from=ingestor._calculate_effective_from(2026, 2),
        effective_to=ingestor._calculate_effective_to(2026, 2),
        files=files,
        discovered_at=datetime.utcnow(),
        downloaded_at=datetime.utcnow(),
    )


def create_loader_tables(engine) -> None:
    """Create only the tables required for the local OPPS loader."""
    opps_tables = [
        OPPSAPCPayment.__table__,
        OPPSHCPCSCrosswalk.__table__,
        RefSILookup.__table__,
    ]
    if engine.dialect.name != "sqlite":
        Base.metadata.create_all(
            bind=engine,
            tables=[*opps_tables, DatasetSnapshot.__table__],
            checkfirst=True,
        )
        return

    Base.metadata.create_all(bind=engine, tables=opps_tables, checkfirst=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dataset_snapshots (
                    dataset_id VARCHAR(50) NOT NULL,
                    release_id VARCHAR(50) NOT NULL,
                    digest VARCHAR(64) NOT NULL,
                    effective_from DATE NOT NULL,
                    effective_to DATE,
                    manifest_url VARCHAR(500),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    PRIMARY KEY (dataset_id, release_id)
                )
                """
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_dataset_snapshots_dataset_effective "
                "ON dataset_snapshots (dataset_id, effective_from, effective_to)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_dataset_snapshots_digest "
                "ON dataset_snapshots (digest)"
            )
        )


async def parse_pinned_release(source_dir: Path, output_dir: Path) -> tuple[OPPSIngestor, OPPSBatchInfo, dict[str, Any]]:
    ingestor = OPPSIngestor(output_dir=output_dir)
    batch_info = build_batch_info(source_dir)
    normalized: dict[str, Any] = {}
    for file_info in batch_info.files:
        normalized.update(await ingestor._parse_zip_file(file_info))
    for df in normalized.values():
        df["year"] = batch_info.year
        df["quarter"] = batch_info.quarter
        df["effective_from"] = batch_info.effective_from
        df["effective_to"] = batch_info.effective_to
        df["release_id"] = batch_info.batch_id
        df["batch_id"] = batch_info.batch_id
    return ingestor, batch_info, normalized


async def run_load(
    *,
    source_dir: Path,
    output_dir: Path,
    database_url: str | None,
    report_json: Path | None,
    parse_only: bool,
) -> dict[str, Any]:
    ingestor, batch_info, normalized = await parse_pinned_release(source_dir, output_dir)
    validation = ingestor.validate_normalized_opps_data(
        normalized,
        min_apc_rows=1000,
        min_hcpcs_rows=19000,
    )
    if not validation["passed"]:
        raise ValueError(f"OPPS validation failed: {validation['errors']}")

    report: dict[str, Any] = {
        "release_id": batch_info.batch_id,
        "year": batch_info.year,
        "quarter": batch_info.quarter,
        "effective_from": batch_info.effective_from.isoformat(),
        "effective_to": batch_info.effective_to.isoformat() if batch_info.effective_to else None,
        "source_files": [
            {
                "filename": file_info.filename,
                "url": file_info.url,
                "sha256": file_info.checksum,
            }
            for file_info in batch_info.files
        ],
        "validation": validation,
        "loaded": None,
        "snapshots": [],
    }

    if not parse_only:
        if not database_url:
            raise ValueError("--database-url is required unless --parse-only is set")
        engine = create_engine(database_url)
        create_loader_tables(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            report["loaded"] = ingestor.persist_normalized_opps_data(
                session,
                normalized,
                batch_info,
                replace_existing=True,
            )
            source_digest = ":".join(file_info.checksum or "" for file_info in batch_info.files)
            report["snapshots"] = ingestor.register_opps_snapshots(
                session,
                batch_info,
                manifest_url=str(report_json) if report_json else None,
                source_digest=source_digest,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    if report_json:
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Load pinned CMS OPPS Addendum A/B into local/dev DB.")
    parser.add_argument("--source-dir", type=Path, default=Path("data/ingestion/opps"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/ingestion/local/opps"))
    parser.add_argument("--database-url")
    parser.add_argument("--report-json", type=Path, default=Path("data/ingestion/local/reports/cms_opps_local_load_latest.json"))
    parser.add_argument("--parse-only", action="store_true")
    args = parser.parse_args()

    report = asyncio.run(
        run_load(
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            database_url=args.database_url,
            report_json=args.report_json,
            parse_only=args.parse_only,
        )
    )
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
