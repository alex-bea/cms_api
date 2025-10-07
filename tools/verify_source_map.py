#!/usr/bin/env python3
"""Verify discovery manifests are reflected in reference documentation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List

from cms_pricing.ingestion.metadata.discovery_manifest import DiscoveryManifestStore


_DATASET_CONFIG = {
    "cms_rvu": {
        "manifest_dir": Path("data/manifests/cms_rvu"),
        "prefix": "cms_rvu_manifest",
        "ref_doc": Path("prds/REF-cms-pricing-source-map-prd-v1.0.md"),
    },
    "cms_mpfs": {
        "manifest_dir": Path("data/scraped/mpfs/manifests"),
        "prefix": "cms_mpfs_manifest",
        "ref_doc": Path("prds/REF-cms-pricing-source-map-prd-v1.0.md"),
    },
    "cms_opps": {
        "manifest_dir": Path("data/scraped/opps/manifests"),
        "prefix": "cms_opps_manifest",
        "ref_doc": Path("prds/REF-cms-pricing-source-map-prd-v1.0.md"),
    },
}


def _load_doc_text(ref_doc: Path) -> str:
    if not ref_doc.exists():
        return ""
    return ref_doc.read_text(encoding="utf-8")


def _latest_manifest(store: DiscoveryManifestStore):
    try:
        return store.load_latest()
    except Exception:
        return None


def verify_dataset(name: str, config: Dict[str, Path]) -> List[str]:
    errors: List[str] = []

    manifest_dir: Path = config["manifest_dir"]
    prefix: str = config["prefix"]
    ref_doc: Path = config["ref_doc"]

    store = DiscoveryManifestStore(manifest_dir, prefix)
    manifest = _latest_manifest(store)

    if manifest is None:
        errors.append(f"[{name}] No manifest found in {manifest_dir}. Run discovery first.")
        return errors

    doc_text = _load_doc_text(ref_doc)
    if not doc_text:
        errors.append(f"[{name}] Reference document {ref_doc} not found or empty.")
        return errors

    # Ensure the primary source URL is referenced in the documentation.
    if manifest.source_url not in doc_text:
        errors.append(
            f"[{name}] Source URL {manifest.source_url} not mentioned in {ref_doc}."
        )

    for entry in manifest.files:
        if entry.url and entry.url in doc_text:
            continue
        if entry.filename and entry.filename in doc_text:
            continue

        errors.append(
            f"[{name}] File '{entry.filename}' ({entry.url}) missing from {ref_doc}."
        )

    return errors


def main(args: Iterable[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Verify that discovery manifests are reflected in REF source maps."
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=sorted(_DATASET_CONFIG.keys()),
        help="Datasets to verify (defaults to all).",
    )

    parsed = parser.parse_args(list(args))
    datasets = parsed.dataset or sorted(_DATASET_CONFIG.keys())

    issues: List[str] = []
    for dataset in datasets:
        config = _DATASET_CONFIG[dataset]
        issues.extend(verify_dataset(dataset, config))

    if issues:
        for issue in issues:
            print(f"ERROR: {issue}")
        return 1

    print("All dataset manifests align with reference documentation.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
