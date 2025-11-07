import pytest

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor


def test_dataset_release_id_mapping_basic():
    base = "rvu_2025_B"

    # Identity for RVU items
    assert RVUIngestor._dataset_release_id("rvu_items", base) == "rvu_2025_B"

    # GPCI mapping
    assert RVUIngestor._dataset_release_id("gpci_indices", base) == "gpci_2025_B"

    # Other dataset mappings
    assert RVUIngestor._dataset_release_id("anescf", base) == "anescf_2025_B"
    assert RVUIngestor._dataset_release_id("localitycounty", base) == "locality_2025_B"
    assert RVUIngestor._dataset_release_id("oppscap", base) == "oppscap_2025_B"


def test_dataset_release_id_mapping_unexpected_format():
    # Unexpected base format should pass through unchanged
    base = "2025B"
    assert RVUIngestor._dataset_release_id("gpci_indices", base) == base

