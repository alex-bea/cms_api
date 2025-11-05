"""
Utility script for Phase 2 Step 2.3.

Creates an RVUIngestor instance inside a temporary output directory and reports
on the availability of legacy helper methods plus automatically wired services.

Run with:
    docker compose exec api python tests/ingestors/scripts/verify_rvu_ingestor_instance.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor


def collect_method_flags(ingestor: RVUIngestor) -> Dict[str, bool]:
    """Return a mapping of stage helper names to availability flags."""
    stage_helpers = [
        "_land_stage",
        "_validate_stage",
        "_normalize_stage",
        "_enrich_stage",
        "_publish_stage",
        "land",
        "validate",
        "normalize",
        "enrich",
        "publish",
    ]
    return {name: hasattr(ingestor, name) for name in stage_helpers}


def summarize(ingestor: RVUIngestor, output_root: Path) -> Dict[str, Any]:
    """Build a JSON-serializable summary of the ingestor instance."""
    method_flags = collect_method_flags(ingestor)

    summary: Dict[str, Any] = {
        "output_dir": str(output_root),
        "method_flags": method_flags,
        "scraper_type": type(getattr(ingestor, "scraper", None)).__name__,
        "historical_manager_type": type(getattr(ingestor, "historical_manager", None)).__name__,
    }
    return summary


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_root = Path(tmpdir)
        ingestor = RVUIngestor(str(output_root))
        summary = summarize(ingestor, output_root)
        print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
