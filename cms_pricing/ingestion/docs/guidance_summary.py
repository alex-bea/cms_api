"""
Helpers for producing RVU guidance summaries (JSON + Markdown) that describe
the supporting CMS documentation, payment formulas, and key policy notes.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Version tracking for guidance extraction and summary generation
GUIDANCE_EXTRACTION_TOOL_VERSION = "1.0.0"  # Will be updated when PDF extraction tool is implemented
SUMMARY_GENERATOR_VERSION = "1.0.0"

PAYMENT_FORMULA = {
    "equation": "Payment = CF × [(Work RVU × Work GPCI) + (PE RVU × PE GPCI) + (MP RVU × MP GPCI)]",
    "variables": {
        "CF": "Conversion factor published by CMS each release",
        "Work RVU": "Relative value for physician work",
        "PE RVU": "Practice expense relative value (facility vs non-facility)",
        "MP RVU": "Malpractice relative value",
        "Work GPCI": "Geographic Practice Cost Index for physician work",
        "PE GPCI": "Geographic Practice Cost Index for practice expense",
        "MP GPCI": "Geographic Practice Cost Index for malpractice"
    }
}

STATUS_INDICATOR_DESCRIPTIONS: Dict[str, str] = {
    "A": "Active code — paid under the physician fee schedule if covered.",
    "B": "Bundled code — payment is always bundled into payment for other services.",
    "C": "Inpatient-Only procedure — not paid under the physician fee schedule.",
    "D": "Deleted code effective for the current or prior fee schedule year.",
    "E": "Excluded from physician fee schedule (paid under another methodology).",
    "F": "Certain preventive services — not subject to deductible or coinsurance.",
    "G": "Professional component only.",
    "H": "Deleted modifier (carrier priced).",
    "I": "Not valid for Medicare purposes; MACs may still price but national payment not provided.",
    "J1": "Hospital OPPS packaged service paid through comprehensive APC (rare in PFS).",
    "N": "Non-covered service.",
    "P": "Bundled/partial physician services (pre- and post-operative).",
    "R": "Restricted coverage procedure (e.g., only when certain criteria met).",
    "S": "Procedure or service separately paid under OPPS.",
    "T": "Procedure subject to multiple procedure payment reduction.",
    "U": "Urology diagnostic service (rare).",
    "V": "Routine ophthalmology service bundled.",
    "W": "Procedures paid under OPPS when provided in hospital outpatient departments.",
    "X": "Ancillary service, priced only when billed alone.",
    "Y": "Supply — under Physician Fee Schedule but incorporated elsewhere.",
    "Z": "Reference laboratory code or theater service."
}

GLOBAL_PERIOD_DESCRIPTIONS: Dict[str, str] = {
    "000": "0-day global period — includes services provided on the day of the procedure only.",
    "010": "10-day global period — includes the procedure day and 10 days of post-op care.",
    "090": "90-day global period — major surgery global (1 pre-op day + 90 post-op days).",
    "MMM": "Clinical laboratory test (global concept does not apply).",
    "XXX": "Global period does not apply — typically diagnostic or evaluation services.",
    "YYY": "Global period determined by the Medicare Administrative Contractor.",
    "ZZZ": "Add-on service; global period determined by primary procedure."
}

POLICY_NOTES: List[str] = [
    "CPT® codes and descriptions are © American Medical Association; dental codes (D-codes) are © American Dental Association.",
    "Payment calculations should apply the CMS-published conversion factor first; sequestration or budget neutrality adjustments are layered afterwards.",
    "Bundled, non-covered, or carrier-priced services require review of status indicators prior to pricing.",
    "Refer to CMS Physician Fee Schedule look-up tool for beneficiary-facing payment amounts and coverage policies."
]

SUPPORT_CONTACTS: Dict[str, str] = {
    "pfs_lookup_tool": "https://www.cms.gov/medicare/physician-fee-schedule/search",
    "contractor_support": "https://www.cms.gov/medicare/medicare-contracting/medicare-administrative-contractors-macs",
    "helpdesk_email": "physicianfeesched@cms.hhs.gov"
}


def _build_markdown(summary: Dict[str, Any]) -> str:
    """Render a lightweight Markdown view of the summary for human readers."""
    release = summary.get("release", {})
    datasets = summary.get("datasets", [])
    guidance_docs = summary.get("guidance_documents", [])
    status_section = summary.get("status_indicators", {})
    global_section = summary.get("global_periods", {})

    lines: List[str] = []
    lines.append(f"# RVU Guidance Summary – {release.get('release_id', 'Unknown Release')}")
    lines.append("")
    lines.append("## Overview")
    lines.append(f"- Release ID: `{release.get('release_id', 'unknown')}`")
    if release.get("product_year"):
        lines.append(f"- Product Year: {release['product_year']}")
    if release.get("release_letter"):
        lines.append(f"- Release Letter: {release['release_letter']}")
    if release.get("posted_at"):
        lines.append(f"- CMS Update Posted: {release['posted_at']}")
    if release.get("conversion_factor") is not None:
        lines.append(f"- Conversion Factor (CF): {release['conversion_factor']}")
    if release.get("docs_count") is not None:
        lines.append(f"- Guidance Documents: {release['docs_count']}")
    lines.append("")

    lines.append("## Datasets In This Release")
    for dataset in datasets:
        lines.append(f"- **{dataset.get('dataset')}**: {dataset.get('row_count', 0)} records")
        source_files = dataset.get("source_files", [])
        if source_files:
            lines.append(f"  - Source files: {', '.join(source_files)}")
        sample_columns = dataset.get("sample_columns", [])
        if sample_columns:
            lines.append(f"  - Sample columns: {', '.join(sample_columns)}")
    lines.append("")

    lines.append("## Payment Formula (Physician Fee Schedule)")
    lines.append(f"{PAYMENT_FORMULA['equation']}")
    lines.append("")
    for var, desc in PAYMENT_FORMULA["variables"].items():
        lines.append(f"- **{var}**: {desc}")
    lines.append("")

    if status_section:
        lines.append("## Status Indicators Observed")
        codes = status_section.get("codes_present", [])
        if codes:
            lines.append(f"Codes observed in data: {', '.join(codes)}")
        glossary = status_section.get("glossary", {})
        for code, desc in glossary.items():
            lines.append(f"- `{code}` — {desc}")
        lines.append("")

    if global_section:
        lines.append("## Global Period Values Observed")
        values = global_section.get("values_present", [])
        if values:
            lines.append(f"Values observed: {', '.join(values)}")
        glossary = global_section.get("glossary", {})
        for code, desc in glossary.items():
            if desc:
                lines.append(f"- `{code}` — {desc}")
        lines.append("")

    if POLITY_NOTES := summary.get("policy_notes", []):
        lines.append("## Policy Notes")
        for note in POLITY_NOTES:
            lines.append(f"- {note}")
        lines.append("")

    if SUP := summary.get("support", {}):
        lines.append("## Helpful Links & Contacts")
        if SUP.get("pfs_lookup_tool"):
            lines.append(f"- Physician Fee Schedule Look-Up Tool: {SUP['pfs_lookup_tool']}")
        if SUP.get("contractor_support"):
            lines.append(f"- Medicare Administrative Contractor directory: {SUP['contractor_support']}")
        if SUP.get("helpdesk_email"):
            lines.append(f"- CMS PFS help desk: {SUP['helpdesk_email']}")
        lines.append("")

    if guidance_docs:
        lines.append("## Guidance Documents Stored")
        for doc in guidance_docs:
            filename = doc.get("filename") or doc.get("path")
            posted = doc.get("posted_at")
            lines.append(f"- {filename} ({posted if posted else 'posted date unknown'})")

    lines.append("")
    lines.append(f"_Generated at {summary.get('generated_at', datetime.utcnow().isoformat())}_")
    return "\n".join(lines)


def write_summary_files(summary: Dict[str, Any], docs_dir: Path) -> Dict[str, str]:
    """Persist summary JSON and Markdown to the docs directory."""
    docs_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = docs_dir / "summary.json"
    summary_markdown_path = docs_dir / "summary.md"

    # Add summary generation metadata
    summary["summary_metadata"] = {
        "generated_at": datetime.now().isoformat(),
        "generator_version": SUMMARY_GENERATOR_VERSION,
        "extraction_tool_version": GUIDANCE_EXTRACTION_TOOL_VERSION
    }

    with open(summary_json_path, "w") as json_file:
        json.dump(summary, json_file, indent=2, default=str)

    markdown_content = _build_markdown(summary)
    summary_markdown_path.write_text(markdown_content)

    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path)
    }


def extract_pdf_page_count(pdf_path: Path) -> Optional[int]:
    """
    Extract page count from a PDF file.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Number of pages, or None if extraction fails
    """
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except ImportError:
        # pypdf not available, try PyPDF2 as fallback
        try:
            import PyPDF2
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                return len(reader.pages)
        except (ImportError, Exception):
            return None
    except Exception:
        # PDF may be corrupted or invalid
        return None
