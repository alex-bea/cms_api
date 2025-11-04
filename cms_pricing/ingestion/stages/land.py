"""
Land stage module for DIS pipeline.

Phase 2 Refactoring Context:
    - Step 5: Stage integration
      • Plan: artifacts/phase2_step5_detailed_plan.md
      • Verification: artifacts/phase2_step5_verification_report.md

Per DIS §3.2: Download and store raw files, create manifests, handle guidance documents.
This module extracts landing logic from ingestors for reuse across datasets.
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import structlog

from ..contracts.ingestor_spec import SourceFile, RawBatch

logger = structlog.get_logger()


@dataclass
class LandConfig:
    """Configuration for land stage"""
    output_dir: str
    dataset_name: str
    enable_guidance_extraction: bool = True
    enable_pdf_page_count: bool = True


def infer_file_type_from_name(filename: str, content_type: Optional[str] = None) -> Optional[str]:
    """Infer a logical file_type label based on filename or content-type."""
    extension_map = {
        ".zip": "zip",
        ".csv": "csv",
        ".txt": "txt",
        ".xlsx": "xlsx",
        ".xls": "xls",
        ".pdf": "pdf"
    }
    mime_map = {
        "application/zip": "zip",
        "application/x-zip-compressed": "zip",
        "application/octet-stream": "binary",
        "text/plain": "txt",
        "text/csv": "csv",
        "application/csv": "csv",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "application/pdf": "pdf"
    }
    ext = Path(filename).suffix.lower()
    if ext in extension_map:
        return extension_map[ext]
    if content_type:
        ctype = content_type.lower()
        for key, label in mime_map.items():
            if key in ctype:
                return label
    return None


def is_guidance_file(file_type: str) -> bool:
    """Check if file type is considered a guidance document."""
    GUIDANCE_FILE_TYPES = {"pdf"}
    return file_type in GUIDANCE_FILE_TYPES


# Phase 2 Step 5: Stage integration extraction
# See: artifacts/phase2_step5_detailed_plan.md
async def execute_land(
    release_id: str,
    source_files: List[SourceFile],
    config: LandConfig,
    scraper: Optional[Any] = None,
    discovery_callback: Optional[callable] = None
) -> Dict[str, Any]:
    """
    Execute land stage with shared services per DIS §3.2.
    
    Downloads files, creates manifests, handles guidance documents, and returns RawBatch.
    
    Args:
        release_id: Unique identifier for this release
        source_files: List of source files to download
        config: Land configuration
        scraper: Optional scraper instance (for lineage metadata)
        discovery_callback: Optional callback for custom file discovery
        
    Returns:
        Landing results with file metadata, RawBatch contents, and manifest paths
    """
    logger.info("Starting land stage", release_id=release_id, files_count=len(source_files))
    
    try:
        # Create raw directory structure per DIS §4
        raw_dir = Path(config.output_dir) / "raw" / config.dataset_name / release_id / "files"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate batch ID
        batch_id = str(uuid.uuid4())
        
        # Initialize manifest data
        manifest_data = {
            "release_id": release_id,
            "batch_id": batch_id,
            "source": config.dataset_name,
            "files": [],
            "fetched_at": datetime.now().isoformat(),
            "discovered_from": "cms_scraper" if scraper else "manual",
            "source_url": "https://www.cms.gov/medicare/payment/fee-schedules",
            "license": {
                "name": "CMS Public Domain",
                "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
                "attribution_required": False
            },
            "notes_url": "https://www.cms.gov/medicare/payment/fee-schedules"
        }
        
        raw_contents: Dict[str, bytes] = {}
        guidance_entries: List[Dict[str, Any]] = []
        data_file_entries: List[Dict[str, Any]] = []
        total_bytes = 0
        docs_dir = Path(config.output_dir) / "docs" / config.dataset_name / release_id
        docs_raw_dir = docs_dir / "raw"
        docs_raw_created = False
        docs_manifest_path: Optional[Path] = None
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for source_file in source_files:
                try:
                    logger.info("Downloading file", url=source_file.url, filename=source_file.filename)
                    
                    # Download file
                    if source_file.url and source_file.url.startswith("file://"):
                        local_path = Path(source_file.url.replace("file://", ""))
                        content = local_path.read_bytes()
                        response_headers = {}
                    elif source_file.url:
                        response = await client.get(source_file.url)
                        content = response.content
                        response_headers = response.headers
                    elif config.output_dir and release_id:
                        # Fallback to existing file
                        fallback_path = Path(config.output_dir) / "raw" / config.dataset_name / release_id / "files" / source_file.filename
                        content = fallback_path.read_bytes()
                        response_headers = {}
                    else:
                        raise FileNotFoundError(f"No URL available for source file {source_file.filename}")
                    
                    total_bytes += len(content)
                    
                    # Calculate file hash
                    file_hash = hashlib.sha256(content).hexdigest()
                    file_type = (source_file.file_type or infer_file_type_from_name(
                        source_file.filename, source_file.content_type
                    ) or "binary")
                    source_file.file_type = file_type
                    
                    # Determine if guidance document
                    is_guidance_doc = config.enable_guidance_extraction and is_guidance_file(file_type)
                    target_dir = raw_dir if not is_guidance_doc else docs_raw_dir
                    if is_guidance_doc and not docs_raw_created:
                        docs_raw_dir.mkdir(parents=True, exist_ok=True)
                        docs_raw_created = True
                    
                    file_path = target_dir / source_file.filename
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    # Update source file metadata
                    source_file.checksum = file_hash
                    source_file.expected_size_bytes = len(content)
                    source_file.last_modified = datetime.utcnow()
                    last_modified_header = response_headers.get('last-modified') if response_headers else None
                    etag_header = response_headers.get('etag') if response_headers else None
                    
                    entry_common = {
                        "sha256": file_hash,
                        "size_bytes": len(content),
                        "content_type": source_file.content_type,
                        "file_type": file_type,
                        "url": source_file.url,
                        "last_modified": last_modified_header or (source_file.last_modified.isoformat() if source_file.last_modified else None),
                        "etag": etag_header
                    }
                    
                    if source_file.metadata:
                        entry_common["metadata"] = source_file.metadata
                    
                    if is_guidance_doc:
                        # Extract PDF page count if enabled
                        pdf_page_count = None
                        if config.enable_pdf_page_count:
                            try:
                                from ..docs.guidance_summary import extract_pdf_page_count
                                pdf_page_count = extract_pdf_page_count(file_path)
                            except Exception as e:
                                logger.debug("Failed to extract PDF page count", 
                                           filename=source_file.filename, 
                                           error=str(e))
                        
                        metadata_entry = {
                            **entry_common,
                            "path": str(file_path.relative_to(docs_dir)),
                            "filename": source_file.filename
                        }
                        
                        if pdf_page_count is not None:
                            metadata_entry["pdf_page_count"] = pdf_page_count
                        
                        # Add lineage metadata
                        lineage_metadata = {}
                        if scraper and hasattr(scraper, 'last_manifest_path') and scraper.last_manifest_path:
                            lineage_metadata["discovery_manifest_path"] = str(scraper.last_manifest_path)
                        if manifest_data.get("batch_id"):
                            lineage_metadata["ingestion_batch_id"] = manifest_data["batch_id"]
                        if lineage_metadata:
                            metadata_entry["lineage"] = lineage_metadata
                        
                        if source_file.metadata and source_file.metadata.get("posted_at"):
                            metadata_entry["posted_at"] = source_file.metadata.get("posted_at")
                        
                        guidance_entries.append(metadata_entry)
                    else:
                        raw_contents[source_file.filename] = content
                        file_info = {
                            **entry_common,
                            "path": str(file_path.relative_to(raw_dir.parent))
                        }
                        data_file_entries.append(file_info)
                    
                    logger.info("File downloaded successfully", 
                              filename=source_file.filename, 
                              size=len(content),
                              hash=file_hash)
                    
                except Exception as e:
                    logger.error("Failed to download file", 
                               url=source_file.url, 
                               error=str(e))
                    raise
        
        # Update manifest with file entries
        manifest_data["files"] = data_file_entries
        manifest_data["rejects_directory"] = str(Path(config.output_dir) / "stage" / config.dataset_name / release_id / "reject")
        
        # Handle guidance documents manifest
        if guidance_entries:
            manifest_data["guidance_docs"] = guidance_entries
            docs_dir.mkdir(parents=True, exist_ok=True)
            docs_manifest = {
                "release_id": release_id,
                "batch_id": batch_id,
                "dataset": config.dataset_name,
                "generated_at": datetime.now().isoformat(),
                "documents": guidance_entries
            }
            
            if scraper and hasattr(scraper, 'last_manifest_path') and scraper.last_manifest_path:
                docs_manifest["discovery_manifest_path"] = str(scraper.last_manifest_path)
            
            docs_manifest_path = docs_dir / "docs_manifest.json"
            with open(docs_manifest_path, 'w') as f:
                json.dump(docs_manifest, f, indent=2)
        
        # Write main manifest
        manifest_path = raw_dir.parent / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=2)
        
        logger.info("Land stage completed", 
                   release_id=release_id, 
                   files_downloaded=len(data_file_entries) + len(guidance_entries))
        
        return {
            "status": "success",
            "release_id": release_id,
            "files_downloaded": len(data_file_entries) + len(guidance_entries),
            "raw_directory": str(raw_dir),
            "manifest_path": str(manifest_path),
            "docs_directory": str(docs_dir) if guidance_entries else None,
            "docs_manifest_path": str(docs_manifest_path) if docs_manifest_path else None,
            "guidance_documents": guidance_entries,
            "total_size_bytes": total_bytes,
            "source_files": source_files,
            "raw_content": raw_contents,
            "manifest": manifest_data,
            "raw_batch": RawBatch(
                source_files=source_files,
                raw_content=raw_contents,
                metadata={
                    "release_id": release_id,
                    "batch_id": batch_id,
                    "source": config.dataset_name
                },
                raw_data_path=str(raw_dir)
            )
        }
        
    except Exception as e:
        logger.error("Land stage failed", error=str(e), release_id=release_id)
        return {
            "status": "failed",
            "release_id": release_id,
            "error": str(e)
        }
