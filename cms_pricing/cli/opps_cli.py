#!/usr/bin/env python3
"""
OPPS CLI Interface
==================

Command-line interface for OPPS ingester operations following Global API Program v1.0 standards.

Author: CMS Pricing Platform Team
Version: 1.0.0
Global API Program Compliance: v1.0
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import argparse
import structlog

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor
from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper
from cms_pricing.ingestion.contracts.schema_registry import SchemaRegistry


class OPPSCLI:
    """CLI interface for OPPS operations."""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.setup_logging()
    
    def setup_logging(self):
        """Setup structured logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    async def discover(self, args) -> int:
        """Discover OPPS files."""
        self.logger.info("Starting OPPS discovery", args=vars(args))
        
        try:
            scraper = CMSOPPSScraper(output_dir=Path(args.output_dir))
            
            if args.latest:
                files = await scraper.discover_latest(quarters=args.quarters)
            else:
                files = await scraper.discover_files(max_quarters=args.max_quarters)
            
            self.logger.info("Discovery completed", files_found=len(files))
            
            # Output results
            if args.output_format == 'json':
                result = {
                    "discovery_time": datetime.utcnow().isoformat(),
                    "files_found": len(files),
                    "files": [
                        {
                            "url": f.url,
                            "filename": f.filename,
                            "file_type": f.file_type,
                            "batch_id": f.batch_id,
                            "metadata": f.metadata
                        }
                        for f in files
                    ]
                }
                print(json.dumps(result, indent=2))
            else:
                print(f"Discovered {len(files)} OPPS files:")
                for file_info in files:
                    print(f"  {file_info.batch_id}: {file_info.filename} ({file_info.file_type})")
            
            return 0
            
        except Exception as e:
            self.logger.error("Discovery failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def download(self, args) -> int:
        """Download OPPS files."""
        self.logger.info("Starting OPPS download", args=vars(args))
        
        try:
            scraper = CMSOPPSScraper(output_dir=Path(args.output_dir))
            
            if args.latest:
                files = await scraper.discover_latest(quarters=args.quarters)
            else:
                files = await scraper.discover_files(max_quarters=args.max_quarters)
            
            downloaded_count = 0
            failed_count = 0
            
            for file_info in files:
                try:
                    await scraper.download_file(file_info)
                    downloaded_count += 1
                    self.logger.info("File downloaded", filename=file_info.filename)
                except Exception as e:
                    failed_count += 1
                    self.logger.error("Download failed", filename=file_info.filename, error=str(e))
            
            self.logger.info("Download completed", 
                           downloaded=downloaded_count, 
                           failed=failed_count)
            
            print(f"Downloaded {downloaded_count} files, {failed_count} failed")
            return 0 if failed_count == 0 else 1
            
        except Exception as e:
            self.logger.error("Download failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def ingest(self, args) -> int:
        """Ingest OPPS data."""
        self.logger.info("Starting OPPS ingestion", args=vars(args))
        
        try:
            ingester = OPPSIngestor(
                output_dir=Path(args.output_dir),
                database_url=args.database_url,
                cpt_masking_enabled=args.cpt_masking
            )
            
            if args.batch_id:
                # Ingest specific batch
                result = await ingester.ingest_batch(args.batch_id)
                
                if result["status"] == "success":
                    self.logger.info("Ingestion completed successfully", batch_id=args.batch_id)
                    print(f"Ingestion completed successfully for batch {args.batch_id}")
                    
                    if args.output_format == 'json':
                        print(json.dumps(result, indent=2))
                    
                    return 0
                else:
                    self.logger.error("Ingestion failed", batch_id=args.batch_id, result=result)
                    print(f"Ingestion failed for batch {args.batch_id}: {result.get('error', 'Unknown error')}")
                    return 1
            else:
                # Discover and ingest latest
                scraper = CMSOPPSScraper(output_dir=Path(args.output_dir))
                files = await scraper.discover_latest(quarters=args.quarters)
                
                if not files:
                    print("No files found to ingest")
                    return 1
                
                # Group files by batch
                batches = {}
                for file_info in files:
                    batch_id = file_info.batch_id
                    if batch_id not in batches:
                        batches[batch_id] = []
                    batches[batch_id].append(file_info)
                
                # Ingest each batch
                success_count = 0
                failed_count = 0
                
                for batch_id in batches:
                    try:
                        result = await ingester.ingest_batch(batch_id)
                        if result["status"] == "success":
                            success_count += 1
                            self.logger.info("Batch ingested successfully", batch_id=batch_id)
                        else:
                            failed_count += 1
                            self.logger.error("Batch ingestion failed", batch_id=batch_id, result=result)
                    except Exception as e:
                        failed_count += 1
                        self.logger.error("Batch ingestion failed", batch_id=batch_id, error=str(e))
                
                self.logger.info("Ingestion completed", 
                               success_count=success_count, 
                               failed_count=failed_count)
                
                print(f"Ingestion completed: {success_count} successful, {failed_count} failed")
                return 0 if failed_count == 0 else 1
            
        except Exception as e:
            self.logger.error("Ingestion failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def reprocess(self, args) -> int:
        """Reprocess OPPS data."""
        self.logger.info("Starting OPPS reprocessing", args=vars(args))
        
        try:
            ingester = OPPSIngestor(
                output_dir=Path(args.output_dir),
                database_url=args.database_url,
                cpt_masking_enabled=args.cpt_masking
            )
            
            # Reprocess specific batch
            result = await ingester.ingest_batch(args.batch_id)
            
            if result["status"] == "success":
                self.logger.info("Reprocessing completed successfully", batch_id=args.batch_id)
                print(f"Reprocessing completed successfully for batch {args.batch_id}")
                
                if args.output_format == 'json':
                    print(json.dumps(result, indent=2))
                
                return 0
            else:
                self.logger.error("Reprocessing failed", batch_id=args.batch_id, result=result)
                print(f"Reprocessing failed for batch {args.batch_id}: {result.get('error', 'Unknown error')}")
                return 1
            
        except Exception as e:
            self.logger.error("Reprocessing failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def backfill(self, args) -> int:
        """Backfill OPPS data."""
        self.logger.info("Starting OPPS backfill", args=vars(args))
        
        try:
            ingester = OPPSIngestor(
                output_dir=Path(args.output_dir),
                database_url=args.database_url,
                cpt_masking_enabled=args.cpt_masking
            )
            
            # Generate batch IDs for backfill period
            start_year = args.start_year
            end_year = args.end_year
            quarters = args.quarters
            
            batch_ids = []
            for year in range(start_year, end_year + 1):
                for quarter in range(1, 5):
                    if year == end_year and quarter > quarters:
                        break
                    batch_id = f"opps_{year}q{quarter}_r01"
                    batch_ids.append(batch_id)
            
            self.logger.info("Backfill batch IDs generated", count=len(batch_ids))
            
            # Process each batch
            success_count = 0
            failed_count = 0
            
            for batch_id in batch_ids:
                try:
                    result = await ingester.ingest_batch(batch_id)
                    if result["status"] == "success":
                        success_count += 1
                        self.logger.info("Batch backfilled successfully", batch_id=batch_id)
                    else:
                        failed_count += 1
                        self.logger.error("Batch backfill failed", batch_id=batch_id, result=result)
                except Exception as e:
                    failed_count += 1
                    self.logger.error("Batch backfill failed", batch_id=batch_id, error=str(e))
            
            self.logger.info("Backfill completed", 
                           success_count=success_count, 
                           failed_count=failed_count)
            
            print(f"Backfill completed: {success_count} successful, {failed_count} failed")
            return 0 if failed_count == 0 else 1
            
        except Exception as e:
            self.logger.error("Backfill failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def validate(self, args) -> int:
        """Validate OPPS data."""
        self.logger.info("Starting OPPS validation", args=vars(args))
        
        try:
            ingester = OPPSIngestor(
                output_dir=Path(args.output_dir),
                database_url=args.database_url,
                cpt_masking_enabled=args.cpt_masking
            )
            
            # Validate specific batch
            result = await ingester.ingest_batch(args.batch_id)
            
            if result["status"] == "success":
                validation_results = result.get("validation_results", {})
                
                if validation_results.get("passed", False):
                    self.logger.info("Validation passed", batch_id=args.batch_id)
                    print(f"Validation passed for batch {args.batch_id}")
                    
                    if args.output_format == 'json':
                        print(json.dumps(validation_results, indent=2))
                    
                    return 0
                else:
                    self.logger.error("Validation failed", batch_id=args.batch_id, results=validation_results)
                    print(f"Validation failed for batch {args.batch_id}")
                    
                    if args.output_format == 'json':
                        print(json.dumps(validation_results, indent=2))
                    
                    return 1
            else:
                self.logger.error("Validation failed", batch_id=args.batch_id, result=result)
                print(f"Validation failed for batch {args.batch_id}: {result.get('error', 'Unknown error')}")
                return 1
            
        except Exception as e:
            self.logger.error("Validation failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    async def status(self, args) -> int:
        """Get OPPS ingester status."""
        self.logger.info("Getting OPPS ingester status", args=vars(args))
        
        try:
            ingester = OPPSIngestor(
                output_dir=Path(args.output_dir),
                database_url=args.database_url,
                cpt_masking_enabled=args.cpt_masking
            )
            
            # Get status information
            status_info = {
                "dataset_name": ingester.dataset_name,
                "release_cadence": ingester.release_cadence,
                "data_classification": ingester.data_classification.value,
                "contract_schema_ref": ingester.contract_schema_ref,
                "cpt_masking_enabled": ingester.cpt_masking_enabled,
                "validation_rules_count": len(ingester.validators),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if args.output_format == 'json':
                print(json.dumps(status_info, indent=2))
            else:
                print("OPPS Ingester Status:")
                for key, value in status_info.items():
                    print(f"  {key}: {value}")
            
            return 0
            
        except Exception as e:
            self.logger.error("Status check failed", error=str(e))
            print(f"Error: {e}", file=sys.stderr)
            return 1
    
    def create_parser(self) -> argparse.ArgumentParser:
        """Create command line argument parser."""
        parser = argparse.ArgumentParser(
            description="OPPS Ingester CLI",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  %(prog)s discover --latest --quarters 2
  %(prog)s download --latest --quarters 2
  %(prog)s ingest --batch-id opps_2025q1_r01
  %(prog)s reprocess --batch-id opps_2025q1_r01
  %(prog)s backfill --start-year 2024 --end-year 2025 --quarters 4
  %(prog)s validate --batch-id opps_2025q1_r01
  %(prog)s status
            """
        )
        
        # Global arguments
        parser.add_argument('--output-dir', type=Path, default=Path('data'), 
                          help='Output directory for data files')
        parser.add_argument('--database-url', help='Database connection URL')
        parser.add_argument('--cpt-masking', action='store_true', default=True,
                          help='Enable CPT masking for external outputs')
        parser.add_argument('--output-format', choices=['text', 'json'], default='text',
                          help='Output format for results')
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Enable verbose logging')
        
        # Subcommands
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Discover command
        discover_parser = subparsers.add_parser('discover', help='Discover OPPS files')
        discover_parser.add_argument('--latest', action='store_true',
                                   help='Discover latest quarters only')
        discover_parser.add_argument('--quarters', type=int, default=2,
                                   help='Number of quarters to discover')
        discover_parser.add_argument('--max-quarters', type=int, default=8,
                                   help='Maximum quarters to discover')
        
        # Download command
        download_parser = subparsers.add_parser('download', help='Download OPPS files')
        download_parser.add_argument('--latest', action='store_true',
                                   help='Download latest quarters only')
        download_parser.add_argument('--quarters', type=int, default=2,
                                   help='Number of quarters to download')
        download_parser.add_argument('--max-quarters', type=int, default=8,
                                   help='Maximum quarters to download')
        
        # Ingest command
        ingest_parser = subparsers.add_parser('ingest', help='Ingest OPPS data')
        ingest_parser.add_argument('--batch-id', help='Specific batch ID to ingest')
        ingest_parser.add_argument('--latest', action='store_true',
                                 help='Ingest latest quarters only')
        ingest_parser.add_argument('--quarters', type=int, default=2,
                                 help='Number of quarters to ingest')
        
        # Reprocess command
        reprocess_parser = subparsers.add_parser('reprocess', help='Reprocess OPPS data')
        reprocess_parser.add_argument('--batch-id', required=True,
                                    help='Batch ID to reprocess')
        
        # Backfill command
        backfill_parser = subparsers.add_parser('backfill', help='Backfill OPPS data')
        backfill_parser.add_argument('--start-year', type=int, required=True,
                                   help='Start year for backfill')
        backfill_parser.add_argument('--end-year', type=int, required=True,
                                   help='End year for backfill')
        backfill_parser.add_argument('--quarters', type=int, default=4,
                                   help='Number of quarters per year')
        
        # Validate command
        validate_parser = subparsers.add_parser('validate', help='Validate OPPS data')
        validate_parser.add_argument('--batch-id', required=True,
                                   help='Batch ID to validate')
        
        # Status command
        status_parser = subparsers.add_parser('status', help='Get ingester status')
        
        return parser
    
    async def run(self, args: List[str] = None) -> int:
        """Run the CLI with given arguments."""
        parser = self.create_parser()
        
        if args is None:
            args = sys.argv[1:]
        
        parsed_args = parser.parse_args(args)
        
        if not parsed_args.command:
            parser.print_help()
            return 1
        
        # Set logging level
        if parsed_args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Route to appropriate command
        command_map = {
            'discover': self.discover,
            'download': self.download,
            'ingest': self.ingest,
            'reprocess': self.reprocess,
            'backfill': self.backfill,
            'validate': self.validate,
            'status': self.status
        }
        
        command_func = command_map.get(parsed_args.command)
        if not command_func:
            print(f"Unknown command: {parsed_args.command}", file=sys.stderr)
            return 1
        
        try:
            return await command_func(parsed_args)
        except KeyboardInterrupt:
            self.logger.info("Operation cancelled by user")
            print("\nOperation cancelled by user", file=sys.stderr)
            return 130
        except Exception as e:
            self.logger.error("Unexpected error", error=str(e))
            print(f"Unexpected error: {e}", file=sys.stderr)
            return 1


# CLI entry point
async def main():
    """Main CLI entry point."""
    cli = OPPSCLI()
    return await cli.run()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
