"""
CLI interface for data ingestion pipeline
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import List, Optional

import structlog

from cms_pricing.ingestion.nearest_zip_ingestion import NearestZipIngestionPipeline

logger = structlog.get_logger()


async def run_ingestion(
    sources: List[str],
    output_dir: str,
    dry_run: bool = False,
    verbose: bool = False
) -> int:
    """Run data ingestion pipeline"""
    
    if verbose:
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
    
    logger.info("Starting data ingestion", sources=sources, dry_run=dry_run)
    
    try:
        pipeline = NearestZipIngestionPipeline(output_dir)
        
        if "all" in sources:
            results = await pipeline.run_full_pipeline()
        else:
            results = {}
            for source in sources:
                result = await pipeline.run_single_source(source)
                results[source] = result
        
        # Print results
        print("\n" + "="*60)
        print("INGESTION RESULTS")
        print("="*60)
        
        for source, result in results.items():
            status = result.get('status', 'unknown')
            record_counts = result.get('record_counts', {})
            
            print(f"\n{source.upper()}:")
            print(f"  Status: {status}")
            
            if record_counts:
                print("  Records loaded:")
                for table, count in record_counts.items():
                    print(f"    {table}: {count:,}")
            
            if 'warnings' in result and result['warnings']:
                print("  Warnings:")
                for warning in result['warnings']:
                    print(f"    - {warning}")
            
            if 'error' in result:
                print(f"  Error: {result['error']}")
        
        # Check overall status
        overall_status = results.get('overall_status', 'unknown')
        if overall_status == 'success':
            print(f"\n✅ All data sources ingested successfully!")
            return 0
        elif overall_status == 'partial_failure':
            print(f"\n⚠️  Some data sources failed to ingest")
            return 1
        else:
            print(f"\n❌ Ingestion failed")
            return 1
            
    except Exception as e:
        logger.error("Ingestion pipeline failed", error=str(e), exc_info=True)
        print(f"\n❌ Ingestion failed: {e}")
        return 1


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Data ingestion pipeline for nearest ZIP resolver",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all data sources
  python -m cms_pricing.cli.ingestion --all

  # Ingest specific sources
  python -m cms_pricing.cli.ingestion --source gazetteer --source uds_crosswalk

  # Dry run (validate only)
  python -m cms_pricing.cli.ingestion --all --dry-run

  # Verbose output
  python -m cms_pricing.cli.ingestion --all --verbose
        """
    )
    
    # Data source selection
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--all", 
        action="store_true",
        help="Ingest all data sources"
    )
    source_group.add_argument(
        "--source",
        action="append",
        choices=["gazetteer", "uds_crosswalk", "cms_zip5", "cms_zip9", "simplemaps", "nber"],
        help="Specific data source to ingest (can be used multiple times)"
    )
    
    # Options
    parser.add_argument(
        "--output-dir",
        default="./data/ingestion",
        help="Output directory for ingested data (default: ./data/ingestion)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate data sources without loading to database"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Determine sources to process
    if args.all:
        sources = ["all"]
    else:
        sources = args.source
    
    # Run ingestion
    try:
        exit_code = asyncio.run(run_ingestion(
            sources=sources,
            output_dir=args.output_dir,
            dry_run=args.dry_run,
            verbose=args.verbose
        ))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Ingestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
