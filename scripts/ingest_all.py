#!/usr/bin/env python3
"""Comprehensive data ingestion script for all CMS datasets"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cms_pricing.ingestion.geography import GeographyIngester
from cms_pricing.ingestion.mpfs import MPFSIngester
from cms_pricing.ingestion.opps import OPPSIngester
from cms_pricing.ingestion.scheduler import scheduler
import structlog

logger = structlog.get_logger()


class DataIngestionManager:
    """Manages comprehensive data ingestion for all CMS datasets"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.ingesters = {
            'GEOGRAPHY': GeographyIngester,
            'MPFS': MPFSIngester,
            'OPPS': OPPSIngester,
            # Add more ingesters as they're created
        }
    
    async def ingest_all_current(self, year: int = None):
        """Ingest all datasets for the current year"""
        if year is None:
            year = datetime.now().year
        
        print(f"🔄 Starting comprehensive ingestion for {year}")
        print(f"📁 Data directory: {self.data_dir}")
        print()
        
        # Define ingestion tasks
        tasks = [
            # Geography data (must be first)
            ('GEOGRAPHY', year, None),
            
            # Annual datasets
            ('MPFS', year, None),
            
            # Quarterly datasets
            ('OPPS', year, '1'),
            ('OPPS', year, '2'),
            ('OPPS', year, '3'),
            ('OPPS', year, '4'),
            
            # Add more datasets as ingesters are created
            # ('ASC', year, '1'),
            # ('ASC', year, '2'),
            # ('ASC', year, '3'),
            # ('ASC', year, '4'),
            # ('CLFS', year, '1'),
            # ('CLFS', year, '2'),
            # ('CLFS', year, '3'),
            # ('CLFS', year, '4'),
            # ('DMEPOS', year, '1'),
            # ('DMEPOS', year, '2'),
            # ('DMEPOS', year, '3'),
            # ('DMEPOS', year, '4'),
            # ('ASP', year, '1'),
            # ('ASP', year, '2'),
            # ('ASP', year, '3'),
            # ('ASP', year, '4'),
        ]
        
        results = []
        failed_tasks = []
        
        for dataset_id, task_year, quarter in tasks:
            try:
                print(f"🔄 Ingesting {dataset_id} {task_year} Q{quarter or 'annual'}")
                
                # Get ingester class
                ingester_class = self.ingesters.get(dataset_id)
                if not ingester_class:
                    print(f"❌ No ingester found for {dataset_id}")
                    failed_tasks.append((dataset_id, task_year, quarter, "No ingester found"))
                    continue
                
                # Create ingester and run
                ingester = ingester_class(self.data_dir)
                result = await ingester.ingest(task_year, quarter)
                
                results.append(result)
                
                print(f"✅ {dataset_id} completed successfully")
                print(f"   Build ID: {result['build_id']}")
                print(f"   Digest: {result['digest'][:16]}...")
                print(f"   Files: {len(result['manifest']['files'])}")
                
                if result['warnings']:
                    print(f"⚠️  Warnings: {len(result['warnings'])}")
                    for warning in result['warnings'][:3]:  # Show first 3 warnings
                        print(f"     - {warning}")
                    if len(result['warnings']) > 3:
                        print(f"     ... and {len(result['warnings']) - 3} more")
                
                print()
                
            except Exception as e:
                print(f"❌ Failed to ingest {dataset_id}: {e}")
                failed_tasks.append((dataset_id, task_year, quarter, str(e)))
                logger.error("Ingestion failed", dataset_id=dataset_id, year=task_year, quarter=quarter, error=str(e))
                print()
        
        # Summary
        print("📊 Ingestion Summary")
        print(f"   ✅ Successful: {len(results)}")
        print(f"   ❌ Failed: {len(failed_tasks)}")
        print()
        
        if failed_tasks:
            print("❌ Failed Tasks:")
            for dataset_id, year, quarter, error in failed_tasks:
                print(f"   - {dataset_id} {year} Q{quarter or 'annual'}: {error}")
            print()
        
        if results:
            print("✅ Successful Ingestions:")
            for result in results:
                print(f"   - {result['dataset_id']} {result['build_id']}")
        
        return results, failed_tasks
    
    async def ingest_specific(self, dataset_id: str, year: int, quarter: str = None):
        """Ingest a specific dataset"""
        print(f"🔄 Ingesting {dataset_id} {year} Q{quarter or 'annual'}")
        
        ingester_class = self.ingesters.get(dataset_id.upper())
        if not ingester_class:
            print(f"❌ No ingester found for {dataset_id}")
            print(f"Available datasets: {', '.join(self.ingesters.keys())}")
            return None
        
        try:
            ingester = ingester_class(self.data_dir)
            result = await ingester.ingest(year, quarter)
            
            print(f"✅ {dataset_id} completed successfully")
            print(f"   Build ID: {result['build_id']}")
            print(f"   Digest: {result['digest']}")
            print(f"   Files: {len(result['manifest']['files'])}")
            
            return result
            
        except Exception as e:
            print(f"❌ Failed to ingest {dataset_id}: {e}")
            logger.error("Specific ingestion failed", dataset_id=dataset_id, year=year, quarter=quarter, error=str(e))
            return None
    
    async def schedule_ingestion(self, dataset_id: str, year: int, quarter: str = None):
        """Schedule an ingestion task"""
        await scheduler.start()
        
        task_id = await scheduler.schedule_ingestion(dataset_id.upper(), year, quarter)
        
        print(f"✅ Scheduled task {task_id}")
        return task_id
    
    def list_available_datasets(self):
        """List available datasets and their ingesters"""
        print("📋 Available Datasets:")
        print()
        
        for dataset_id, ingester_class in self.ingesters.items():
            print(f"   {dataset_id}: {ingester_class.__name__}")
        
        print()
        print("💡 Usage:")
        print("   python scripts/ingest_all.py --dataset MPFS --year 2025")
        print("   python scripts/ingest_all.py --all --year 2025")
        print("   python scripts/ingest_all.py --schedule --dataset OPPS --year 2025 --quarter 1")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="CMS Data Ingestion Tool")
    parser.add_argument('--data-dir', default='./data', help='Data directory')
    parser.add_argument('--year', type=int, default=datetime.now().year, help='Valuation year')
    parser.add_argument('--dataset', help='Specific dataset to ingest')
    parser.add_argument('--quarter', help='Quarter for quarterly datasets')
    parser.add_argument('--all', action='store_true', help='Ingest all datasets')
    parser.add_argument('--schedule', action='store_true', help='Schedule ingestion task')
    parser.add_argument('--list', action='store_true', help='List available datasets')
    
    args = parser.parse_args()
    
    manager = DataIngestionManager(args.data_dir)
    
    if args.list:
        manager.list_available_datasets()
        return
    
    if args.schedule:
        if not args.dataset:
            print("❌ --dataset required for scheduling")
            return
        
        await manager.schedule_ingestion(args.dataset, args.year, args.quarter)
        return
    
    if args.all:
        await manager.ingest_all_current(args.year)
        return
    
    if args.dataset:
        await manager.ingest_specific(args.dataset, args.year, args.quarter)
        return
    
    # Default: show help
    parser.print_help()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Ingestion cancelled by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.error("Unexpected error in main", error=str(e), exc_info=True)
        sys.exit(1)
