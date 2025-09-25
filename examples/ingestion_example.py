#!/usr/bin/env python3
"""
Example: How to use the CMS Pricing API ingestion system

This example demonstrates:
1. Running individual ingesters
2. Using the CLI tool
3. Setting up automated ingestion
4. Monitoring ingestion tasks
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cms_pricing.ingestion.mpfs import MPFSIngester
from cms_pricing.ingestion.opps import OPPSIngester
from cms_pricing.ingestion.scheduler import scheduler


async def example_individual_ingestion():
    """Example: Run individual ingesters"""
    print("🔄 Example 1: Individual Ingestion")
    print("=" * 50)
    
    # Ingest MPFS data for 2025
    print("📊 Ingesting MPFS data for 2025...")
    mpfs_ingester = MPFSIngester("./data")
    
    try:
        result = await mpfs_ingester.ingest(2025, None)
        print(f"✅ MPFS ingestion completed!")
        print(f"   Build ID: {result['build_id']}")
        print(f"   Digest: {result['digest']}")
        print(f"   Files: {len(result['manifest']['files'])}")
        
        if result['warnings']:
            print(f"⚠️  Warnings: {len(result['warnings'])}")
            for warning in result['warnings'][:3]:
                print(f"     - {warning}")
        
    except Exception as e:
        print(f"❌ MPFS ingestion failed: {e}")
    
    print()
    
    # Ingest OPPS data for Q1 2025
    print("📊 Ingesting OPPS data for Q1 2025...")
    opps_ingester = OPPSIngester("./data")
    
    try:
        result = await opps_ingester.ingest(2025, "1")
        print(f"✅ OPPS ingestion completed!")
        print(f"   Build ID: {result['build_id']}")
        print(f"   Digest: {result['digest']}")
        print(f"   Files: {len(result['manifest']['files'])}")
        
    except Exception as e:
        print(f"❌ OPPS ingestion failed: {e}")
    
    print()


async def example_scheduler():
    """Example: Use the ingestion scheduler"""
    print("🤖 Example 2: Ingestion Scheduler")
    print("=" * 50)
    
    # Start the scheduler
    await scheduler.start()
    
    # Schedule some tasks
    print("📅 Scheduling ingestion tasks...")
    
    task1 = await scheduler.schedule_ingestion("MPFS", 2025, None)
    task2 = await scheduler.schedule_ingestion("OPPS", 2025, "1")
    task3 = await scheduler.schedule_ingestion("OPPS", 2025, "2")
    
    print(f"✅ Scheduled tasks:")
    print(f"   - {task1}")
    print(f"   - {task2}")
    print(f"   - {task3}")
    
    # Wait a bit for tasks to process
    print("⏳ Waiting for tasks to process...")
    await asyncio.sleep(5)
    
    # Check task status
    print("📊 Task Status:")
    for task_id in [task1, task2, task3]:
        task = scheduler.get_task_status(task_id)
        if task:
            print(f"   {task_id}: {task.status.value}")
            if task.error_message:
                print(f"     Error: {task.error_message}")
    
    print()


def example_cli_usage():
    """Example: CLI usage"""
    print("💻 Example 3: CLI Usage")
    print("=" * 50)
    
    print("The CMS Pricing API includes a comprehensive CLI tool:")
    print()
    
    print("📋 List available datasets:")
    print("   python -m cms_pricing.cli ingestion --list")
    print()
    
    print("🔄 Run a single ingestion:")
    print("   python -m cms_pricing.cli ingestion ingest --dataset MPFS --year 2025")
    print("   python -m cms_pricing.cli ingestion ingest --dataset OPPS --year 2025 --quarter 1")
    print()
    
    print("📊 List ingestion tasks:")
    print("   python -m cms_pricing.cli ingestion list-tasks")
    print("   python -m cms_pricing.cli ingestion list-tasks --status completed")
    print()
    
    print("📸 List database snapshots:")
    print("   python -m cms_pricing.cli snapshots list-snapshots")
    print("   python -m cms_pricing.cli snapshots list-snapshots --dataset MPFS --year 2025")
    print()
    
    print("🔍 Show system status:")
    print("   python -m cms_pricing.cli status")
    print()
    
    print("📅 Schedule a task:")
    print("   python -m cms_pricing.cli ingestion schedule --dataset OPPS --year 2025")
    print()
    
    print("🚫 Cancel a task:")
    print("   python -m cms_pricing.cli ingestion cancel-task <task-id>")
    print()
    
    print("🔄 Retry a failed task:")
    print("   python -m cms_pricing.cli ingestion retry-task <task-id>")
    print()


def example_automated_setup():
    """Example: Automated setup"""
    print("⚙️  Example 4: Automated Setup")
    print("=" * 50)
    
    print("For production deployment, you can set up automated ingestion:")
    print()
    
    print("1️⃣  Set up cron jobs:")
    print("   ./scripts/setup_cron.sh")
    print()
    
    print("2️⃣  Use the comprehensive ingestion script:")
    print("   python scripts/ingest_all.py --all --year 2025")
    print("   python scripts/ingest_all.py --dataset MPFS --year 2025")
    print("   python scripts/ingest_all.py --schedule --dataset OPPS --year 2025 --quarter 1")
    print()
    
    print("3️⃣  Monitor ingestion:")
    print("   # Check logs")
    print("   tail -f logs/ingestion.log")
    print()
    print("   # Check data directory")
    print("   ls -la data/")
    print()
    print("   # Check database snapshots")
    print("   python -m cms_pricing.cli snapshots list-snapshots")
    print()


async def example_data_validation():
    """Example: Data validation and quality checks"""
    print("🔍 Example 5: Data Validation")
    print("=" * 50)
    
    print("The ingestion system includes comprehensive validation:")
    print()
    
    print("✅ Automatic validation checks:")
    print("   - Required columns present")
    print("   - Data types correct")
    print("   - HCPCS codes valid (5 characters)")
    print("   - CBSA codes valid (5 digits)")
    print("   - Status indicators valid")
    print("   - Wage index values reasonable (0.5 - 2.0)")
    print("   - No negative rates")
    print("   - No null critical values")
    print()
    
    print("📊 Validation results:")
    print("   - Warnings logged for review")
    print("   - Invalid data flagged")
    print("   - Quality metrics tracked")
    print()
    
    print("🛠️  Custom validation:")
    print("   # Override validate_data method in your ingester")
    print("   def validate_data(self, normalized_data):")
    print("       warnings = []")
    print("       # Add your custom validation logic")
    print("       return warnings")
    print()


async def main():
    """Run all examples"""
    print("🚀 CMS Pricing API Ingestion System Examples")
    print("=" * 60)
    print()
    
    # Run examples
    await example_individual_ingestion()
    await example_scheduler()
    example_cli_usage()
    example_automated_setup()
    await example_data_validation()
    
    print("🎯 Summary")
    print("=" * 50)
    print("The ingestion system provides:")
    print("✅ Individual ingesters for each dataset type")
    print("✅ Automated scheduling and task management")
    print("✅ Comprehensive CLI tool for management")
    print("✅ Data validation and quality checks")
    print("✅ Audit trails and manifest generation")
    print("✅ Production-ready automated setup")
    print()
    print("📚 Next steps:")
    print("   1. Create additional ingesters for ASC, CLFS, DMEPOS, ASP, NADAC")
    print("   2. Set up automated ingestion with cron jobs")
    print("   3. Monitor ingestion quality and performance")
    print("   4. Integrate with your data pipeline")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Examples cancelled by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
