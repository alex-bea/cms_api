"""Command-line interface for CMS Pricing API management"""

import asyncio
import click
import json
from datetime import datetime
from typing import Optional

from cms_pricing.ingestion.scheduler import scheduler, TaskStatus
from cms_pricing.ingestion.mpfs import MPFSIngester
from cms_pricing.ingestion.opps import OPPSIngester
from cms_pricing.database import SessionLocal
from cms_pricing.models.snapshots import Snapshot
import structlog

logger = structlog.get_logger()


@click.group()
def cli():
    """CMS Pricing API Management CLI"""
    pass


@cli.group()
def ingestion():
    """Ingestion management commands"""
    pass


@ingestion.command()
@click.option('--dataset', '-d', required=True, help='Dataset ID (MPFS, OPPS, ASC, etc.)')
@click.option('--year', '-y', required=True, type=int, help='Valuation year')
@click.option('--quarter', '-q', type=str, help='Quarter (1-4) for quarterly datasets')
@click.option('--data-dir', default='./data', help='Data directory')
def ingest(dataset: str, year: int, quarter: Optional[str], data_dir: str):
    """Run a single ingestion task"""
    
    async def run_ingestion():
        try:
            # Get the appropriate ingester
            ingesters = {
                'MPFS': MPFSIngester,
                'OPPS': OPPSIngester,
            }
            
            ingester_class = ingesters.get(dataset.upper())
            if not ingester_class:
                click.echo(f"❌ Unknown dataset: {dataset}")
                click.echo(f"Available datasets: {', '.join(ingesters.keys())}")
                return
            
            # Create ingester and run
            ingester = ingester_class(data_dir)
            
            click.echo(f"🔄 Starting ingestion for {dataset} {year} Q{quarter or 'annual'}")
            
            result = await ingester.ingest(year, quarter)
            
            click.echo(f"✅ Ingestion completed successfully!")
            click.echo(f"   Dataset: {result['dataset_id']}")
            click.echo(f"   Build ID: {result['build_id']}")
            click.echo(f"   Digest: {result['digest']}")
            click.echo(f"   Files: {len(result['manifest']['files'])}")
            
            if result['warnings']:
                click.echo(f"⚠️  Warnings: {len(result['warnings'])}")
                for warning in result['warnings']:
                    click.echo(f"   - {warning}")
            
        except Exception as e:
            click.echo(f"❌ Ingestion failed: {e}")
            logger.error("CLI ingestion failed", error=str(e), exc_info=True)
    
    asyncio.run(run_ingestion())


@ingestion.command()
@click.option('--dataset', '-d', help='Filter by dataset ID')
@click.option('--year', '-y', type=int, help='Filter by year')
@click.option('--status', '-s', type=click.Choice(['pending', 'running', 'completed', 'failed', 'cancelled']), help='Filter by status')
def list_tasks(dataset: Optional[str], year: Optional[int], status: Optional[str]):
    """List ingestion tasks"""
    
    async def list_tasks_async():
        await scheduler.start()
        
        # Get tasks
        task_status = TaskStatus(status) if status else None
        tasks = scheduler.list_tasks(task_status)
        
        # Apply filters
        if dataset:
            tasks = [t for t in tasks if t.dataset_id.upper() == dataset.upper()]
        if year:
            tasks = [t for t in tasks if t.year == year]
        
        if not tasks:
            click.echo("No tasks found")
            return
        
        # Display tasks
        click.echo(f"Found {len(tasks)} tasks:")
        click.echo()
        
        for task in tasks:
            status_emoji = {
                TaskStatus.PENDING: "⏳",
                TaskStatus.RUNNING: "🔄",
                TaskStatus.COMPLETED: "✅",
                TaskStatus.FAILED: "❌",
                TaskStatus.CANCELLED: "🚫"
            }.get(task.status, "❓")
            
            click.echo(f"{status_emoji} {task.task_id}")
            click.echo(f"   Dataset: {task.dataset_id}")
            click.echo(f"   Year: {task.year}, Quarter: {task.quarter or 'annual'}")
            click.echo(f"   Status: {task.status.value}")
            click.echo(f"   Created: {task.created_at}")
            
            if task.started_at:
                click.echo(f"   Started: {task.started_at}")
            if task.completed_at:
                duration = task.completed_at - (task.started_at or task.created_at)
                click.echo(f"   Completed: {task.completed_at} (took {duration})")
            if task.error_message:
                click.echo(f"   Error: {task.error_message}")
            
            click.echo()
    
    asyncio.run(list_tasks_async())


@ingestion.command()
@click.argument('task_id')
def cancel_task(task_id: str):
    """Cancel a pending task"""
    
    async def cancel_task_async():
        await scheduler.start()
        
        success = await scheduler.cancel_task(task_id)
        
        if success:
            click.echo(f"✅ Cancelled task {task_id}")
        else:
            click.echo(f"❌ Could not cancel task {task_id} (not found or not pending)")
    
    asyncio.run(cancel_task_async())


@ingestion.command()
@click.argument('task_id')
def retry_task(task_id: str):
    """Retry a failed task"""
    
    async def retry_task_async():
        await scheduler.start()
        
        success = await scheduler.retry_failed_task(task_id)
        
        if success:
            click.echo(f"✅ Retrying task {task_id}")
        else:
            click.echo(f"❌ Could not retry task {task_id} (not found or not failed)")
    
    asyncio.run(retry_task_async())


@ingestion.command()
@click.option('--dataset', '-d', help='Filter by dataset ID')
@click.option('--year', '-y', type=int, help='Filter by year')
def schedule(dataset: str, year: int):
    """Schedule a new ingestion task"""
    
    async def schedule_async():
        await scheduler.start()
        
        # Determine quarter for quarterly datasets
        quarter = None
        if dataset.upper() in ['OPPS', 'ASC', 'CLFS', 'DMEPOS', 'ASP']:
            quarter = "1"  # Default to Q1
        
        task_id = await scheduler.schedule_ingestion(dataset.upper(), year, quarter)
        
        click.echo(f"✅ Scheduled task {task_id}")
    
    asyncio.run(schedule_async())


@cli.group()
def snapshots():
    """Snapshot management commands"""
    pass


@snapshots.command()
@click.option('--dataset', '-d', help='Filter by dataset ID')
@click.option('--year', '-y', type=int, help='Filter by year')
@click.option('--limit', '-l', type=int, default=10, help='Limit number of results')
def list_snapshots(dataset: Optional[str], year: Optional[int], limit: int):
    """List database snapshots"""
    
    db = SessionLocal()
    
    try:
        query = db.query(Snapshot)
        
        if dataset:
            query = query.filter(Snapshot.dataset_id == dataset.upper())
        if year:
            query = query.filter(Snapshot.effective_from >= f"{year}-01-01")
            query = query.filter(Snapshot.effective_from <= f"{year}-12-31")
        
        snapshots = query.order_by(Snapshot.created_at.desc()).limit(limit).all()
        
        if not snapshots:
            click.echo("No snapshots found")
            return
        
        click.echo(f"Found {len(snapshots)} snapshots:")
        click.echo()
        
        for snapshot in snapshots:
            click.echo(f"📸 {snapshot.dataset_id} - {snapshot.effective_from}")
            click.echo(f"   Digest: {snapshot.digest[:16]}...")
            click.echo(f"   Created: {snapshot.created_at}")
            if snapshot.source_url:
                click.echo(f"   Source: {snapshot.source_url}")
            click.echo()
    
    finally:
        db.close()


@snapshots.command()
@click.argument('digest')
def show_snapshot(digest: str):
    """Show details of a specific snapshot"""
    
    db = SessionLocal()
    
    try:
        snapshot = db.query(Snapshot).filter(Snapshot.digest == digest).first()
        
        if not snapshot:
            click.echo(f"❌ Snapshot not found: {digest}")
            return
        
        click.echo(f"📸 Snapshot Details")
        click.echo(f"   Dataset: {snapshot.dataset_id}")
        click.echo(f"   Effective: {snapshot.effective_from} to {snapshot.effective_to or 'ongoing'}")
        click.echo(f"   Digest: {snapshot.digest}")
        click.echo(f"   Created: {snapshot.created_at}")
        
        if snapshot.source_url:
            click.echo(f"   Source: {snapshot.source_url}")
        
        if snapshot.manifest_json:
            manifest = json.loads(snapshot.manifest_json)
            click.echo(f"   Files: {len(manifest.get('files', []))}")
            
            for file_info in manifest.get('files', []):
                click.echo(f"     - {file_info['filename']} ({file_info.get('size_bytes', 0)} bytes)")
    
    finally:
        db.close()


@cli.command()
def status():
    """Show system status"""
    
    async def show_status():
        await scheduler.start()
        
        # Get task counts by status
        all_tasks = scheduler.list_tasks()
        status_counts = {}
        for task in all_tasks:
            status_counts[task.status.value] = status_counts.get(task.status.value, 0) + 1
        
        click.echo("🔍 CMS Pricing API Status")
        click.echo()
        
        click.echo("📊 Ingestion Tasks:")
        for status, count in status_counts.items():
            emoji = {
                'pending': '⏳',
                'running': '🔄',
                'completed': '✅',
                'failed': '❌',
                'cancelled': '🚫'
            }.get(status, '❓')
            click.echo(f"   {emoji} {status.title()}: {count}")
        
        click.echo()
        
        # Get recent snapshots
        db = SessionLocal()
        try:
            recent_snapshots = db.query(Snapshot).order_by(Snapshot.created_at.desc()).limit(5).all()
            
            click.echo("📸 Recent Snapshots:")
            for snapshot in recent_snapshots:
                click.echo(f"   {snapshot.dataset_id} - {snapshot.effective_from} ({snapshot.created_at})")
        
        finally:
            db.close()
    
    asyncio.run(show_status())


if __name__ == '__main__':
    cli()
