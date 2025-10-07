"""
CLI commands for geography data ingestion

Provides command-line interface for manual geography data ingestion
and monitoring.
"""

import asyncio
import click
from datetime import datetime
from typing import Optional

from cms_pricing.ingestion.geography import GeographyIngester
from cms_pricing.ingestion.scheduler import IngestionScheduler
from cms_pricing.ingestion.geography_notifications import geography_notifications
import structlog

logger = structlog.get_logger()


@click.group()
def geography():
    """Geography data ingestion commands"""
    pass


@geography.command()
@click.option('--year', default=None, type=int, help='Year to ingest (defaults to current year)')
@click.option('--force', is_flag=True, help='Force ingestion even if no changes detected')
@click.option('--dry-run', is_flag=True, help='Check for changes without downloading')
async def ingest(year: Optional[int], force: bool, dry_run: bool):
    """Ingest geography data from CMS"""
    
    if year is None:
        year = datetime.now().year
    
    click.echo(f"Starting geography data ingestion for year {year}")
    
    try:
        ingester = GeographyIngester("./data")
        
        if dry_run:
            click.echo("Dry run mode - checking for changes only")
            # Check for changes without downloading
            geography_url = ingester.downloader.cms_urls["geography"]["zip_code_carrier_locality"]
            last_etag, last_modified = await ingester._get_last_file_metadata(geography_url)
            
            change_check = await ingester.downloader.check_file_changes(
                geography_url, last_etag, last_modified
            )
            
            if change_check["has_changes"]:
                click.echo("✅ Changes detected - ingestion would proceed")
                click.echo(f"   ETag changed: {change_check.get('etag_changed', False)}")
                click.echo(f"   Last-Modified changed: {change_check.get('modified_changed', False)}")
            else:
                click.echo("❌ No changes detected - ingestion would be skipped")
        else:
            # Perform actual ingestion
            result = await ingester.ingest(year)
            
            if result.get("success"):
                click.echo("✅ Geography data ingestion completed successfully")
                click.echo(f"   Dataset digest: {result.get('digest', 'unknown')}")
                click.echo(f"   Records processed: {result.get('record_count', 'unknown')}")
            else:
                click.echo("❌ Geography data ingestion failed")
                click.echo(f"   Error: {result.get('error', 'unknown')}")
                
    except Exception as e:
        click.echo(f"❌ Error during geography ingestion: {e}")
        logger.error("Geography ingestion failed", error=str(e), exc_info=True)


@geography.command()
@click.option('--limit', default=20, type=int, help='Number of recent notifications to show')
def notifications(limit: int):
    """Show recent geography notifications"""
    
    recent_notifications = geography_notifications.get_recent_notifications(limit)
    
    if not recent_notifications:
        click.echo("No recent notifications")
        return
    
    click.echo(f"Recent geography notifications (last {limit}):")
    click.echo("=" * 60)
    
    for notification in recent_notifications:
        timestamp = notification.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        level_icon = {
            "info": "ℹ️",
            "warning": "⚠️", 
            "error": "❌",
            "success": "✅"
        }.get(notification.level.value, "📝")
        
        click.echo(f"{level_icon} [{timestamp}] {notification.title}")
        click.echo(f"   {notification.message}")
        
        if notification.metadata:
            click.echo(f"   Metadata: {notification.metadata}")
        click.echo()


@geography.command()
@click.option('--type', 'notification_type', help='Filter by notification type')
def list_notifications(notification_type: Optional[str]):
    """List geography notifications by type"""
    
    if notification_type:
        notifications = geography_notifications.get_notifications_by_type(notification_type)
        click.echo(f"Notifications of type '{notification_type}':")
    else:
        notifications = geography_notifications.notifications
        click.echo("All geography notifications:")
    
    if not notifications:
        click.echo("No notifications found")
        return
    
    click.echo("=" * 60)
    
    for notification in notifications:
        timestamp = notification.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        level_icon = {
            "info": "ℹ️",
            "warning": "⚠️", 
            "error": "❌",
            "success": "✅"
        }.get(notification.level.value, "📝")
        
        click.echo(f"{level_icon} [{timestamp}] {notification.title}")
        click.echo(f"   {notification.message}")
        click.echo()


@geography.command()
def status():
    """Show geography ingestion status"""
    
    click.echo("Geography Data Ingestion Status")
    click.echo("=" * 40)
    
    # Check scheduler status
    try:
        scheduler = IngestionScheduler("./data")
        click.echo(f"Scheduler running: {scheduler.running}")
        click.echo(f"Active tasks: {len([t for t in scheduler.tasks.values() if t.status.value == 'running'])}")
        click.echo(f"Pending tasks: {len([t for t in scheduler.tasks.values() if t.status.value == 'pending'])}")
    except Exception as e:
        click.echo(f"Error checking scheduler status: {e}")
    
    # Show recent notifications summary
    recent_notifications = geography_notifications.get_recent_notifications(5)
    if recent_notifications:
        click.echo("\nRecent activity:")
        for notification in recent_notifications:
            timestamp = notification.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            level_icon = {
                "info": "ℹ️",
                "warning": "⚠️", 
                "error": "❌",
                "success": "✅"
            }.get(notification.level.value, "📝")
            click.echo(f"  {level_icon} [{timestamp}] {notification.title}")


@geography.command()
@click.option('--year', default=None, type=int, help='Year to check (defaults to current year)')
async def check_changes(year: Optional[int]):
    """Check for changes in geography data without downloading"""
    
    if year is None:
        year = datetime.now().year
    
    click.echo(f"Checking for geography data changes for year {year}")
    
    try:
        ingester = GeographyIngester("./data")
        geography_url = ingester.downloader.cms_urls["geography"]["zip_code_carrier_locality"]
        
        click.echo(f"Checking URL: {geography_url}")
        
        last_etag, last_modified = await ingester._get_last_file_metadata(geography_url)
        
        change_check = await ingester.downloader.check_file_changes(
            geography_url, last_etag, last_modified
        )
        
        click.echo("Change detection results:")
        click.echo(f"  Has changes: {change_check['has_changes']}")
        click.echo(f"  Current ETag: {change_check.get('etag', 'None')}")
        click.echo(f"  Current Last-Modified: {change_check.get('last_modified', 'None')}")
        click.echo(f"  ETag changed: {change_check.get('etag_changed', False)}")
        click.echo(f"  Last-Modified changed: {change_check.get('modified_changed', False)}")
        
        if change_check.get('error'):
            click.echo(f"  Error: {change_check['error']}")
            
    except Exception as e:
        click.echo(f"❌ Error checking for changes: {e}")
        logger.error("Change check failed", error=str(e), exc_info=True)


# Make the commands available for async execution
def run_async_command(coro):
    """Helper to run async commands"""
    return asyncio.run(coro)


# Add async command wrappers
@click.command()
@click.pass_context
def ingest_sync(ctx):
    """Sync wrapper for ingest command"""
    run_async_command(ingest.callback(**ctx.params))


if __name__ == "__main__":
    geography()



