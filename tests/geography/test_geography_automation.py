"""
Tests for geography data automation

Tests the automatic ingestion, change detection, and notification systems
for geography data.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

scheduler_module = pytest.importorskip("cms_pricing.ingestion.scheduler")
geography_module = pytest.importorskip("cms_pricing.ingestion.geography")

from cms_pricing.ingestion.cms_downloader import CMSDownloader
from cms_pricing.ingestion.geography_notifications import GeographyNotificationService

IngestionScheduler = scheduler_module.IngestionScheduler
TaskStatus = scheduler_module.TaskStatus
GeographyIngester = geography_module.GeographyIngester


class TestGeographyAutomation:
    """Test geography data automation features"""
    
    @pytest.fixture
    def scheduler(self):
        """Create test scheduler"""
        return IngestionScheduler("./test_data")
    
    @pytest.fixture
    def notification_service(self):
        """Create test notification service"""
        return GeographyNotificationService()
    
    @pytest.mark.asyncio
    async def test_geography_scheduler_integration(self, scheduler):
        """Test geography integration with scheduler"""
        
        # Verify geography ingester is registered
        assert "GEOGRAPHY" in scheduler.ingesters
        assert scheduler.ingesters["GEOGRAPHY"] == GeographyIngester
    
    @pytest.mark.asyncio
    async def test_geography_change_detection(self):
        """Test geography change detection"""
        
        downloader = CMSDownloader()
        
        # Mock the change check response
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.headers = {
                "etag": '"new-etag-123"',
                "last-modified": "Mon, 01 Jan 2025 12:00:00 GMT"
            }
            mock_response.status_code = 200
            
            mock_client.return_value.__aenter__.return_value.head = AsyncMock(return_value=mock_response)
            
            # Test change detection
            result = await downloader.check_file_changes(
                "https://example.com/test.zip",
                last_etag='"old-etag-456"',
                last_modified="Sun, 31 Dec 2024 12:00:00 GMT"
            )
            
            assert result["has_changes"] is True
            assert result["etag_changed"] is True
            assert result["modified_changed"] is True
            assert result["etag"] == '"new-etag-123"'
    
    @pytest.mark.asyncio
    async def test_geography_no_changes_detection(self):
        """Test when no changes are detected"""
        
        downloader = CMSDownloader()
        
        # Mock the change check response with same ETag
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.headers = {
                "etag": '"same-etag-123"',
                "last-modified": "Mon, 01 Jan 2025 12:00:00 GMT"
            }
            mock_response.status_code = 200
            
            mock_client.return_value.__aenter__.return_value.head = AsyncMock(return_value=mock_response)
            
            # Test no change detection
            result = await downloader.check_file_changes(
                "https://example.com/test.zip",
                last_etag='"same-etag-123"',
                last_modified="Mon, 01 Jan 2025 12:00:00 GMT"
            )
            
            assert result["has_changes"] is False
            assert result["etag_changed"] is False
            assert result["modified_changed"] is False
    
    @pytest.mark.asyncio
    async def test_geography_notifications(self, notification_service):
        """Test geography notification system"""
        
        # Test ingestion started notification
        await notification_service.notify_geography_ingestion_started(2025, "scheduled")
        
        notifications = notification_service.get_recent_notifications(1)
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.title == "Geography Data Ingestion Started"
        assert notification.metadata["year"] == 2025
        assert notification.metadata["reason"] == "scheduled"
        assert notification.metadata["type"] == "ingestion_started"
    
    @pytest.mark.asyncio
    async def test_geography_ingestion_completed_notification(self, notification_service):
        """Test ingestion completed notification"""
        
        result = {
            "success": True,
            "digest": "test-digest-123",
            "record_count": 1000
        }
        
        await notification_service.notify_geography_ingestion_completed(2025, result)
        
        notifications = notification_service.get_recent_notifications(1)
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.title == "Geography Data Ingestion Completed"
        assert notification.level.value == "success"
        assert notification.metadata["success"] is True
        assert notification.metadata["result"] == result
    
    @pytest.mark.asyncio
    async def test_geography_changes_detected_notification(self, notification_service):
        """Test changes detected notification"""
        
        changes = {
            "etag_changed": True,
            "modified_changed": False,
            "etag": '"new-etag-123"'
        }
        
        await notification_service.notify_geography_changes_detected(
            "https://example.com/test.zip", changes
        )
        
        notifications = notification_service.get_recent_notifications(1)
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.title == "Geography Data Changes Detected"
        assert notification.metadata["url"] == "https://example.com/test.zip"
        assert notification.metadata["changes"] == changes
    
    @pytest.mark.asyncio
    async def test_geography_error_notification(self, notification_service):
        """Test error notification"""
        
        context = {
            "year": 2025,
            "operation": "download"
        }
        
        await notification_service.notify_geography_error("Download failed", context)
        
        notifications = notification_service.get_recent_notifications(1)
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.title == "Geography Data Error"
        assert notification.level.value == "error"
        assert notification.metadata["error"] == "Download failed"
        assert notification.metadata["context"] == context
    
    @pytest.mark.asyncio
    async def test_geography_scheduler_monthly_check(self, scheduler):
        """Test monthly geography check logic"""
        
        # Mock the change detection
        with patch.object(scheduler, '_check_geography_changes', return_value=True):
            await scheduler._schedule_geography_ingestion(2025)
            
            # Verify task was scheduled
            geography_tasks = [t for t in scheduler.tasks.values() if t.dataset_id == "GEOGRAPHY"]
            assert len(geography_tasks) > 0
            
            task = geography_tasks[0]
            assert task.year == 2025
            assert task.quarter is None
            assert task.status == TaskStatus.PENDING
    
    @pytest.mark.asyncio
    async def test_geography_scheduler_no_changes_skip(self, scheduler):
        """Test scheduler skips ingestion when no changes"""
        
        # Mock the change detection to return False
        with patch.object(scheduler, '_check_geography_changes', return_value=False):
            initial_task_count = len(scheduler.tasks)
            
            await scheduler._schedule_geography_ingestion(2025)
            
            # Verify no new task was created
            assert len(scheduler.tasks) == initial_task_count
    
    @pytest.mark.asyncio
    async def test_geography_ingester_with_change_detection(self):
        """Test geography ingester with change detection"""
        
        ingester = GeographyIngester("./test_data")
        
        # Mock the change detection to return no changes
        with patch.object(ingester.downloader, 'check_file_changes', return_value={"has_changes": False}):
            result = await ingester.fetch_data(2025)
            
            assert result["skipped"] is True
            assert result["reason"] == "no_changes"
    
    @pytest.mark.asyncio
    async def test_geography_ingester_with_changes_detected(self):
        """Test geography ingester when changes are detected"""
        
        ingester = GeographyIngester("./test_data")
        
        # Mock the change detection to return changes
        change_result = {
            "has_changes": True,
            "etag_changed": True,
            "modified_changed": False,
            "etag": '"new-etag-123"'
        }
        
        with patch.object(ingester.downloader, 'check_file_changes', return_value=change_result):
            with patch.object(ingester.downloader, 'download_dataset', return_value={"success": True, "files": []}):
                result = await ingester.fetch_data(2025)
                
                assert "skipped" not in result
                assert result["success"] is True
    
    def test_notification_filtering_by_type(self, notification_service):
        """Test filtering notifications by type"""
        
        # Add different types of notifications
        asyncio.run(notification_service.notify_geography_ingestion_started(2025, "manual"))
        asyncio.run(notification_service.notify_geography_changes_detected("test.url", {}))
        asyncio.run(notification_service.notify_geography_error("test error", {}))
        
        # Test filtering by type
        ingestion_notifications = notification_service.get_notifications_by_type("ingestion_started")
        assert len(ingestion_notifications) == 1
        
        change_notifications = notification_service.get_notifications_by_type("changes_detected")
        assert len(change_notifications) == 1
        
        error_notifications = notification_service.get_notifications_by_type("error")
        assert len(error_notifications) == 1


class TestGeographyCLI:
    """Test geography CLI commands"""
    
    @pytest.mark.asyncio
    async def test_ingest_command_dry_run(self):
        """Test ingest command in dry run mode"""
        
        from cms_pricing.cli.geography import ingest
        
        # Mock the ingester
        with patch('cms_pricing.cli.geography.GeographyIngester') as mock_ingester_class:
            mock_ingester = Mock()
            mock_ingester_class.return_value = mock_ingester
            
            # Mock change detection
            mock_ingester.downloader.cms_urls = {
                "geography": {"zip_code_carrier_locality": "https://test.url"}
            }
            mock_ingester._get_last_file_metadata = AsyncMock(return_value=(None, None))
            mock_ingester.downloader.check_file_changes = AsyncMock(return_value={
                "has_changes": True,
                "etag_changed": True,
                "modified_changed": False
            })
            
            # Test dry run
            await ingest.callback(year=2025, force=False, dry_run=True)
            
            # Verify change detection was called
            mock_ingester.downloader.check_file_changes.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
