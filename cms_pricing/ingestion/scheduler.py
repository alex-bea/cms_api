"""Automated ingestion scheduler and task management"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum
import structlog

from cms_pricing.database import SessionLocal
from cms_pricing.models.snapshots import Snapshot
from cms_pricing.ingestion.mpfs import MPFSIngester
from cms_pricing.ingestion.opps import OPPSIngester

logger = structlog.get_logger()


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class IngestionTask:
    """Represents an ingestion task"""
    task_id: str
    dataset_id: str
    year: int
    quarter: Optional[str]
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class IngestionScheduler:
    """Manages automated ingestion tasks"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.tasks: Dict[str, IngestionTask] = {}
        self.running = False
        self.ingesters = {
            "MPFS": MPFSIngester,
            "OPPS": OPPSIngester,
            # Add more ingesters as they're created
        }
    
    async def start(self):
        """Start the ingestion scheduler"""
        self.running = True
        logger.info("Starting ingestion scheduler")
        
        # Start background task processing
        asyncio.create_task(self._process_tasks())
        
        # Schedule periodic ingestion checks
        asyncio.create_task(self._schedule_periodic_ingestion())
    
    async def stop(self):
        """Stop the ingestion scheduler"""
        self.running = False
        logger.info("Stopping ingestion scheduler")
    
    async def _process_tasks(self):
        """Process pending ingestion tasks"""
        while self.running:
            try:
                # Get pending tasks
                pending_tasks = [task for task in self.tasks.values() if task.status == TaskStatus.PENDING]
                
                for task in pending_tasks:
                    await self._execute_task(task)
                
                # Sleep for a bit before checking again
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error("Error in task processing loop", error=str(e), exc_info=True)
                await asyncio.sleep(30)
    
    async def _execute_task(self, task: IngestionTask):
        """Execute a single ingestion task"""
        try:
            # Mark task as running
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            
            logger.info("Starting ingestion task", task_id=task.task_id, dataset_id=task.dataset_id)
            
            # Get the appropriate ingester
            ingester_class = self.ingesters.get(task.dataset_id)
            if not ingester_class:
                raise ValueError(f"No ingester found for dataset {task.dataset_id}")
            
            # Create ingester and run ingestion
            ingester = ingester_class(self.data_dir)
            result = await ingester.ingest(task.year, task.quarter)
            
            # Mark task as completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.result = result
            
            # Store snapshot in database
            await self._store_snapshot(result)
            
            logger.info(
                "Ingestion task completed",
                task_id=task.task_id,
                dataset_id=task.dataset_id,
                digest=result.get('digest')
            )
            
        except Exception as e:
            # Mark task as failed
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.utcnow()
            task.error_message = str(e)
            
            logger.error(
                "Ingestion task failed",
                task_id=task.task_id,
                dataset_id=task.dataset_id,
                error=str(e),
                exc_info=True
            )
    
    async def _store_snapshot(self, result: Dict[str, Any]):
        """Store ingestion result as a snapshot in the database"""
        try:
            db = SessionLocal()
            
            # Create snapshot record
            snapshot = Snapshot(
                dataset_id=result['dataset_id'],
                effective_from=datetime.strptime(result['manifest']['files'][0]['effective_from'], '%Y-%m-%d').date(),
                effective_to=None,  # Could be calculated from manifest
                digest=result['digest'],
                source_url=result['manifest'].get('source_url'),
                manifest_json=json.dumps(result['manifest']),
                created_at=datetime.utcnow().date()
            )
            
            db.add(snapshot)
            db.commit()
            
            logger.info("Stored snapshot in database", dataset_id=result['dataset_id'], digest=result['digest'])
            
        except Exception as e:
            logger.error("Failed to store snapshot", error=str(e), exc_info=True)
        finally:
            db.close()
    
    async def _schedule_periodic_ingestion(self):
        """Schedule periodic ingestion tasks"""
        while self.running:
            try:
                # Check for new data every hour
                await asyncio.sleep(3600)
                
                if not self.running:
                    break
                
                # Schedule ingestion for current year/quarter
                current_date = datetime.utcnow()
                current_year = current_date.year
                current_quarter = str((current_date.month - 1) // 3 + 1)
                
                # Schedule MPFS (annual)
                await self.schedule_ingestion("MPFS", current_year, None)
                
                # Schedule OPPS (quarterly)
                await self.schedule_ingestion("OPPS", current_year, current_quarter)
                
                logger.info("Scheduled periodic ingestion", year=current_year, quarter=current_quarter)
                
            except Exception as e:
                logger.error("Error in periodic scheduling", error=str(e), exc_info=True)
                await asyncio.sleep(3600)
    
    async def schedule_ingestion(self, dataset_id: str, year: int, quarter: Optional[str] = None) -> str:
        """Schedule a new ingestion task"""
        task_id = f"{dataset_id}_{year}_{quarter or 'annual'}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Check if task already exists
        existing_task = self.tasks.get(task_id)
        if existing_task and existing_task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]:
            logger.info("Task already exists", task_id=task_id)
            return task_id
        
        # Create new task
        task = IngestionTask(
            task_id=task_id,
            dataset_id=dataset_id,
            year=year,
            quarter=quarter,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow()
        )
        
        self.tasks[task_id] = task
        
        logger.info("Scheduled ingestion task", task_id=task_id, dataset_id=dataset_id, year=year, quarter=quarter)
        
        return task_id
    
    def get_task_status(self, task_id: str) -> Optional[IngestionTask]:
        """Get the status of a specific task"""
        return self.tasks.get(task_id)
    
    def list_tasks(self, status: Optional[TaskStatus] = None) -> List[IngestionTask]:
        """List all tasks, optionally filtered by status"""
        tasks = list(self.tasks.values())
        
        if status:
            tasks = [task for task in tasks if task.status == status]
        
        return sorted(tasks, key=lambda t: t.created_at, reverse=True)
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task"""
        task = self.tasks.get(task_id)
        
        if not task:
            return False
        
        if task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            logger.info("Cancelled task", task_id=task_id)
            return True
        
        return False
    
    async def retry_failed_task(self, task_id: str) -> bool:
        """Retry a failed task"""
        task = self.tasks.get(task_id)
        
        if not task or task.status != TaskStatus.FAILED:
            return False
        
        # Reset task status
        task.status = TaskStatus.PENDING
        task.started_at = None
        task.completed_at = None
        task.error_message = None
        task.result = None
        
        logger.info("Retrying failed task", task_id=task_id)
        return True


# Global scheduler instance
scheduler = IngestionScheduler()
