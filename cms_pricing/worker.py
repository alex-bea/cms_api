"""Background worker for data ingestion and processing"""

import asyncio
import signal
import sys
from typing import Dict, Any

import structlog
from cms_pricing.config import settings
from cms_pricing.ingestion.mpfs import MPFSIngester

logger = structlog.get_logger()


class Worker:
    """Background worker for data processing tasks"""
    
    def __init__(self):
        self.running = True
        self.tasks: Dict[str, asyncio.Task] = {}
    
    async def start(self):
        """Start the worker"""
        logger.info("Starting CMS Pricing worker")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start background tasks
        self.tasks["data_ingestion"] = asyncio.create_task(self._data_ingestion_loop())
        
        # Wait for tasks to complete
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
    
    async def _data_ingestion_loop(self):
        """Background data ingestion loop"""
        logger.info("Starting data ingestion loop")
        
        while self.running:
            try:
                # Check for ingestion tasks
                # In a real implementation, this would poll a queue or database
                await self._process_ingestion_tasks()
                
                # Sleep for a while
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error("Error in data ingestion loop", error=str(e), exc_info=True)
                await asyncio.sleep(60)
    
    async def _process_ingestion_tasks(self):
        """Process pending ingestion tasks"""
        # TODO(alex, GH-432): Implement actual task processing
        # This would involve:
        # 1. Checking for pending ingestion tasks
        # 2. Running appropriate ingesters
        # 3. Updating task status
        
        logger.debug("Processing ingestion tasks")
        
        # Example: Ingest MPFS data for current year
        if settings.debug:  # Only in development
            ingester = MPFSIngester("./data")
            try:
                result = await ingester.ingest(2025)
                logger.info("MPFS ingestion completed", result=result)
            except Exception as e:
                logger.error("MPFS ingestion failed", error=str(e))
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal", signal=signum)
        self.running = False
        
        # Cancel all tasks
        for task_name, task in self.tasks.items():
            if not task.done():
                logger.info("Cancelling task", task_name=task_name)
                task.cancel()


async def main():
    """Main worker entry point"""
    worker = Worker()
    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker shutdown by user")
    except Exception as e:
        logger.error("Worker failed", error=str(e), exc_info=True)
        sys.exit(1)
