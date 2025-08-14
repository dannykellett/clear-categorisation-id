import asyncio
from typing import Optional
from datetime import datetime, timezone
from app.services.taxonomy import taxonomy_service
from app.core.logging import get_logger, log_with_context
from app.core.config import config

logger = get_logger(__name__)


class TaxonomyScheduler:
    """Background scheduler for taxonomy synchronization"""
    
    def __init__(self):
        self.sync_interval = getattr(config, 'taxonomy_sync_interval', 300)  # 5 minutes default
        self._task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self):
        """Start the background scheduler"""
        if self._running:
            log_with_context(logger, "warning", "Scheduler already running")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._schedule_loop())
        
        log_with_context(
            logger, "info", "Taxonomy scheduler started",
            sync_interval_seconds=self.sync_interval
        )
    
    async def stop(self):
        """Stop the background scheduler"""
        if not self._running:
            return
        
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        log_with_context(logger, "info", "Taxonomy scheduler stopped")
    
    async def _schedule_loop(self):
        """Main scheduler loop"""
        log_with_context(
            logger, "info", "Starting taxonomy sync scheduler loop",
            interval_seconds=self.sync_interval
        )
        
        # Initial sync on startup
        await self._perform_sync("startup")
        
        try:
            while self._running:
                await asyncio.sleep(self.sync_interval)
                
                if self._running:  # Check again after sleep
                    await self._perform_sync("scheduled")
                    
        except asyncio.CancelledError:
            log_with_context(logger, "info", "Scheduler loop cancelled")
            raise
        except Exception as e:
            log_with_context(
                logger, "error", "Unexpected error in scheduler loop",
                error=str(e),
                error_type=type(e).__name__
            )
            # Continue running despite errors
            if self._running:
                await asyncio.sleep(60)  # Wait 1 minute before retrying
                await self._schedule_loop()  # Restart loop
    
    async def _perform_sync(self, trigger_type: str):
        """Perform taxonomy synchronization with error handling"""
        sync_start = datetime.now(timezone.utc)
        
        log_with_context(
            logger, "info", "Starting scheduled taxonomy sync",
            trigger_type=trigger_type,
            started_at=sync_start.isoformat()
        )
        
        try:
            success, validation_result = await taxonomy_service.sync_taxonomy()
            
            sync_end = datetime.now(timezone.utc)
            duration_ms = (sync_end - sync_start).total_seconds() * 1000
            
            if success:
                current_version = taxonomy_service.get_current_version()
                version_string = current_version.version_string if current_version else None
                
                log_with_context(
                    logger, "info", "Scheduled taxonomy sync completed successfully",
                    trigger_type=trigger_type,
                    version=version_string,
                    duration_ms=round(duration_ms, 2),
                    warnings=len(validation_result.warnings)
                )
            else:
                log_with_context(
                    logger, "warning", "Scheduled taxonomy sync failed validation",
                    trigger_type=trigger_type,
                    error_count=len(validation_result.errors),
                    warning_count=len(validation_result.warnings),
                    duration_ms=round(duration_ms, 2),
                    errors=validation_result.errors[:3]  # Log first 3 errors
                )
                
                # Emit alert for failed validation
                await self._emit_sync_alert("validation_failed", validation_result.errors)
        
        except Exception as e:
            sync_end = datetime.now(timezone.utc)
            duration_ms = (sync_end - sync_start).total_seconds() * 1000
            
            log_with_context(
                logger, "error", "Scheduled taxonomy sync failed with exception",
                trigger_type=trigger_type,
                error=str(e),
                error_type=type(e).__name__,
                duration_ms=round(duration_ms, 2)
            )
            
            # Emit alert for sync failure
            await self._emit_sync_alert("sync_failed", [str(e)])
    
    async def _emit_sync_alert(self, alert_type: str, errors: list):
        """Emit alerts for sync failures (placeholder for monitoring integration)"""
        log_with_context(
            logger, "error", "Taxonomy sync alert",
            alert_type=alert_type,
            error_count=len(errors),
            errors=errors
        )
        
        # In a production system, this would integrate with:
        # - Prometheus metrics
        # - Slack/Teams notifications
        # - Email alerts
        # - PagerDuty/incident management
        
        # For now, we just log structured alerts
    
    def is_running(self) -> bool:
        """Check if scheduler is currently running"""
        return self._running


# Global instance
taxonomy_scheduler = TaxonomyScheduler()