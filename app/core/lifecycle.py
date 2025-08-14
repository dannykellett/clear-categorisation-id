"""
Graceful shutdown handling for Kafka consumer lifecycle management.

Provides signal handling and coordinated shutdown for consumer services.
"""

import asyncio
import signal
import logging
from typing import Optional, List, Callable, Awaitable
from contextlib import asynccontextmanager

from app.core.kafka_logging import consumer_logger

logger = logging.getLogger(__name__)


class GracefulShutdownManager:
    """
    Manages graceful shutdown of Kafka consumers and related services.
    
    Provides signal handling, shutdown coordination, and cleanup orchestration
    for the Kafka message processing pipeline.
    """
    
    def __init__(self, shutdown_timeout: float = 30.0):
        self.shutdown_timeout = shutdown_timeout
        self.shutdown_event = asyncio.Event()
        self.cleanup_tasks: List[Callable[[], Awaitable[None]]] = []
        self._shutdown_initiated = False
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        logger.info(f"Graceful shutdown manager initialized with {shutdown_timeout}s timeout")
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        try:
            # Handle common shutdown signals
            for sig in [signal.SIGTERM, signal.SIGINT]:
                signal.signal(sig, self._signal_handler)
            
            logger.info("Signal handlers registered for SIGTERM and SIGINT")
            
        except Exception as e:
            logger.warning(f"Failed to setup signal handlers: {e}")
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received {signal_name} signal, initiating graceful shutdown")
        
        # Trigger shutdown in async context
        asyncio.create_task(self.initiate_shutdown(f"signal_{signal_name}"))
    
    async def initiate_shutdown(self, reason: str = "manual") -> None:
        """
        Initiate graceful shutdown process.
        
        Args:
            reason: Reason for shutdown (for logging)
        """
        if self._shutdown_initiated:
            logger.warning("Shutdown already initiated, ignoring duplicate request")
            return
        
        self._shutdown_initiated = True
        
        logger.info(f"Initiating graceful shutdown: {reason}")
        
        # Log shutdown initiation
        consumer_logger.log_consumer_event(
            event_type="shutdown_initiated",
            consumer_group="all",
            topic="all",
            details={
                "reason": reason,
                "timeout": self.shutdown_timeout
            }
        )
        
        # Signal all waiting tasks
        self.shutdown_event.set()
    
    def add_cleanup_task(self, cleanup_func: Callable[[], Awaitable[None]]) -> None:
        """
        Add a cleanup task to be executed during shutdown.
        
        Args:
            cleanup_func: Async function to call during cleanup
        """
        self.cleanup_tasks.append(cleanup_func)
        logger.debug(f"Added cleanup task: {cleanup_func.__name__}")
    
    async def execute_cleanup(self) -> None:
        """Execute all registered cleanup tasks."""
        if not self.cleanup_tasks:
            logger.info("No cleanup tasks to execute")
            return
        
        logger.info(f"Executing {len(self.cleanup_tasks)} cleanup tasks")
        
        cleanup_results = []
        for i, cleanup_task in enumerate(self.cleanup_tasks):
            try:
                logger.debug(f"Executing cleanup task {i+1}/{len(self.cleanup_tasks)}: {cleanup_task.__name__}")
                
                # Execute with timeout
                await asyncio.wait_for(
                    cleanup_task(),
                    timeout=self.shutdown_timeout / len(self.cleanup_tasks)
                )
                
                cleanup_results.append({"task": cleanup_task.__name__, "status": "success"})
                logger.debug(f"Cleanup task {cleanup_task.__name__} completed successfully")
                
            except asyncio.TimeoutError:
                error_msg = f"Cleanup task {cleanup_task.__name__} timed out"
                logger.error(error_msg)
                cleanup_results.append({"task": cleanup_task.__name__, "status": "timeout"})
                
            except Exception as e:
                error_msg = f"Cleanup task {cleanup_task.__name__} failed: {e}"
                logger.error(error_msg)
                cleanup_results.append({"task": cleanup_task.__name__, "status": "failed", "error": str(e)})
        
        # Log cleanup summary
        successful_tasks = sum(1 for result in cleanup_results if result["status"] == "success")
        consumer_logger.log_consumer_event(
            event_type="cleanup_completed",
            consumer_group="all",
            topic="all",
            details={
                "total_tasks": len(self.cleanup_tasks),
                "successful_tasks": successful_tasks,
                "failed_tasks": len(self.cleanup_tasks) - successful_tasks,
                "cleanup_results": cleanup_results
            }
        )
        
        logger.info(f"Cleanup completed: {successful_tasks}/{len(self.cleanup_tasks)} tasks successful")
    
    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self.shutdown_event.wait()
    
    @asynccontextmanager
    async def managed_lifecycle(self):
        """
        Context manager for managed service lifecycle.
        
        Automatically handles cleanup when exiting the context.
        """
        try:
            yield self
        finally:
            if not self._shutdown_initiated:
                await self.initiate_shutdown("context_exit")
            
            await self.execute_cleanup()


class ConsumerLifecycleManager:
    """
    Specialized lifecycle manager for Kafka consumers.
    
    Provides consumer-specific shutdown handling and coordination.
    """
    
    def __init__(
        self,
        shutdown_timeout: float = 30.0,
        graceful_stop_timeout: float = 10.0
    ):
        self.shutdown_manager = GracefulShutdownManager(shutdown_timeout)
        self.graceful_stop_timeout = graceful_stop_timeout
        self.consumers: List = []
        self.producers: List = []
        
        logger.info(f"Consumer lifecycle manager initialized")
    
    def register_consumer(self, consumer) -> None:
        """
        Register a consumer for lifecycle management.
        
        Args:
            consumer: Consumer instance to manage
        """
        self.consumers.append(consumer)
        
        # Add consumer cleanup task
        async def cleanup_consumer():
            try:
                logger.info(f"Stopping consumer for topic {getattr(consumer, 'input_topic', 'unknown')}")
                
                # Stop consumer gracefully
                if hasattr(consumer, 'stop') and consumer.is_running:
                    await asyncio.wait_for(
                        consumer.stop(),
                        timeout=self.graceful_stop_timeout
                    )
                
                logger.info(f"Consumer stopped successfully")
                
            except asyncio.TimeoutError:
                logger.error(f"Consumer stop timed out after {self.graceful_stop_timeout}s")
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")
        
        self.shutdown_manager.add_cleanup_task(cleanup_consumer)
        logger.info(f"Registered consumer for lifecycle management")
    
    def register_producer(self, producer) -> None:
        """
        Register a producer for lifecycle management.
        
        Args:
            producer: Producer instance to manage
        """
        self.producers.append(producer)
        
        # Add producer cleanup task
        async def cleanup_producer():
            try:
                logger.info(f"Stopping producer for topic {getattr(producer, 'output_topic', 'unknown')}")
                
                # Flush pending messages
                if hasattr(producer, 'flush'):
                    await asyncio.wait_for(
                        producer.flush(),
                        timeout=5.0
                    )
                
                # Stop producer gracefully
                if hasattr(producer, 'stop') and producer.is_running:
                    await asyncio.wait_for(
                        producer.stop(),
                        timeout=self.graceful_stop_timeout
                    )
                
                logger.info(f"Producer stopped successfully")
                
            except asyncio.TimeoutError:
                logger.error(f"Producer stop timed out after {self.graceful_stop_timeout}s")
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
        
        self.shutdown_manager.add_cleanup_task(cleanup_producer)
        logger.info(f"Registered producer for lifecycle management")
    
    async def start_with_lifecycle_management(self, main_coroutine) -> None:
        """
        Start the main coroutine with lifecycle management.
        
        Args:
            main_coroutine: Main async function to run
        """
        try:
            async with self.shutdown_manager.managed_lifecycle() as manager:
                # Create main task
                main_task = asyncio.create_task(main_coroutine())
                
                # Create shutdown monitoring task
                shutdown_task = asyncio.create_task(manager.wait_for_shutdown())
                
                # Wait for either completion or shutdown
                done, pending = await asyncio.wait(
                    [main_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                # Check for exceptions in completed tasks
                for task in done:
                    if task.exception():
                        raise task.exception()
                
                logger.info("Main coroutine completed successfully")
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in main coroutine: {e}")
            raise
    
    async def run_consumer_with_lifecycle(self, consumer, shutdown_event: Optional[asyncio.Event] = None) -> None:
        """
        Run a consumer with full lifecycle management.
        
        Args:
            consumer: Consumer instance to run
            shutdown_event: Optional external shutdown event
        """
        # Register consumer for lifecycle management
        self.register_consumer(consumer)
        
        # Register producer if available
        if hasattr(consumer, 'classified_producer') and consumer.classified_producer:
            self.register_producer(consumer.classified_producer)
        
        # Use provided shutdown event or create our own
        if shutdown_event:
            # Monitor external shutdown event
            async def monitor_external_shutdown():
                await shutdown_event.wait()
                await self.shutdown_manager.initiate_shutdown("external_event")
            
            asyncio.create_task(monitor_external_shutdown())
        
        # Start consumer with lifecycle management
        await self.start_with_lifecycle_management(
            lambda: consumer.run_with_graceful_shutdown(self.shutdown_manager.shutdown_event)
        )


# Global lifecycle manager instance
_global_lifecycle_manager: Optional[ConsumerLifecycleManager] = None


def get_lifecycle_manager() -> ConsumerLifecycleManager:
    """Get or create global lifecycle manager."""
    global _global_lifecycle_manager
    
    if _global_lifecycle_manager is None:
        _global_lifecycle_manager = ConsumerLifecycleManager()
    
    return _global_lifecycle_manager


async def run_with_graceful_shutdown(main_coroutine, shutdown_timeout: float = 30.0) -> None:
    """
    Convenience function to run any coroutine with graceful shutdown handling.
    
    Args:
        main_coroutine: Main async function to run
        shutdown_timeout: Timeout for shutdown process
    """
    shutdown_manager = GracefulShutdownManager(shutdown_timeout)
    
    async with shutdown_manager.managed_lifecycle() as manager:
        # Create main task
        main_task = asyncio.create_task(main_coroutine)
        
        # Create shutdown monitoring task
        shutdown_task = asyncio.create_task(manager.wait_for_shutdown())
        
        # Wait for either completion or shutdown
        done, pending = await asyncio.wait(
            [main_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass