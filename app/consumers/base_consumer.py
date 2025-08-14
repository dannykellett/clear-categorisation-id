"""
Base Kafka consumer with connection management and error handling.

Provides a reusable base class for all Kafka consumers in the application
with proper lifecycle management, error handling, and monitoring.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError, ConsumerStoppedError
from aiokafka.structs import ConsumerRecord

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class BaseKafkaConsumer(ABC):
    """
    Base class for Kafka consumers with connection management and error handling.
    
    Provides common functionality for:
    - Connection lifecycle management
    - Error handling and retries
    - Graceful shutdown
    - Basic monitoring metrics
    """
    
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        max_poll_records: int = 500,
        session_timeout_ms: int = 30000,
        heartbeat_interval_ms: int = 10000,
        **kwargs
    ):
        """
        Initialize base consumer.
        
        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            bootstrap_servers: Kafka broker addresses (comma-separated)
            auto_offset_reset: Where to start consuming ('earliest', 'latest')
            enable_auto_commit: Whether to auto-commit offsets
            auto_commit_interval_ms: Auto-commit interval in milliseconds
            max_poll_records: Maximum records to return in a single poll
            session_timeout_ms: Session timeout in milliseconds
            heartbeat_interval_ms: Heartbeat interval in milliseconds
            **kwargs: Additional consumer configuration
        """
        self.topics = topics
        self.group_id = group_id
        
        # Get settings from environment if not provided
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_brokers
        
        # Consumer configuration
        self.consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': enable_auto_commit,
            'auto_commit_interval_ms': auto_commit_interval_ms,
            'max_poll_records': max_poll_records,
            'session_timeout_ms': session_timeout_ms,
            'heartbeat_interval_ms': heartbeat_interval_ms,
            **kwargs
        }
        
        # Consumer instance and state
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.is_running = False
        self.is_stopping = False
        
        # Monitoring metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_message_timestamp = None
        
        logger.info(f"Initialized consumer for topics {topics} with group {group_id}")
    
    async def start(self) -> None:
        """
        Start the consumer and subscribe to topics.
        
        Raises:
            KafkaError: If consumer fails to start
        """
        if self.is_running:
            logger.warning("Consumer already running")
            return
        
        try:
            logger.info(f"Starting consumer for topics {self.topics}")
            
            # Create and start consumer
            self.consumer = AIOKafkaConsumer(**self.consumer_config)
            await self.consumer.start()
            
            # Subscribe to topics
            self.consumer.subscribe(self.topics)
            
            self.is_running = True
            self.is_stopping = False
            
            logger.info(f"Consumer started successfully for topics {self.topics}")
            
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            await self._cleanup()
            raise KafkaError(f"Consumer startup failed: {e}")
    
    async def stop(self) -> None:
        """
        Stop the consumer gracefully.
        """
        if not self.is_running or self.is_stopping:
            return
        
        logger.info("Stopping consumer...")
        self.is_stopping = True
        
        await self._cleanup()
        
        self.is_running = False
        self.is_stopping = False
        
        logger.info("Consumer stopped successfully")
    
    async def _cleanup(self) -> None:
        """Clean up consumer resources."""
        if self.consumer:
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")
            finally:
                self.consumer = None
    
    async def consume_messages(self) -> None:
        """
        Main consumption loop.
        
        Continuously polls for messages and processes them using the
        abstract process_message method.
        """
        if not self.is_running:
            raise RuntimeError("Consumer not started")
        
        logger.info("Starting message consumption loop")
        
        try:
            async for message in self.consumer:
                if self.is_stopping:
                    logger.info("Consumer stopping, breaking from consumption loop")
                    break
                
                await self._handle_message(message)
                
        except ConsumerStoppedError:
            logger.info("Consumer stopped during message consumption")
        except Exception as e:
            logger.error(f"Error in consumption loop: {e}")
            raise
    
    async def _handle_message(self, message: ConsumerRecord) -> None:
        """
        Handle a single message with error handling and metrics.
        
        Args:
            message: Kafka message to process
        """
        try:
            # Update metrics
            self.last_message_timestamp = message.timestamp
            
            # Process message using subclass implementation
            await self.process_message(message)
            
            self.messages_processed += 1
            
            # Log periodically
            if self.messages_processed % 100 == 0:
                logger.info(f"Processed {self.messages_processed} messages")
                
        except Exception as e:
            self.messages_failed += 1
            logger.error(
                f"Failed to process message from topic {message.topic}, "
                f"partition {message.partition}, offset {message.offset}: {e}"
            )
            
            # Allow subclass to handle errors
            await self.handle_message_error(message, e)
    
    @abstractmethod
    async def process_message(self, message: ConsumerRecord) -> None:
        """
        Process a single Kafka message.
        
        Must be implemented by subclasses to define message processing logic.
        
        Args:
            message: Kafka message to process
            
        Raises:
            Exception: Any processing errors should be raised and will be
                      handled by the base class error handling
        """
        pass
    
    async def handle_message_error(self, message: ConsumerRecord, error: Exception) -> None:
        """
        Handle message processing errors.
        
        Default implementation logs the error. Subclasses can override
        to implement custom error handling (e.g., DLQ publishing).
        
        Args:
            message: Message that failed processing
            error: Exception that occurred during processing
        """
        logger.error(f"Message processing error: {error}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get consumer metrics.
        
        Returns:
            Dictionary containing consumer metrics
        """
        return {
            'topics': self.topics,
            'group_id': self.group_id,
            'is_running': self.is_running,
            'messages_processed': self.messages_processed,
            'messages_failed': self.messages_failed,
            'last_message_timestamp': self.last_message_timestamp,
            'error_rate': (
                self.messages_failed / (self.messages_processed + self.messages_failed)
                if (self.messages_processed + self.messages_failed) > 0
                else 0
            )
        }
    
    async def get_consumer_lag(self) -> Dict[str, int]:
        """
        Get consumer lag for subscribed topics.
        
        Returns:
            Dictionary mapping topic-partition to lag
        """
        if not self.consumer:
            return {}
        
        try:
            # Get assigned partitions
            partitions = self.consumer.assignment()
            if not partitions:
                return {}
            
            # Get high water marks (latest offsets)
            high_water_marks = await self.consumer.end_offsets(partitions)
            
            # Get current consumer positions
            lag_info = {}
            for tp in partitions:
                try:
                    current_offset = await self.consumer.position(tp)
                    high_water_mark = high_water_marks.get(tp, 0)
                    lag = high_water_mark - current_offset
                    lag_info[f"{tp.topic}-{tp.partition}"] = max(0, lag)
                except Exception as e:
                    logger.warning(f"Could not get lag for {tp}: {e}")
                    lag_info[f"{tp.topic}-{tp.partition}"] = -1
            
            return lag_info
            
        except Exception as e:
            logger.error(f"Error getting consumer lag: {e}")
            return {}
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on consumer.
        
        Returns:
            Health check results
        """
        health = {
            'status': 'healthy' if self.is_running and not self.is_stopping else 'unhealthy',
            'consumer_running': self.is_running,
            'consumer_stopping': self.is_stopping,
            'topics_subscribed': self.topics,
            'group_id': self.group_id
        }
        
        if self.consumer:
            try:
                # Check if consumer has assignment
                partitions = self.consumer.assignment()
                health['partitions_assigned'] = len(partitions)
                health['partition_details'] = [
                    f"{tp.topic}-{tp.partition}" for tp in partitions
                ]
            except Exception as e:
                health['status'] = 'unhealthy'
                health['error'] = str(e)
        
        return health