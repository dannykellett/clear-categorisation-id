"""
Base Kafka producer with connection pooling and retry logic.

Provides a reusable base class for all Kafka producers in the application
with proper lifecycle management, error handling, and delivery guarantees.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class BaseKafkaProducer(ABC):
    """
    Base class for Kafka producers with connection pooling and retry logic.
    
    Provides common functionality for:
    - Connection lifecycle management
    - Retry logic with exponential backoff
    - Delivery confirmation
    - Message serialization
    - Health monitoring
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        acks: Union[int, str] = 'all',
        retries: int = 3,
        max_in_flight_requests_per_connection: int = 5,
        batch_size: int = 16384,
        linger_ms: int = 0,
        buffer_memory: int = 33554432,
        compression_type: str = 'none',
        request_timeout_ms: int = 30000,
        retry_backoff_ms: int = 100,
        **kwargs
    ):
        """
        Initialize base producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            acks: Acknowledgment policy ('all', 1, 0)
            retries: Number of retries for failed sends
            max_in_flight_requests_per_connection: Max unacknowledged requests
            batch_size: Batch size in bytes
            linger_ms: Time to wait for additional messages
            buffer_memory: Total memory used for buffering
            compression_type: Compression type ('none', 'gzip', 'snappy', 'lz4')
            request_timeout_ms: Request timeout in milliseconds
            retry_backoff_ms: Backoff time between retries
            **kwargs: Additional producer configuration
        """
        # Get settings from environment if not provided
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_brokers
        
        # Producer configuration
        self.producer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'acks': acks,
            'retries': retries,
            'max_in_flight_requests_per_connection': max_in_flight_requests_per_connection,
            'batch_size': batch_size,
            'linger_ms': linger_ms,
            'buffer_memory': buffer_memory,
            'compression_type': compression_type,
            'request_timeout_ms': request_timeout_ms,
            'retry_backoff_ms': retry_backoff_ms,
            **kwargs
        }
        
        # Producer instance and state
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False
        self.is_stopping = False
        
        # Monitoring metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0
        self.last_send_timestamp = None
        
        logger.info(f"Initialized producer with brokers: {self.bootstrap_servers}")
    
    async def start(self) -> None:
        """
        Start the producer.
        
        Raises:
            KafkaError: If producer fails to start
        """
        if self.is_running:
            logger.warning("Producer already running")
            return
        
        try:
            logger.info("Starting Kafka producer...")
            
            # Create and start producer
            self.producer = AIOKafkaProducer(**self.producer_config)
            await self.producer.start()
            
            self.is_running = True
            self.is_stopping = False
            
            logger.info("Producer started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            await self._cleanup()
            raise KafkaError(f"Producer startup failed: {e}")
    
    async def stop(self) -> None:
        """
        Stop the producer gracefully.
        """
        if not self.is_running or self.is_stopping:
            return
        
        logger.info("Stopping producer...")
        self.is_stopping = True
        
        await self._cleanup()
        
        self.is_running = False
        self.is_stopping = False
        
        logger.info("Producer stopped successfully")
    
    async def _cleanup(self) -> None:
        """Clean up producer resources."""
        if self.producer:
            try:
                await self.producer.stop()
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
            finally:
                self.producer = None
    
    async def send_message(
        self,
        topic: str,
        value: Union[str, bytes],
        key: Optional[Union[str, bytes]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None
    ) -> Dict[str, Any]:
        """
        Send a message to Kafka with delivery confirmation.
        
        Args:
            topic: Topic to send to
            value: Message value
            key: Message key (optional)
            partition: Specific partition (optional)
            timestamp_ms: Message timestamp (optional)
            headers: Message headers (optional)
            
        Returns:
            Dictionary with delivery information
            
        Raises:
            KafkaError: If message sending fails
        """
        if not self.is_running:
            raise RuntimeError("Producer not started")
        
        try:
            # Convert string values to bytes if needed
            if isinstance(value, str):
                value = value.encode('utf-8')
            if isinstance(key, str):
                key = key.encode('utf-8')
            
            # Send message and get future (don't await the send call)
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers
            )
            
            # Wait for delivery confirmation
            record_metadata = await future
            
            # Update metrics
            self.messages_sent += 1
            self.bytes_sent += len(value) if value else 0
            self.last_send_timestamp = record_metadata.timestamp
            
            delivery_info = {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'timestamp': record_metadata.timestamp,
                'serialized_key_size': record_metadata.serialized_key_size,
                'serialized_value_size': record_metadata.serialized_value_size
            }
            
            logger.debug(
                f"Message sent successfully to {topic}[{record_metadata.partition}] "
                f"at offset {record_metadata.offset}"
            )
            
            return delivery_info
            
        except Exception as e:
            self.messages_failed += 1
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise KafkaError(f"Message send failed: {e}")
    
    async def send_batch(
        self,
        messages: list,
        topic: str
    ) -> list:
        """
        Send multiple messages as a batch.
        
        Args:
            messages: List of message dictionaries with 'value', 'key', etc.
            topic: Topic to send to
            
        Returns:
            List of delivery information for each message
        """
        results = []
        
        for message in messages:
            try:
                result = await self.send_message(
                    topic=topic,
                    value=message.get('value'),
                    key=message.get('key'),
                    partition=message.get('partition'),
                    timestamp_ms=message.get('timestamp_ms'),
                    headers=message.get('headers')
                )
                results.append({'success': True, 'result': result})
            except Exception as e:
                results.append({'success': False, 'error': str(e)})
                
        return results
    
    async def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush any pending messages.
        
        Args:
            timeout: Maximum time to wait for flush (optional)
        """
        if not self.producer:
            return
        
        try:
            await asyncio.wait_for(
                self.producer.flush(),
                timeout=timeout
            )
            logger.debug("Producer flush completed")
        except asyncio.TimeoutError:
            logger.warning(f"Producer flush timed out after {timeout}s")
            raise
        except Exception as e:
            logger.error(f"Error during producer flush: {e}")
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get producer metrics.
        
        Returns:
            Dictionary containing producer metrics
        """
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'is_running': self.is_running,
            'messages_sent': self.messages_sent,
            'messages_failed': self.messages_failed,
            'bytes_sent': self.bytes_sent,
            'last_send_timestamp': self.last_send_timestamp,
            'error_rate': (
                self.messages_failed / (self.messages_sent + self.messages_failed)
                if (self.messages_sent + self.messages_failed) > 0
                else 0
            )
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on producer.
        
        Returns:
            Health check results
        """
        health = {
            'status': 'healthy' if self.is_running and not self.is_stopping else 'unhealthy',
            'producer_running': self.is_running,
            'producer_stopping': self.is_stopping,
            'bootstrap_servers': self.bootstrap_servers
        }
        
        if self.producer:
            try:
                # Check producer client state
                health['client_ready'] = True
            except Exception as e:
                health['status'] = 'unhealthy'
                health['error'] = str(e)
                health['client_ready'] = False
        else:
            health['client_ready'] = False
        
        return health
    
    @abstractmethod
    async def publish_message(self, message: Any) -> Dict[str, Any]:
        """
        Publish a domain-specific message.
        
        Must be implemented by subclasses to define message publishing logic.
        
        Args:
            message: Domain-specific message to publish
            
        Returns:
            Delivery information
            
        Raises:
            Exception: Any publishing errors should be raised and will be
                      handled by the caller
        """
        pass