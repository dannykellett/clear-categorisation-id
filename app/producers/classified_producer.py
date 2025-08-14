"""
Kafka producer for classified text results.

Publishes classification results to the 'scraped-classified' topic
with proper message formatting and delivery guarantees.
"""

import logging
from typing import Any, Dict, Optional

from app.producers.base_producer import BaseKafkaProducer
from app.schemas import ClassifiedMessage, serialize_message
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class ClassifiedMessageProducer(BaseKafkaProducer):
    """
    Producer for classified text results.
    
    Publishes classification results to the configured output topic
    with proper schema validation and delivery guarantees.
    """
    
    def __init__(
        self,
        output_topic: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize classified message producer.
        
        Args:
            output_topic: Output topic name (defaults to KAFKA_OUT_TOPIC setting)
            **kwargs: Additional producer configuration
        """
        settings = get_settings()
        
        # Use provided value or fall back to settings
        self.output_topic = output_topic or settings.kafka_out_topic
        
        # Initialize base producer
        super().__init__(**kwargs)
        
        # Store settings for later use
        self.settings = settings
        
        logger.info(
            f"Initialized classified message producer for topic '{self.output_topic}'"
        )
    
    async def publish_message(self, message: ClassifiedMessage) -> Dict[str, Any]:
        """
        Publish a classified message to the output topic.
        
        Args:
            message: ClassifiedMessage instance to publish
            
        Returns:
            Delivery information from Kafka
            
        Raises:
            KafkaError: If message publishing fails
        """
        try:
            # Serialize message to JSON
            serialized_message = serialize_message(message)
            
            # Use article_id as the message key for partitioning
            # This ensures messages for the same article go to the same partition
            message_key = message.article_id
            
            logger.debug(
                f"Publishing classified message: id={message.id}, "
                f"article_id={message.article_id}, "
                f"classification_count={len(message.classification)}"
            )
            
            # Send message
            delivery_info = await self.send_message(
                topic=self.output_topic,
                value=serialized_message,
                key=message_key
            )
            
            logger.info(
                f"Published classified message for article {message.article_id} "
                f"to partition {delivery_info['partition']} at offset {delivery_info['offset']}"
            )
            
            return delivery_info
            
        except Exception as e:
            logger.error(
                f"Failed to publish classified message for article {message.article_id}: {e}"
            )
            raise
    
    async def publish_batch(self, messages: list[ClassifiedMessage]) -> list:
        """
        Publish multiple classified messages as a batch.
        
        Args:
            messages: List of ClassifiedMessage instances
            
        Returns:
            List of delivery results for each message
        """
        if not messages:
            return []
        
        logger.info(f"Publishing batch of {len(messages)} classified messages")
        
        # Convert to message format expected by base class
        kafka_messages = []
        for msg in messages:
            try:
                serialized = serialize_message(msg)
                kafka_messages.append({
                    'value': serialized,
                    'key': msg.article_id
                })
            except Exception as e:
                logger.error(f"Failed to serialize message for article {msg.article_id}: {e}")
                kafka_messages.append({
                    'value': None,
                    'key': msg.article_id,
                    'error': str(e)
                })
        
        # Send batch
        results = await self.send_batch(kafka_messages, self.output_topic)
        
        # Log batch results
        success_count = sum(1 for r in results if r.get('success', False))
        failed_count = len(results) - success_count
        
        logger.info(
            f"Batch publish completed: {success_count} successful, {failed_count} failed"
        )
        
        return results
    
    async def publish_with_retry(
        self,
        message: ClassifiedMessage,
        max_retries: int = 3,
        backoff_factor: float = 1.0
    ) -> Dict[str, Any]:
        """
        Publish a message with retry logic.
        
        Args:
            message: ClassifiedMessage to publish
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff factor
            
        Returns:
            Delivery information
            
        Raises:
            KafkaError: If all retry attempts fail
        """
        import asyncio
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                return await self.publish_message(message)
                
            except Exception as e:
                last_error = e
                
                if attempt < max_retries:
                    delay = backoff_factor * (2 ** attempt)
                    logger.warning(
                        f"Failed to publish message for article {message.article_id} "
                        f"(attempt {attempt + 1}/{max_retries + 1}). "
                        f"Retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Failed to publish message for article {message.article_id} "
                        f"after {max_retries + 1} attempts: {e}"
                    )
        
        # If we get here, all retries failed
        raise last_error
    
    def set_output_topic(self, topic: str) -> None:
        """
        Change the output topic.
        
        Args:
            topic: New output topic name
        """
        old_topic = self.output_topic
        self.output_topic = topic
        logger.info(f"Output topic changed from '{old_topic}' to '{topic}'")
    
    def get_topic_metrics(self) -> Dict[str, Any]:
        """
        Get topic-specific metrics.
        
        Returns:
            Dictionary with topic metrics
        """
        base_metrics = self.get_metrics()
        
        topic_metrics = {
            **base_metrics,
            'output_topic': self.output_topic,
            'messages_per_topic': {
                self.output_topic: self.messages_sent
            }
        }
        
        return topic_metrics
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on classified producer.
        
        Returns:
            Health check results including topic information
        """
        base_health = await super().health_check()
        
        health = {
            **base_health,
            'output_topic': self.output_topic,
            'topic_configured': bool(self.output_topic)
        }
        
        return health


# Convenience function for creating and managing producer
async def create_and_start_producer(
    output_topic: Optional[str] = None,
    **kwargs
) -> ClassifiedMessageProducer:
    """
    Create and start a classified message producer.
    
    Args:
        output_topic: Output topic name (optional)
        **kwargs: Additional producer configuration
        
    Returns:
        Started ClassifiedMessageProducer instance
    """
    producer = ClassifiedMessageProducer(
        output_topic=output_topic,
        **kwargs
    )
    
    await producer.start()
    
    return producer