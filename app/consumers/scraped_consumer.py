"""
Kafka consumer for scraped text messages.

Consumes messages from the 'scraped' topic, validates them, classifies the text,
and publishes results to the 'scraped-classified' topic.
"""

import asyncio
import logging
from typing import Optional

from aiokafka.structs import ConsumerRecord

from app.consumers.base_consumer import BaseKafkaConsumer
from app.schemas import (
    ScrapedMessage,
    MessageValidationError,
    validate_scraped_message,
    serialize_message
)
from app.core.config import get_settings
from app.engine.stream_processor import stream_processor
from app.producers import ClassifiedMessageProducer
from app.services.persistence import get_persistence_service
from app.core.kafka_logging import consumer_logger
from app.core.lifecycle import get_lifecycle_manager

logger = logging.getLogger(__name__)


class ScrapedMessageConsumer(BaseKafkaConsumer):
    """
    Consumer for scraped text messages.
    
    Processes messages from the 'scraped' topic, validates the schema,
    performs text classification, and publishes results to the output topic.
    """
    
    def __init__(
        self,
        input_topic: Optional[str] = None,
        group_id: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize scraped message consumer.
        
        Args:
            input_topic: Input topic name (defaults to KAFKA_IN_TOPIC setting)
            group_id: Consumer group ID (defaults to KAFKA_GROUP_ID setting)
            **kwargs: Additional consumer configuration
        """
        settings = get_settings()
        
        # Use provided values or fall back to settings
        self.input_topic = input_topic or settings.kafka_in_topic
        consumer_group_id = group_id or settings.kafka_group_id
        
        # Initialize base consumer with single topic
        super().__init__(
            topics=[self.input_topic],
            group_id=consumer_group_id,
            **kwargs
        )
        
        # Store settings for later use
        self.settings = settings
        
        # Initialize services
        self.stream_processor = stream_processor
        self.persistence_service = get_persistence_service()
        self.classified_producer = None
        
        logger.info(
            f"Initialized scraped message consumer for topic '{self.input_topic}' "
            f"with group '{consumer_group_id}'"
        )
    
    async def start(self) -> None:
        """
        Start the consumer and initialize dependencies.
        """
        await super().start()
        
        # Initialize and start the classified message producer
        self.classified_producer = ClassifiedMessageProducer()
        await self.classified_producer.start()
        
        # Log consumer startup event
        consumer_logger.log_consumer_event(
            event_type="start",
            consumer_group=self.group_id,
            topic=self.input_topic,
            details={
                "producer_output_topic": self.classified_producer.output_topic,
                "stream_processor_ready": True,
                "persistence_service_ready": True
            }
        )
        
        logger.info("Scraped message consumer started with classification pipeline")
    
    async def process_message(self, message: ConsumerRecord) -> None:
        """
        Process a single scraped message.
        
        Args:
            message: Kafka message containing scraped text
            
        Raises:
            MessageValidationError: If message validation fails
            Exception: If classification or publishing fails
        """
        try:
            # Validate message schema
            scraped_msg = validate_scraped_message(message.value)
            
            # Log message processing start with structured logging
            consumer_logger.log_message_start(
                message_id=scraped_msg.id,
                article_id=scraped_msg.article_id,
                source=scraped_msg.source,
                text_length=len(scraped_msg.text),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
            )
            
            logger.debug(
                f"Processing scraped message: id={scraped_msg.id}, "
                f"article_id={scraped_msg.article_id}, "
                f"source={scraped_msg.source}, "
                f"text_length={len(scraped_msg.text)}"
            )
            
            # Process message through classification pipeline
            classified_msg = await self.stream_processor.process_message(scraped_msg)
            
            # Persist to database with idempotent writes
            await self.persistence_service.persist_message_pair(scraped_msg, classified_msg)
            
            # Publish classified result to output topic
            delivery_info = await self.classified_producer.publish_message(classified_msg)
            
            # Log successful processing with structured logging
            consumer_logger.log_message_success(
                message_id=scraped_msg.id,
                article_id=scraped_msg.article_id,
                classification_count=len(classified_msg.classification),
                taxonomy_version=classified_msg.taxonomy_version,
                delivery_info=delivery_info,
                extra_context={
                    "provider": classified_msg.provider,
                    "model": classified_msg.model,
                    "usage_tokens": classified_msg.usage.total_tokens if classified_msg.usage else None
                }
            )
            
            # Enrich with processing metadata
            logger.info(
                f"Successfully processed article {scraped_msg.article_id}: "
                f"classified with {len(classified_msg.classification)} categories, "
                f"published to partition {delivery_info['partition']} at offset {delivery_info['offset']}, "
                f"using taxonomy {classified_msg.taxonomy_version}"
            )
            
        except MessageValidationError as e:
            # Log validation failure with structured logging
            consumer_logger.log_message_failure(
                message_id="unknown",
                article_id=None,
                error=e,
                stage="validation",
                kafka_metadata={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "value_preview": str(message.value)[:200] if message.value else None
                }
            )
            logger.error(f"Message validation failed: {e}")
            # Re-raise to trigger error handling
            raise
        
        except Exception as e:
            # Determine processing stage based on available data
            message_id = getattr(scraped_msg, 'id', 'unknown') if 'scraped_msg' in locals() else 'unknown'
            article_id = getattr(scraped_msg, 'article_id', None) if 'scraped_msg' in locals() else None
            
            # Determine stage based on what was successful
            stage = "unknown"
            if 'classified_msg' in locals():
                stage = "publishing"
            elif 'scraped_msg' in locals():
                stage = "classification"
            else:
                stage = "validation"
            
            # Log failure with structured logging
            consumer_logger.log_message_failure(
                message_id=message_id,
                article_id=article_id,
                error=e,
                stage=stage,
                kafka_metadata={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset
                }
            )
            
            logger.error(
                f"Failed to process scraped message id={message_id}: {e}",
                exc_info=True
            )
            raise
    
    async def handle_message_error(self, message: ConsumerRecord, error: Exception) -> None:
        """
        Handle message processing errors.
        
        Args:
            message: Message that failed processing
            error: Exception that occurred
        """
        # Log detailed error information with enriched metadata
        error_metadata = {
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp,
            'error_type': type(error).__name__,
            'error_message': str(error)
        }
        
        logger.error(
            f"Error processing message from topic {message.topic}, "
            f"partition {message.partition}, offset {message.offset}: {error}",
            extra={'kafka_error_metadata': error_metadata}
        )
        
        # For validation errors, log the raw message for debugging
        if isinstance(error, MessageValidationError):
            logger.error(f"Invalid message content: {message.value}")
        
        # Implement basic DLQ-like error tracking
        await self._track_error_for_dlq(message, error)
    
    async def _track_error_for_dlq(self, message: ConsumerRecord, error: Exception) -> None:
        """
        Track errors for potential Dead Letter Queue processing.
        
        Args:
            message: Failed message
            error: Exception that occurred
        """
        try:
            # For now, just log structured error information
            # In a production system, this could write to a DLQ topic or error tracking system
            error_record = {
                'original_topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'timestamp': message.timestamp,
                'key': message.key.decode('utf-8') if message.key else None,
                'value_preview': str(message.value)[:200] if message.value else None,
                'error_type': type(error).__name__,
                'error_message': str(error),
                'failed_at': asyncio.get_event_loop().time()
            }
            
            logger.warning(
                f"DLQ candidate: {error_record['error_type']} for message at "
                f"offset {error_record['offset']}",
                extra={'dlq_record': error_record}
            )
            
            # Log DLQ candidate with structured logging
            consumer_logger.log_dlq_candidate(
                message_id=error_record.get('key', 'unknown'),
                article_id=None,  # Not available from raw message
                error_type=error_record['error_type'],
                kafka_metadata={
                    'topic': error_record['original_topic'],
                    'partition': error_record['partition'],
                    'offset': error_record['offset'],
                    'timestamp': error_record['timestamp']
                }
            )
            
            # TODO: Consider implementing actual DLQ publishing
            # await self.publish_to_dlq_topic(error_record)
            
        except Exception as dlq_error:
            logger.error(f"Failed to track DLQ error: {dlq_error}")
    
    def set_classification_service(self, service):
        """
        Set the classification service (will be used in Task 4).
        
        Args:
            service: Classification service instance
        """
        self.classification_service = service
        logger.info("Classification service set")
    
    def set_producer(self, producer):
        """
        Set the message producer (will be used in Task 3).
        
        Args:
            producer: Message producer instance
        """
        self.producer = producer
        logger.info("Message producer set")
    
    async def run(self) -> None:
        """
        Run the consumer with proper lifecycle management.
        
        This is the main entry point for running the consumer.
        """
        try:
            await self.start()
            await self.consume_messages()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            # Log consumer shutdown event
            consumer_logger.log_consumer_event(
                event_type="stop",
                consumer_group=self.group_id,
                topic=self.input_topic,
                details={
                    "shutdown_reason": "normal",
                    "final_metrics": self.get_metrics()
                }
            )
            
            # Clean shutdown of producer
            if self.classified_producer:
                await self.classified_producer.stop()
            await self.stop()
    
    async def run_with_graceful_shutdown(self, shutdown_event: asyncio.Event) -> None:
        """
        Run consumer with graceful shutdown support.
        
        Args:
            shutdown_event: Event to signal shutdown
        """
        try:
            await self.start()
            
            # Create consumption task
            consume_task = asyncio.create_task(self.consume_messages())
            
            # Wait for either consumption to complete or shutdown signal
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            
            done, pending = await asyncio.wait(
                [consume_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Consumer shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during consumer execution: {e}")
            raise
        finally:
            # Log graceful shutdown event
            consumer_logger.log_consumer_event(
                event_type="graceful_shutdown",
                consumer_group=self.group_id,
                topic=self.input_topic,
                details={
                    "shutdown_reason": "graceful",
                    "final_metrics": self.get_metrics()
                }
            )
            
            # Clean shutdown of producer
            if self.classified_producer:
                await self.classified_producer.stop()
            await self.stop()


# Convenience function for running the consumer
async def create_and_run_consumer(
    input_topic: Optional[str] = None,
    group_id: Optional[str] = None,
    shutdown_event: Optional[asyncio.Event] = None
) -> None:
    """
    Create and run a scraped message consumer.
    
    Args:
        input_topic: Input topic name (optional)
        group_id: Consumer group ID (optional)
        shutdown_event: Event to signal shutdown (optional)
    """
    consumer = ScrapedMessageConsumer(
        input_topic=input_topic,
        group_id=group_id
    )
    
    if shutdown_event:
        await consumer.run_with_graceful_shutdown(shutdown_event)
    else:
        await consumer.run()


async def run_consumer_with_lifecycle_management(
    input_topic: Optional[str] = None,
    group_id: Optional[str] = None,
    shutdown_event: Optional[asyncio.Event] = None
) -> None:
    """
    Create and run a scraped message consumer with full lifecycle management.
    
    This function provides comprehensive lifecycle management including:
    - Signal handling for graceful shutdown
    - Coordinated cleanup of consumer and producer
    - Structured logging of lifecycle events
    - Automatic error recovery and reporting
    
    Args:
        input_topic: Input topic name (optional)
        group_id: Consumer group ID (optional)
        shutdown_event: External shutdown event (optional)
    """
    consumer = ScrapedMessageConsumer(
        input_topic=input_topic,
        group_id=group_id
    )
    
    # Get lifecycle manager and run with full management
    lifecycle_manager = get_lifecycle_manager()
    await lifecycle_manager.run_consumer_with_lifecycle(consumer, shutdown_event)