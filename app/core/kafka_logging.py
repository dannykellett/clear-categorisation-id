"""
Structured logging utilities for Kafka message processing.

Provides enhanced logging with message correlation IDs and processing context.
"""

import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import json

from app.core.logging import get_logger


class KafkaMessageLogger:
    """
    Enhanced logger for Kafka message processing with structured context.
    
    Provides consistent logging format for message processing events
    with correlation tracking and performance metrics.
    """
    
    def __init__(self, logger_name: str):
        self.logger = get_logger(logger_name)
        self._processing_start_times: Dict[str, float] = {}
    
    def log_message_start(
        self,
        message_id: str,
        article_id: str,
        source: str,
        text_length: int,
        topic: str,
        partition: int,
        offset: int,
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log the start of message processing.
        
        Args:
            message_id: Unique message identifier
            article_id: Article identifier for correlation
            source: Message source
            text_length: Length of text content
            topic: Kafka topic
            partition: Kafka partition
            offset: Kafka offset
            extra_context: Additional context to include
        """
        self._processing_start_times[message_id] = time.time()
        
        context = {
            "event": "message_processing_start",
            "message_id": message_id,
            "article_id": article_id,
            "source": source,
            "text_length": text_length,
            "kafka": {
                "topic": topic,
                "partition": partition,
                "offset": offset
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Starting processing for message {message_id} from {source}",
            extra={"kafka_context": context}
        )
    
    def log_message_success(
        self,
        message_id: str,
        article_id: str,
        classification_count: int,
        taxonomy_version: str,
        delivery_info: Dict[str, Any],
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log successful message processing.
        
        Args:
            message_id: Unique message identifier
            article_id: Article identifier for correlation
            classification_count: Number of classifications generated
            taxonomy_version: Taxonomy version used
            delivery_info: Kafka delivery information
            extra_context: Additional context to include
        """
        processing_time = None
        if message_id in self._processing_start_times:
            processing_time = time.time() - self._processing_start_times[message_id]
            del self._processing_start_times[message_id]
        
        context = {
            "event": "message_processing_success",
            "message_id": message_id,
            "article_id": article_id,
            "classification_count": classification_count,
            "taxonomy_version": taxonomy_version,
            "processing_time_ms": round(processing_time * 1000, 2) if processing_time else None,
            "delivery": {
                "topic": delivery_info.get("topic"),
                "partition": delivery_info.get("partition"),
                "offset": delivery_info.get("offset")
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Successfully processed message {message_id} for article {article_id} "
            f"in {context['processing_time_ms']}ms with {classification_count} categories",
            extra={"kafka_context": context}
        )
    
    def log_message_failure(
        self,
        message_id: str,
        article_id: Optional[str],
        error: Exception,
        stage: str,
        kafka_metadata: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log message processing failure.
        
        Args:
            message_id: Unique message identifier
            article_id: Article identifier for correlation (if available)
            error: Exception that occurred
            stage: Processing stage where error occurred
            kafka_metadata: Kafka message metadata
            extra_context: Additional context to include
        """
        processing_time = None
        if message_id in self._processing_start_times:
            processing_time = time.time() - self._processing_start_times[message_id]
            del self._processing_start_times[message_id]
        
        context = {
            "event": "message_processing_failure",
            "message_id": message_id,
            "article_id": article_id,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "stage": stage
            },
            "processing_time_ms": round(processing_time * 1000, 2) if processing_time else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if kafka_metadata:
            context["kafka"] = kafka_metadata
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.error(
            f"Failed to process message {message_id} at stage '{stage}': {error}",
            extra={"kafka_context": context}
        )
    
    def log_batch_start(
        self,
        batch_id: str,
        batch_size: int,
        topic: str,
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log the start of batch processing.
        
        Args:
            batch_id: Unique batch identifier
            batch_size: Number of messages in batch
            topic: Kafka topic
            extra_context: Additional context to include
        """
        self._processing_start_times[batch_id] = time.time()
        
        context = {
            "event": "batch_processing_start",
            "batch_id": batch_id,
            "batch_size": batch_size,
            "topic": topic,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Starting batch processing: {batch_size} messages",
            extra={"kafka_context": context}
        )
    
    def log_batch_complete(
        self,
        batch_id: str,
        successful_count: int,
        failed_count: int,
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log batch processing completion.
        
        Args:
            batch_id: Unique batch identifier
            successful_count: Number of successfully processed messages
            failed_count: Number of failed messages
            extra_context: Additional context to include
        """
        processing_time = None
        if batch_id in self._processing_start_times:
            processing_time = time.time() - self._processing_start_times[batch_id]
            del self._processing_start_times[batch_id]
        
        total_messages = successful_count + failed_count
        success_rate = (successful_count / total_messages * 100) if total_messages > 0 else 0
        
        context = {
            "event": "batch_processing_complete",
            "batch_id": batch_id,
            "total_messages": total_messages,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "success_rate_percent": round(success_rate, 2),
            "processing_time_ms": round(processing_time * 1000, 2) if processing_time else None,
            "avg_time_per_message_ms": round((processing_time * 1000) / total_messages, 2) if processing_time and total_messages > 0 else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Batch processing complete: {successful_count}/{total_messages} successful "
            f"({success_rate:.1f}%) in {context['processing_time_ms']}ms",
            extra={"kafka_context": context}
        )
    
    def log_consumer_event(
        self,
        event_type: str,
        consumer_group: str,
        topic: str,
        details: Dict[str, Any],
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log consumer lifecycle events.
        
        Args:
            event_type: Type of event (start, stop, rebalance, etc.)
            consumer_group: Consumer group ID
            topic: Kafka topic
            details: Event-specific details
            extra_context: Additional context to include
        """
        context = {
            "event": f"consumer_{event_type}",
            "consumer_group": consumer_group,
            "topic": topic,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Consumer {event_type}: {consumer_group} on topic {topic}",
            extra={"kafka_context": context}
        )
    
    def log_producer_event(
        self,
        event_type: str,
        topic: str,
        details: Dict[str, Any],
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log producer lifecycle events.
        
        Args:
            event_type: Type of event (start, stop, send, etc.)
            topic: Kafka topic
            details: Event-specific details
            extra_context: Additional context to include
        """
        context = {
            "event": f"producer_{event_type}",
            "topic": topic,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Producer {event_type}: topic {topic}",
            extra={"kafka_context": context}
        )
    
    def log_dlq_candidate(
        self,
        message_id: str,
        article_id: Optional[str],
        error_type: str,
        kafka_metadata: Dict[str, Any],
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log messages that are candidates for Dead Letter Queue.
        
        Args:
            message_id: Unique message identifier
            article_id: Article identifier for correlation
            error_type: Type of error that occurred
            kafka_metadata: Original Kafka message metadata
            extra_context: Additional context to include
        """
        context = {
            "event": "dlq_candidate",
            "message_id": message_id,
            "article_id": article_id,
            "error_type": error_type,
            "kafka": kafka_metadata,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.warning(
            f"DLQ candidate: {error_type} for message {message_id}",
            extra={"kafka_context": context}
        )
    
    def log_performance_metrics(
        self,
        component: str,
        metrics: Dict[str, Any],
        extra_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log performance metrics for monitoring.
        
        Args:
            component: Component name (consumer, producer, processor, etc.)
            metrics: Performance metrics
            extra_context: Additional context to include
        """
        context = {
            "event": "performance_metrics",
            "component": component,
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if extra_context:
            context.update(extra_context)
        
        self.logger.info(
            f"Performance metrics for {component}",
            extra={"kafka_context": context}
        )


# Create logger instances for different components
consumer_logger = KafkaMessageLogger("kafka.consumer")
producer_logger = KafkaMessageLogger("kafka.producer")
processor_logger = KafkaMessageLogger("kafka.processor")


def log_structured_event(
    logger: logging.Logger,
    level: str,
    event_type: str,
    message: str,
    context: Dict[str, Any]
) -> None:
    """
    Log a structured event with consistent formatting.
    
    Args:
        logger: Logger instance to use
        level: Log level (info, warning, error, etc.)
        event_type: Type of event for filtering
        message: Human-readable message
        context: Structured context data
    """
    enhanced_context = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **context
    }
    
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message, extra={"structured_context": enhanced_context})