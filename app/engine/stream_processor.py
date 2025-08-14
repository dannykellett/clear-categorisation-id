"""
Kafka stream processing integration for text classification.

Extends the existing classification engine to handle batch message processing
from Kafka with proper schema validation and error handling.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

from app.engine.classifier import classification_engine
from app.schemas import ScrapedMessage, ClassifiedMessage, CategoryPath
from app.services.taxonomy import taxonomy_service
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class StreamProcessor:
    """
    Stream processor for Kafka message classification.
    
    Handles batch processing of scraped messages, classifies them using
    the existing classification engine, and prepares classified results
    for publishing to output topic.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.batch_size = getattr(self.settings, 'kafka_batch_size', 10)
        self.processing_timeout = getattr(self.settings, 'kafka_processing_timeout', 30.0)
        
        logger.info(
            f"Initialized stream processor with batch_size={self.batch_size}, "
            f"timeout={self.processing_timeout}s"
        )
    
    async def process_message(
        self,
        message: ScrapedMessage,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> ClassifiedMessage:
        """
        Process a single scraped message and return classified result.
        
        Args:
            message: ScrapedMessage to classify
            provider: Optional AI provider override
            model: Optional model override
            
        Returns:
            ClassifiedMessage with classification results
            
        Raises:
            ClassificationError: If classification fails
        """
        try:
            logger.debug(
                f"Processing message: id={message.id}, "
                f"article_id={message.article_id}, "
                f"text_length={len(message.text)}"
            )
            
            start_time = datetime.now(timezone.utc)
            
            # Classify the text using existing engine
            categories, usage = await classification_engine.classify_text(
                text=message.text,
                provider=provider,
                model=model
            )
            
            processed_ts = datetime.now(timezone.utc)
            
            # Get current taxonomy version
            taxonomy_version = taxonomy_service.get_current_version()
            version_string = taxonomy_version.version_string if taxonomy_version else "unknown"
            
            # Convert CategoryPath objects to match output schema
            classification_results = []
            for cat in categories:
                # Parse path into tiers (e.g., "technology/software/web-development")
                path_parts = cat.path.split('/')
                
                classification_result = CategoryPath(
                    tier_1=path_parts[0] if len(path_parts) > 0 else "",
                    tier_2=path_parts[1] if len(path_parts) > 1 else None,
                    tier_3=path_parts[2] if len(path_parts) > 2 else None,
                    tier_4=path_parts[3] if len(path_parts) > 3 else None,
                    confidence=cat.confidence,
                    reasoning=cat.reasoning
                )
                classification_results.append(classification_result)
            
            # Create classified message
            classified_message = ClassifiedMessage(
                id=message.id,
                article_id=message.article_id,
                classification=classification_results,
                provider=usage.provider,
                model=usage.model,
                usage={
                    "input_tokens": usage.input_tokens or 0,
                    "output_tokens": usage.output_tokens or 0,
                    "total_tokens": (usage.input_tokens or 0) + (usage.output_tokens or 0)
                },
                taxonomy_version=version_string,
                processed_ts=processed_ts
            )
            
            processing_time = (processed_ts - start_time).total_seconds()
            logger.info(
                f"Successfully processed message for article {message.article_id} "
                f"in {processing_time:.3f}s with {len(classification_results)} categories"
            )
            
            return classified_message
            
        except Exception as e:
            logger.error(
                f"Failed to process message for article {message.article_id}: {e}",
                exc_info=True
            )
            # Re-raise with more context
            raise ClassificationError(
                f"Classification failed for article {message.article_id}: {e}"
            ) from e
    
    async def process_batch(
        self,
        messages: List[ScrapedMessage],
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[List[ClassifiedMessage], List[Tuple[ScrapedMessage, Exception]]]:
        """
        Process a batch of scraped messages.
        
        Args:
            messages: List of ScrapedMessage instances to classify
            provider: Optional AI provider override
            model: Optional model override
            
        Returns:
            Tuple of (successful_results, failed_messages_with_errors)
        """
        if not messages:
            return [], []
        
        logger.info(f"Processing batch of {len(messages)} messages")
        
        start_time = datetime.now(timezone.utc)
        successful_results = []
        failed_messages = []
        
        # Process messages concurrently with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.batch_size)
        
        async def process_single(msg: ScrapedMessage):
            async with semaphore:
                try:
                    result = await asyncio.wait_for(
                        self.process_message(msg, provider, model),
                        timeout=self.processing_timeout
                    )
                    return msg, result, None
                except Exception as e:
                    return msg, None, e
        
        # Execute all tasks concurrently
        tasks = [process_single(msg) for msg in messages]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=False)
            
            # Separate successful and failed results
            for original_msg, classified_msg, error in results:
                if error:
                    failed_messages.append((original_msg, error))
                    logger.warning(
                        f"Failed to process message {original_msg.id}: {error}"
                    )
                else:
                    successful_results.append(classified_msg)
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            # If gather fails, treat all messages as failed
            failed_messages = [(msg, e) for msg in messages]
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        logger.info(
            f"Batch processing completed in {processing_time:.3f}s: "
            f"{len(successful_results)} successful, {len(failed_messages)} failed"
        )
        
        return successful_results, failed_messages
    
    async def process_with_retry(
        self,
        message: ScrapedMessage,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> Optional[ClassifiedMessage]:
        """
        Process a message with retry logic.
        
        Args:
            message: ScrapedMessage to process
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff factor
            provider: Optional AI provider override
            model: Optional model override
            
        Returns:
            ClassifiedMessage if successful, None if all retries failed
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                return await self.process_message(message, provider, model)
                
            except Exception as e:
                last_error = e
                
                if attempt < max_retries:
                    delay = backoff_factor * (2 ** attempt)
                    logger.warning(
                        f"Processing failed for message {message.id} "
                        f"(attempt {attempt + 1}/{max_retries + 1}). "
                        f"Retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Processing failed for message {message.id} "
                        f"after {max_retries + 1} attempts: {e}"
                    )
        
        # If we get here, all retries failed
        return None
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get stream processor metrics.
        
        Returns:
            Dictionary with processor metrics
        """
        return {
            'batch_size': self.batch_size,
            'processing_timeout': self.processing_timeout,
            'classification_engine_ready': hasattr(classification_engine, 'default_provider'),
            'taxonomy_service_ready': taxonomy_service.get_current_version() is not None
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on stream processor.
        
        Returns:
            Health check results
        """
        health = {
            'status': 'healthy',
            'processor_ready': True,
            'classification_engine_ready': hasattr(classification_engine, 'default_provider'),
            'taxonomy_service_ready': False,
            'batch_size': self.batch_size,
            'timeout': self.processing_timeout
        }
        
        # Check taxonomy service
        try:
            taxonomy_version = taxonomy_service.get_current_version()
            health['taxonomy_service_ready'] = taxonomy_version is not None
            if taxonomy_version:
                health['taxonomy_version'] = taxonomy_version.version_string
        except Exception as e:
            logger.warning(f"Taxonomy service health check failed: {e}")
            health['taxonomy_service_ready'] = False
        
        # Overall status
        if not health['classification_engine_ready'] or not health['taxonomy_service_ready']:
            health['status'] = 'degraded'
        
        return health


class ClassificationError(Exception):
    """Raised when message classification fails."""
    pass


# Global instance
stream_processor = StreamProcessor()