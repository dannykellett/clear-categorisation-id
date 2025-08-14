from fastapi import APIRouter, HTTPException
from typing import Dict, Any, Optional
import time
from app.services.taxonomy import taxonomy_service
from app.models.taxonomy import TaxonomyVersionResponse, TaxonomyPreviewResponse, SyncTriggerResponse
from app.core.logging import get_logger, log_with_context
from app.consumers import ScrapedMessageConsumer
from app.producers import ClassifiedMessageProducer
from app.engine.stream_processor import stream_processor
from app.services.persistence import get_persistence_service

logger = get_logger(__name__)

router = APIRouter(tags=["admin"])


@router.get("/healthz")
async def health_check() -> Dict[str, Any]:
    """Basic application health check"""
    try:
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "service": "classification-api",
            "version": "0.1.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@router.get("/readyz")
async def readiness_check() -> Dict[str, Any]:
    """Dependency readiness check including Kafka pipeline"""
    try:
        # Check basic readiness
        dependencies = {
            "api": True,  # API is running if we get here
            "models": True,  # Models are loaded if we get here
        }
        
        # Check Kafka pipeline readiness
        try:
            stream_health = await stream_processor.health_check()
            dependencies["stream_processor"] = stream_health.get('status') == 'healthy'
        except Exception:
            dependencies["stream_processor"] = False
        
        try:
            persistence_service = get_persistence_service()
            persistence_health = await persistence_service.health_check()
            dependencies["database"] = persistence_health.get('status') == 'healthy'
        except Exception:
            dependencies["database"] = False
        
        try:
            # Check Kafka connectivity
            consumer = ScrapedMessageConsumer()
            producer = ClassifiedMessageProducer()
            
            consumer_health = await consumer.health_check()
            producer_health = await producer.health_check()
            
            dependencies["kafka_consumer"] = consumer_health.get('status') == 'healthy'
            dependencies["kafka_producer"] = producer_health.get('status') == 'healthy'
        except Exception:
            dependencies["kafka_consumer"] = False
            dependencies["kafka_producer"] = False
        
        # Overall readiness
        all_ready = all(dependencies.values())
        
        return {
            "status": "ready" if all_ready else "not_ready",
            "timestamp": time.time(),
            "dependencies": dependencies,
            "kafka_pipeline_ready": dependencies.get("kafka_consumer", False) and 
                                  dependencies.get("kafka_producer", False) and
                                  dependencies.get("stream_processor", False) and
                                  dependencies.get("database", False)
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Readiness check failed: {str(e)}")


@router.get("/taxonomy/version", response_model=TaxonomyVersionResponse)
async def get_taxonomy_version() -> TaxonomyVersionResponse:
    """Get current taxonomy version information"""
    try:
        log_with_context(logger, "info", "Taxonomy version requested")
        
        current_version = taxonomy_service.get_current_version()
        
        if current_version:
            return TaxonomyVersionResponse(
                version=current_version.version_string,
                created_at=current_version.created_at,
                status="active",
                metadata={
                    "service": "classification-api",
                    "endpoint": "/taxonomy/version"
                }
            )
        else:
            return TaxonomyVersionResponse(
                version=None,
                created_at=None,
                status="no_data",
                metadata={
                    "message": "No taxonomy data available",
                    "service": "classification-api"
                }
            )
            
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get taxonomy version",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get taxonomy version: {str(e)}"
        )


@router.get("/taxonomy/preview", response_model=TaxonomyPreviewResponse)
async def get_taxonomy_preview() -> TaxonomyPreviewResponse:
    """Get taxonomy structure preview for debugging"""
    try:
        log_with_context(logger, "info", "Taxonomy preview requested")
        
        preview_data = taxonomy_service.get_taxonomy_preview()
        
        return TaxonomyPreviewResponse(
            version=preview_data["version"],
            status=preview_data["status"],
            structure=preview_data["structure"],
            metadata=preview_data["metadata"]
        )
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get taxonomy preview",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get taxonomy preview: {str(e)}"
        )


@router.post("/taxonomy/sync", response_model=SyncTriggerResponse)
async def trigger_taxonomy_sync() -> SyncTriggerResponse:
    """Manually trigger taxonomy synchronization"""
    try:
        log_with_context(logger, "info", "Manual taxonomy sync triggered")
        
        success, validation_result = await taxonomy_service.sync_taxonomy(force=True)
        
        current_version = taxonomy_service.get_current_version()
        version_string = current_version.version_string if current_version else None
        
        if success:
            message = f"Taxonomy sync completed successfully. New version: {version_string}"
        else:
            message = f"Taxonomy sync failed. {len(validation_result.errors)} errors found."
        
        log_with_context(
            logger, "info", "Manual taxonomy sync completed",
            success=success,
            version=version_string,
            error_count=len(validation_result.errors)
        )
        
        return SyncTriggerResponse(
            success=success,
            version=version_string,
            validation_result=validation_result,
            message=message
        )
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to trigger taxonomy sync",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger taxonomy sync: {str(e)}"
        )


@router.get("/kafka/consumer/metrics")
async def get_consumer_metrics() -> Dict[str, Any]:
    """Get Kafka consumer metrics and status"""
    try:
        log_with_context(logger, "info", "Consumer metrics requested")
        
        # Create a temporary consumer instance to get metrics
        # In production, this would be from a running consumer instance
        consumer = ScrapedMessageConsumer()
        
        # Get base consumer metrics
        metrics = consumer.get_metrics()
        
        # Add processing pipeline health
        stream_health = await stream_processor.health_check()
        persistence_service = get_persistence_service()
        persistence_health = await persistence_service.health_check()
        
        response = {
            "status": "healthy",
            "timestamp": time.time(),
            "consumer": {
                "metrics": metrics,
                "input_topic": consumer.input_topic,
                "group_id": consumer.group_id,
                "is_running": consumer.is_running,
                "is_stopping": consumer.is_stopping
            },
            "processing_pipeline": {
                "stream_processor": stream_health,
                "persistence_service": persistence_health
            }
        }
        
        # Determine overall status
        if (not stream_health.get('status') == 'healthy' or 
            not persistence_health.get('status') == 'healthy'):
            response["status"] = "degraded"
        
        return response
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get consumer metrics",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get consumer metrics: {str(e)}"
        )


@router.get("/kafka/producer/metrics")
async def get_producer_metrics() -> Dict[str, Any]:
    """Get Kafka producer metrics and status"""
    try:
        log_with_context(logger, "info", "Producer metrics requested")
        
        # Create a temporary producer instance to get metrics
        producer = ClassifiedMessageProducer()
        
        # Get producer metrics
        metrics = producer.get_topic_metrics()
        health = await producer.health_check()
        
        response = {
            "status": health.get("status", "unknown"),
            "timestamp": time.time(),
            "producer": {
                "metrics": metrics,
                "health": health,
                "output_topic": producer.output_topic,
                "is_running": producer.is_running,
                "is_stopping": producer.is_stopping
            }
        }
        
        return response
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get producer metrics",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get producer metrics: {str(e)}"
        )


@router.get("/kafka/pipeline/health")
async def get_pipeline_health() -> Dict[str, Any]:
    """Get comprehensive Kafka pipeline health check"""
    try:
        log_with_context(logger, "info", "Pipeline health check requested")
        
        # Check all pipeline components
        stream_health = await stream_processor.health_check()
        persistence_service = get_persistence_service()
        persistence_health = await persistence_service.health_check()
        
        # Create temporary instances for health checks
        consumer = ScrapedMessageConsumer()
        producer = ClassifiedMessageProducer()
        
        consumer_health = await consumer.health_check()
        producer_health = await producer.health_check()
        
        # Compile comprehensive health status
        components = {
            "stream_processor": stream_health,
            "persistence_service": persistence_health,
            "kafka_consumer": consumer_health,
            "kafka_producer": producer_health
        }
        
        # Determine overall pipeline health
        healthy_components = sum(1 for comp in components.values() 
                               if comp.get('status') == 'healthy')
        total_components = len(components)
        
        if healthy_components == total_components:
            overall_status = "healthy"
        elif healthy_components > 0:
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"
        
        response = {
            "status": overall_status,
            "timestamp": time.time(),
            "health_score": f"{healthy_components}/{total_components}",
            "components": components,
            "pipeline_ready": overall_status in ["healthy", "degraded"]
        }
        
        return response
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get pipeline health",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get pipeline health: {str(e)}"
        )


@router.get("/kafka/consumer/lag")
async def get_consumer_lag() -> Dict[str, Any]:
    """Get Kafka consumer lag information"""
    try:
        log_with_context(logger, "info", "Consumer lag information requested")
        
        # Create temporary consumer for lag checking
        consumer = ScrapedMessageConsumer()
        
        # Get consumer metrics which include lag information
        metrics = consumer.get_metrics()
        
        # Get partition assignment and lag details
        lag_info = {
            "status": "monitoring",
            "timestamp": time.time(),
            "consumer_group": consumer.group_id,
            "topic": consumer.input_topic,
            "lag_metrics": {
                "is_running": consumer.is_running,
                "messages_processed": metrics.get("messages_processed", 0),
                "messages_failed": metrics.get("messages_failed", 0),
                "last_poll_timestamp": metrics.get("last_poll_timestamp"),
                "processing_rate": metrics.get("processing_rate", 0.0)
            },
            "notes": [
                "Detailed per-partition lag requires active consumer connection",
                "Use external Kafka monitoring tools for comprehensive lag metrics"
            ]
        }
        
        return lag_info
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get consumer lag",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get consumer lag: {str(e)}"
        )


@router.get("/kafka/processing/stats")
async def get_processing_stats() -> Dict[str, Any]:
    """Get message processing statistics"""
    try:
        log_with_context(logger, "info", "Processing statistics requested")
        
        # Get stream processor metrics
        stream_metrics = stream_processor.get_metrics()
        
        # Get persistence service health for database stats
        persistence_service = get_persistence_service()
        persistence_health = await persistence_service.health_check()
        
        stats = {
            "status": "operational",
            "timestamp": time.time(),
            "stream_processing": {
                "batch_size": stream_metrics.get("batch_size", 0),
                "processing_timeout": stream_metrics.get("processing_timeout", 0),
                "classification_engine_ready": stream_metrics.get("classification_engine_ready", False),
                "taxonomy_service_ready": stream_metrics.get("taxonomy_service_ready", False)
            },
            "database": {
                "status": persistence_health.get("status", "unknown"),
                "connected": persistence_health.get("database_connected", False),
                "tables_ready": persistence_health.get("required_tables_exist", False)
            },
            "performance": {
                "target_processing_time": "< 100ms per message (excluding AI call)",
                "target_throughput": "1000 messages/sec per consumer",
                "database_target": "5000 inserts/sec capability"
            }
        }
        
        return stats
        
    except Exception as e:
        log_with_context(
            logger, "error", "Failed to get processing stats",
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get processing stats: {str(e)}"
        )