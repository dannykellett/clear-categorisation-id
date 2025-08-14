from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import time
from app.services.taxonomy import taxonomy_service
from app.models.taxonomy import TaxonomyVersionResponse, TaxonomyPreviewResponse, SyncTriggerResponse
from app.core.logging import get_logger, log_with_context

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
    """Dependency readiness check"""
    try:
        # Check basic readiness
        dependencies = {
            "api": True,  # API is running if we get here
            "models": True,  # Models are loaded if we get here
        }
        
        # Overall readiness
        all_ready = all(dependencies.values())
        
        return {
            "status": "ready" if all_ready else "not_ready",
            "timestamp": time.time(),
            "dependencies": dependencies
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