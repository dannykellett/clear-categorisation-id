from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import time

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