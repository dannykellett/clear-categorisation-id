from fastapi import APIRouter, HTTPException
from app.core.logging import get_logger, log_with_context
from app.models.requests import ClassificationRequest, BatchClassificationRequest
from app.models.responses import ClassificationResponse, BatchClassificationResponse
from app.engine.classifier import classification_engine

router = APIRouter(prefix="/classify", tags=["classification"])
logger = get_logger(__name__)


@router.post("/", response_model=ClassificationResponse)
async def classify_text(request: ClassificationRequest):
    """Single text classification endpoint"""
    try:
        log_with_context(
            logger, "info", "Classification request received",
            text_length=len(request.text),
            provider=request.provider,
            model=request.model,
            event="classification_started"
        )
        
        # Use classification engine
        categories, usage = await classification_engine.classify_text(
            text=request.text,
            provider=request.provider,
            model=request.model
        )
        
        log_with_context(
            logger, "info", "Classification completed",
            categories_count=len(categories),
            provider=usage.provider,
            model=usage.model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            event="classification_completed"
        )
        
        return ClassificationResponse(
            categories=categories,
            usage=usage
        )
    except Exception as e:
        logger.error(f"Classification failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Classification failed: {str(e)}")


@router.post("/batch", response_model=BatchClassificationResponse)
async def classify_batch(request: BatchClassificationRequest):
    """Batch text classification endpoint"""
    try:
        log_with_context(
            logger, "info", "Batch classification request received",
            texts_count=len(request.texts),
            provider=request.provider,
            model=request.model,
            event="batch_classification_started"
        )
        
        # Use classification engine
        batch_results, total_usage = await classification_engine.classify_batch(
            texts=request.texts,
            provider=request.provider,
            model=request.model
        )
        
        # Convert to response format
        results = []
        for categories, usage in batch_results:
            results.append(ClassificationResponse(
                categories=categories,
                usage=usage
            ))
        
        log_with_context(
            logger, "info", "Batch classification completed",
            results_count=len(results),
            total_input_tokens=total_usage.input_tokens,
            total_output_tokens=total_usage.output_tokens,
            event="batch_classification_completed"
        )
        
        return BatchClassificationResponse(
            results=results,
            total_usage=total_usage
        )
    except Exception as e:
        logger.error(f"Batch classification failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch classification failed: {str(e)}")