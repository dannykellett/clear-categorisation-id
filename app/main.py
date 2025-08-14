from fastapi import FastAPI, Request
import uuid
from app.core.config import config
from app.core.logging import setup_logging, set_correlation_id, get_logger
from app.routers import classify, admin

# Configure structured logging
setup_logging(config.log_level)
logger = get_logger(__name__)

app = FastAPI(
    title=config.title,
    description=config.description,
    version=config.version,
)


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Add correlation ID to all requests for tracing"""
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    set_correlation_id(correlation_id)
    
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    
    return response


# Include routers
app.include_router(classify.router)
app.include_router(admin.router)


@app.get("/")
async def root():
    """Root endpoint for API status"""
    logger.info("Root endpoint accessed")
    return {"message": f"{config.title} API is running", "version": config.version}