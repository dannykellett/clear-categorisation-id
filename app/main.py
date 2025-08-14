from fastapi import FastAPI, Request
import uuid
from contextlib import asynccontextmanager
from app.core.config import config
from app.core.logging import setup_logging, set_correlation_id, get_logger
from app.core.cache import cache_manager
from app.tasks.scheduler import taxonomy_scheduler
from app.routers import classify, admin

# Configure structured logging
setup_logging(config.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown"""
    # Startup
    logger.info("Application starting up")
    
    # Initialize cache manager
    await cache_manager.initialize()
    
    # Start taxonomy scheduler
    await taxonomy_scheduler.start()
    
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Application shutting down")
    
    # Stop scheduler
    await taxonomy_scheduler.stop()
    
    # Close cache connections
    await cache_manager.close()
    
    logger.info("Application shutdown complete")


app = FastAPI(
    title=config.title,
    description=config.description,
    version=config.version,
    lifespan=lifespan
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