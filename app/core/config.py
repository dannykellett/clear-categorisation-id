"""Application configuration management"""
from typing import Optional
from pydantic import BaseModel
import os


class AppConfig(BaseModel):
    """Application configuration settings"""
    
    # Application settings
    title: str = "ClearCategorisationIQ"
    description: str = "Text classification API with hierarchical taxonomy support"
    version: str = "0.1.0"
    
    # Logging settings
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = True
    
    # Classification engine settings
    default_provider: str = "mock"
    default_model: str = "mock-model"
    
    # Cache settings
    use_redis: bool = True
    redis_url: str = "redis://localhost:6379"
    
    # Taxonomy sync settings
    taxonomy_sync_interval: int = 300  # 5 minutes
    
    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create config from environment variables"""
        return cls(
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8000")),
            reload=os.getenv("RELOAD", "true").lower() == "true",
            default_provider=os.getenv("DEFAULT_PROVIDER", "mock"),
            default_model=os.getenv("DEFAULT_MODEL", "mock-model"),
            use_redis=os.getenv("USE_REDIS", "true").lower() == "true",
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            taxonomy_sync_interval=int(os.getenv("TAXONOMY_SYNC_INTERVAL", "300")),
        )


# Global config instance
config = AppConfig.from_env()