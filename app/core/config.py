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
    
    # Kafka settings
    kafka_brokers: str = "localhost:9092"
    kafka_in_topic: str = "scraped"
    kafka_out_topic: str = "scraped-classified"
    kafka_group_id: str = "classification-service"
    
    # Database settings
    database_host: str = "localhost"
    database_port: int = 5432
    database_name: str = "clear_categorisation"
    database_user: str = "postgres"
    database_password: str = "postgres"
    database_pool_min_size: int = 5
    database_pool_max_size: int = 20
    database_command_timeout: int = 60
    
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
            kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
            kafka_in_topic=os.getenv("KAFKA_IN_TOPIC", "scraped"),
            kafka_out_topic=os.getenv("KAFKA_OUT_TOPIC", "scraped-classified"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "classification-service"),
            database_host=os.getenv("DATABASE_HOST", "localhost"),
            database_port=int(os.getenv("DATABASE_PORT", "5432")),
            database_name=os.getenv("DATABASE_NAME", "clear_categorisation"),
            database_user=os.getenv("DATABASE_USER", "postgres"),
            database_password=os.getenv("DATABASE_PASSWORD", "postgres"),
            database_pool_min_size=int(os.getenv("DATABASE_POOL_MIN_SIZE", "5")),
            database_pool_max_size=int(os.getenv("DATABASE_POOL_MAX_SIZE", "20")),
            database_command_timeout=int(os.getenv("DATABASE_COMMAND_TIMEOUT", "60")),
        )


# Global config instance
config = AppConfig.from_env()


def get_settings() -> AppConfig:
    """Get application settings instance"""
    return config