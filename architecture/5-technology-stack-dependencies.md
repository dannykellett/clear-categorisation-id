# 5. Technology Stack & Dependencies

## 5.1 Core Framework
- **FastAPI** 0.104+: High-performance async web framework
- **Pydantic** 2.0+: Data validation and serialization
- **Uvicorn**: ASGI server for production deployment

## 5.2 Data & Messaging
- **PostgreSQL** 15+: Primary data store with JSONB support
- **Redis** 7+: Caching and session storage
- **Kafka** 3.5+: Event streaming platform
- **asyncpg**: High-performance async PostgreSQL driver

## 5.3 AI & ML
- **OpenAI Python SDK**: Cloud AI provider integration
- **httpx**: HTTP client for Ollama integration
- **tiktoken**: Token counting for cost estimation
- **sentence-transformers**: Embedding models for similarity search

## 5.4 Operations & Monitoring
- **structlog**: Structured logging with JSON output
- **prometheus-client**: Metrics collection (future)
- **alembic**: Database migration management
- **pytest**: Testing framework

---
