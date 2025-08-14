# 13. Decision Log

## 13.1 Architecture Decisions

**ADR-001**: Microservice vs Monolith
- **Decision**: Microservice architecture
- **Rationale**: Independent scaling, technology diversity, team autonomy
- **Trade-offs**: Complexity vs flexibility

**ADR-002**: Event-Driven vs Request-Response
- **Decision**: Hybrid approach (both patterns)
- **Rationale**: Sync for immediate needs, async for high volume
- **Trade-offs**: Complexity vs performance

**ADR-003**: Database Choice
- **Decision**: PostgreSQL with JSONB
- **Rationale**: Relational + document flexibility, mature ecosystem
- **Trade-offs**: Single database vs polyglot persistence

## 13.2 Technology Decisions

**TDR-001**: FastAPI vs Django/Flask
- **Decision**: FastAPI
- **Rationale**: Performance, async support, OpenAPI integration
- **Trade-offs**: Maturity vs modern features

**TDR-002**: Kafka vs RabbitMQ/SQS
- **Decision**: Kafka
- **Rationale**: High throughput, durability, stream processing
- **Trade-offs**: Complexity vs capability

---
