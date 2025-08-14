# 9. Monitoring & Observability

## 9.1 Logging Strategy

**Structured JSON Logging**:
```json
{
  "timestamp": "2025-08-14T10:30:00Z",
  "level": "INFO",
  "service": "classification-engine",
  "trace_id": "abc123",
  "event": "classification_completed",
  "article_id": "art-456",
  "provider": "openai",
  "model": "gpt-4o-mini",
  "usage": {"input_tokens": 150, "output_tokens": 20},
  "latency_ms": 1250,
  "taxonomy_version": "tax_v20250814_103000"
}
```

## 9.2 Health Checks

**Endpoint Monitoring**:
- `/healthz`: Basic application health
- `/readyz`: Dependency readiness check
- `/metrics`: Prometheus metrics (future)

**Dependency Checks**:
- Database connectivity
- Redis availability  
- Kafka cluster health
- AI provider status
- Google Sheets accessibility

## 9.3 Alerting Strategy

**Critical Alerts**:
- Service unavailability > 1 minute
- Classification error rate > 5%
- Taxonomy sync failures
- Database connection failures

**Warning Alerts**:
- High latency (p95 > 3s)
- Low cache hit rates
- Provider cost spikes
- Queue backlog growth

---
