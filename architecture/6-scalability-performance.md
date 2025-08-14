# 6. Scalability & Performance

## 6.1 Horizontal Scaling Points

**API Layer**:
- Stateless FastAPI instances behind load balancer
- Auto-scaling based on CPU/memory metrics
- Connection pooling for database access

**Event Processing**:
- Multiple Kafka consumer instances per consumer group
- Partition-based parallelism (scale by topic partitions)
- Independent scaling from API layer

**Database Layer**:
- Read replicas for analytics workloads
- Partitioning of large tables by date
- Connection pooling and prepared statements

## 6.2 Performance Characteristics

**Latency Targets**:
- Synchronous API: p95 < 2s, p99 < 5s
- Stream processing: < 100ms per message (excluding AI call)
- Taxonomy sync: < 30s end-to-end

**Throughput Targets**:
- API: 100 RPS per instance
- Kafka processing: 1000 messages/sec per consumer
- Database writes: 5000 inserts/sec

## 6.3 Caching Strategy

**Multi-Level Caching**:
1. **Application Cache**: In-memory taxonomy and prompts
2. **Redis Cache**: Cross-instance shared state
3. **Database Cache**: PostgreSQL query cache
4. **Provider Cache**: AI response caching (optional)

---
