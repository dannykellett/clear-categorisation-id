# 7. Security Architecture

## 7.1 Authentication & Authorization

**API Security**:
- API key authentication for service-to-service
- OIDC integration for user-facing tools
- Rate limiting per client/key
- Request/response logging for audit

**Data Security**:
- Encryption at rest (database level)
- Encryption in transit (TLS 1.3)
- PII redaction configurable
- Audit trail for all data access

## 7.2 Network Security

**Network Topology**:
- Private subnets for database and Kafka
- Public subnet for API gateway only
- VPC peering for cross-service communication
- WAF protection for API endpoints

---
