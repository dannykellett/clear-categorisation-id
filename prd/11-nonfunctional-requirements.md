# 11) Non‑Functional Requirements

* **Security**: Service auth via API key or OIDC (service‑to‑service). Google Sheet is **public read‑only**; fetch via unauthenticated export (no OAuth). Secrets in vault. No training on customer data without opt‑in.
* **Scalability**: Horizontal scale for API workers and Kafka consumers; stateless app.
* **Availability**: Target 99.9% for API (business hours SLA acceptable for MVP if needed).
* **Observability**: Structured logging in **JSONL**; compatible with future Elasticsearch + Kibana. Tracing/OTel omitted for now. Metrics can be added later.
* **Internationalization**: Accept UTF‑8 text; optional language detection to improve prompting.

---
