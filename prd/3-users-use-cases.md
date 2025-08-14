# 3) Users & Use Cases

## 3.1 Primary Users

* **Data engineers** integrating event pipelines.
* **ML/Platform engineers** managing model providers and cost/latency.
* **Analysts** consuming classification outputs for downstream analytics.

## 3.2 Key Use Cases

1. **Ad hoc classification**: POST text to `/classify` and receive hierarchical labels.
2. **Batch classification**: POST an array of texts to `/classify/batch` with consistent performance envelopes and usage reporting.
3. **Streaming ingestion**: Consume Kafka topic `scraped` and produce results to `scraped-classified` (topic naming: use hyphens `-`, no full stops).
4. **Taxonomy governance**: Update taxonomy in Google Sheets; service detects, validates, and version‑tags changes.

---
