# 2) Goals & Non‑Goals

## 2.1 Goals

* Accurately classify text into a 1–4 tier hierarchy defined in a Google Sheet (tab `event_type`).
* Return results with confidence and human-readable reasoning.
* Offer two input paths: FastAPI endpoint (single & batch) and Kafka consumer (near‑real‑time).
* Abstract model providers behind an OpenAI-compatible interface (cloud or local, e.g., OpenAI / Ollama).
* Provide robust observability (structured JSON logs) and **cost visibility**; budgets are configurable but **disabled by default** in MVP.
* Enable a data flywheel for future fine‑tuning using accumulated classification data.

## 2.2 Non‑Goals (v1)

* Building a UI/console (monitoring will be via metrics/logging only).
* Human-in-the-loop review tools (export only; no review dashboard in v1).
* Training a fully custom model (planned post‑MVP using collected data).

---
