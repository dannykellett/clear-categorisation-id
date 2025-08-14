# 15) Rollout Plan & Milestones

## 15.1 MVP (v0)

* FastAPI `/classify` single + batch (default **multiple paths** per item).
* Google Sheet sync + versioning + validation (public read‑only sheet; no OAuth).
* OpenAI + Ollama adapters; config routing.
* Basic structured logging (JSONL).
* Kafka consumer (`scraped`) → producer (`scraped-classified`).

**Acceptance criteria:**

* [ ] Given valid text, API returns one or more category paths with confidence & reasoning.
* [ ] Taxonomy sync runs on schedule and exposes current version; invalid sheet changes do not break prod.
* [ ] Kafka messages from `scraped` are classified and emitted to `scraped-classified` with matching `id`/`article_id`.
* [ ] Switching provider/model per request works and logs usage.
* [ ] `GET /articles/{article_id}/classifications` returns the latest result(s).

## 15.2 v1

* Persistence of requests/results (Postgres) complete; DLQ for failures; evaluation harness & seed dataset; few‑shot coverage dashboard endpoints.

## 15.3 v2+

* Active learning loop (feedback API), semi‑supervised fine‑tune, improved batching, autoscaling.

---
