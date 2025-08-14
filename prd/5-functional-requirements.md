# 5) Functional Requirements

## 5.1 Classification

* Accept **single** or **batch** text.
* Return 0..N applicable category paths (1 to 4 tiers each).
* Include `confidence` (0.0–1.0) and short `reasoning` string per path.
* Respect model/provider selection and token/cost ceilings.
* Support deterministic or temperature‑controlled runs (configurable).

## 5.2 Kafka Ingestion

* **Consumer** subscribes to topic `scraped` (naming: use `-`, avoid `.`).
* Message schema includes at minimum: `{ id, article_id?, text, source, ts, attrs? }` (include `article_id` when available to support DB keying and retrieval).
* **Producer** publishes classification results to `scraped-classified` with original `id` and `article_id` for joinability.
* **Consumer group**: configurable `KAFKA_GROUP_ID`; scale by partitions.
* **Delivery**: at-least-once; DB writes idempotent by `article_id`.
* **DLQ**: optional for v1 (can be added later) using `scraped-classified-dlq`.

## 5.3 Admin/Operations

* Live health: `/healthz`, readiness: `/readyz`.
* Taxonomy endpoints: `/taxonomy/version`, `/taxonomy/preview`.
* Provider endpoints: `/providers`, `/providers/active`, and `/switch` (if enabled).
* **Few‑shot & permutation ops**: `GET/POST /fewshots`, `GET /fewshots/coverage`, `GET/POST /permutations`, `GET /permutations/coverage`.
* Metrics endpoint (Prometheus): `/metrics` (deferred).

## 5.4 Auditing & Storage (optional in MVP)

* Persist inputs/outputs/usage to Postgres for evaluation and future fine‑tuning.
* Retention window configurable (PII/DSAR compliant).

---
