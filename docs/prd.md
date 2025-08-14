# ClearCategorisationIQ — Product Requirements Document (PRD)

**Doc version:** v0.1 (Draft)
**Date:** 14 Aug 2025
**Owner:** PM (you)
**Editors:** PM, Architect, PO, Dev
**Status:** Draft for review

---

## 1) Summary

ClearCategorisationIQ is an API-only backend that performs multi-level categorisation of unstructured text (e.g., blog posts, social media, press releases) against a Google Sheet–driven taxonomy. It returns hierarchical labels (up to 4 tiers), a confidence score, and a brief explanation. Events can be classified ad hoc via HTTP or streamed in real time from Kafka.

---

## 2) Goals & Non‑Goals

### 2.1 Goals

* Accurately classify text into a 1–4 tier hierarchy defined in a Google Sheet (tab `event_type`).
* Return results with confidence and human-readable reasoning.
* Offer two input paths: FastAPI endpoint (single & batch) and Kafka consumer (near‑real‑time).
* Abstract model providers behind an OpenAI-compatible interface (cloud or local, e.g., OpenAI / Ollama).
* Provide robust observability (structured JSON logs) and **cost visibility**; budgets are configurable but **disabled by default** in MVP.
* Enable a data flywheel for future fine‑tuning using accumulated classification data.

### 2.2 Non‑Goals (v1)

* Building a UI/console (monitoring will be via metrics/logging only).
* Human-in-the-loop review tools (export only; no review dashboard in v1).
* Training a fully custom model (planned post‑MVP using collected data).

---

## 3) Users & Use Cases

### 3.1 Primary Users

* **Data engineers** integrating event pipelines.
* **ML/Platform engineers** managing model providers and cost/latency.
* **Analysts** consuming classification outputs for downstream analytics.

### 3.2 Key Use Cases

1. **Ad hoc classification**: POST text to `/classify` and receive hierarchical labels.
2. **Batch classification**: POST an array of texts to `/classify/batch` with consistent performance envelopes and usage reporting.
3. **Streaming ingestion**: Consume Kafka topic `scraped` and produce results to `scraped-classified` (topic naming: use hyphens `-`, no full stops).
4. **Taxonomy governance**: Update taxonomy in Google Sheets; service detects, validates, and version‑tags changes.

---

## 4) Taxonomy Source of Truth

* **Sheet:** [https://docs.google.com/spreadsheets/d/1LQEbAKH7KjAFfuMR1pWK9rSqaBfpkRi8DDw\_7gU\_qTg](https://docs.google.com/spreadsheets/d/1LQEbAKH7KjAFfuMR1pWK9rSqaBfpkRi8DDw_7gU_qTg)  (tab: `event_type`)
* **Rows considered:** Only rows where `Classification Type` == `Event Type`.
* **Hierarchy columns:** `Classification tier 1` → `tier 2` → `tier 3` → `tier 4`.
* **Sync strategy:**

  * Poll on a schedule (e.g., every 5–10 min) and on demand via admin endpoint.
  * Validate schema (required columns present; no cycles; no orphans; tiers contiguous).
  * Build **taxonomy version** (e.g., `tax_vYYYYMMDD_HHMMSS`) and cache in Redis (or in‑process for MVP).
  * Expose `/taxonomy/version` and `/taxonomy/preview` for debugging.
* **Failure behavior:** If new sheet version fails validation, keep serving prior valid version and emit alerts.

---

## 5) Functional Requirements

### 5.1 Classification

* Accept **single** or **batch** text.
* Return 0..N applicable category paths (1 to 4 tiers each).
* Include `confidence` (0.0–1.0) and short `reasoning` string per path.
* Respect model/provider selection and token/cost ceilings.
* Support deterministic or temperature‑controlled runs (configurable).

### 5.2 Kafka Ingestion

* **Consumer** subscribes to topic `scraped` (naming: use `-`, avoid `.`).
* Message schema includes at minimum: `{ id, article_id?, text, source, ts, attrs? }` (include `article_id` when available to support DB keying and retrieval).
* **Producer** publishes classification results to `scraped-classified` with original `id` and `article_id` for joinability.
* **Consumer group**: configurable `KAFKA_GROUP_ID`; scale by partitions.
* **Delivery**: at-least-once; DB writes idempotent by `article_id`.
* **DLQ**: optional for v1 (can be added later) using `scraped-classified-dlq`.

### 5.3 Admin/Operations

* Live health: `/healthz`, readiness: `/readyz`.
* Taxonomy endpoints: `/taxonomy/version`, `/taxonomy/preview`.
* Provider endpoints: `/providers`, `/providers/active`, and `/switch` (if enabled).
* **Few‑shot & permutation ops**: `GET/POST /fewshots`, `GET /fewshots/coverage`, `GET/POST /permutations`, `GET /permutations/coverage`.
* Metrics endpoint (Prometheus): `/metrics` (deferred).

### 5.4 Auditing & Storage (optional in MVP)

* Persist inputs/outputs/usage to Postgres for evaluation and future fine‑tuning.
* Retention window configurable (PII/DSAR compliant).

---

## 6) API Design (FastAPI)

### 6.1 Request/Response — Single

`POST /classify`

```json
{
  "text": "Apple unveils new M-series chips at WWDC...",
  "hint": "tech, launches",        // optional
  "max_paths": 3,                   // optional (default 3)
  "provider": "openai",            // optional (default from config)
  "model": "gpt-4o-mini",          // optional (default from config)
  "temperature": 0.1,               // optional
  "taxonomy_version": "latest",    // or explicit version
  "confidence_threshold": 0.7       // optional; if omitted, use global or per-tree override
}
```

**200 OK**

```json
{
  "classification": [
    {
      "tier_1": "Event Category",
      "tier_2": "Subcategory",
      "tier_3": "Detail",
      "tier_4": "Granular Detail",
      "confidence": 0.92,
      "reasoning": "Keyword match with 'X'; inferred relevance due to 'Y'"
    }
  ],
  "provider": "openai",
  "model": "gpt-4o-mini",
  "usage": {"input_tokens": 150, "output_tokens": 20, "total_tokens": 170},
  "taxonomy_version": "tax_v20250814_103000",
  "request_id": "b3e9..."
}
```

### 6.2 Request/Response — Batch

`POST /classify/batch`

```json
{
  "items": [
    {"id": "a1", "text": "...", "article_id": "art-123"},
    {"id": "a2", "text": "...", "article_id": "art-456"}
  ],
  "max_paths": 3,
  "provider": "ollama",
  "model": "qwen3:30b",
  "confidence_threshold": 0.7
}
```

**200 OK**

```json
{
  "results": [
    {
      "id": "a1",
      "article_id": "art-123",
      "classification": [ { /* same as single */ } ],
      "usage": {"input_tokens": 120, "output_tokens": 18, "total_tokens": 138}
    },
    {
      "id": "a2",
      "article_id": "art-456",
      "classification": [],
      "usage": {"input_tokens": 75, "output_tokens": 8, "total_tokens": 83}
    }
  ],
  "provider": "ollama",
  "model": "qwen3:30b"
}
```

### 6.3 Error Codes

* `400` invalid request (missing text, unsupported model, taxonomy unavailable)
* `409` taxonomy version conflict
* `429` budget/throughput limits exceeded
* `500` provider failure / unknown

### 6.4 Retrieval by `article_id`

`GET /articles/{article_id}/classifications`

* Returns latest classification document(s) for the given `article_id` from Postgres.
* Query params: `limit` (default 1), `include_usage` (bool, default true).

---

## 7) Kafka Contracts

### 7.1 Input Topic (`scraped`)

```json
{
  "id": "uuid",
  "article_id": "string",       // preferred key for persistence
  "text": "string",
  "source": "string",            // e.g., blog, social, press_release
  "ts": "ISO-8601",
  "attrs": {"lang": "en", "author": "..."}
}
```

### 7.2 Output Topic (`scraped-classified`)

```json
{
  "id": "uuid",                  // original id
  "article_id": "string",        // for DB joinability
  "classification": [ { /* as above */ } ],
  "provider": "openai|ollama",
  "model": "string",
  "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
  "taxonomy_version": "tax_v...",
  "processed_ts": "ISO-8601"
}
```

> **Topic naming rule:** use hyphens (`-`), avoid full stops (`.`).

---

## 8) Model & Provider Strategy

* **Abstraction:** Use OpenAI Python SDK interface for both cloud (OpenAI) and local (Ollama) providers.
* **Routing:** Default provider/model via config; per‑request override allowed. Fallback on failure (e.g., try provider B if A 5xx).
* **Prompting:** System prompts align to taxonomy; few‑shot examples loaded per tier where available.
* **Cost**: focus on **visibility** first (usage logging, per‑request token counts). Budget enforcement is configurable but **off by default**.
* **Future:** Fine‑tune/open-source model from stored labeled data to reduce cost & latency.

### 8.1 Few‑shot prompting strategy

* **Source of examples:** Maintain a curated library in Postgres table `few_shots` (see §9) with columns: `id`, `path_key` (e.g., `tier1/tier2/tier3/tier4` or subtree key), `example_input`, `example_label_json`, `explanation`, `taxonomy_version`, `active`, `created_by`, `created_at`.
* **Selection policy:**

  * **Primary**: select by exact `path_key` (per category path) if present;
  * **Fallback**: select by parent subtree (e.g., tier1/tier2/\*) and/or **semantic similarity** (embedding search over `example_input`).
  * Use `top_k` (e.g., 3–5) examples; hard cap total prompt tokens.
  * Bind examples to `taxonomy_version` to avoid drift.
* **Injection mechanics:** Compose a structured system prompt (taxonomy + rules) + few‑shot examples + the input. Cache rendered prompts keyed by `(model, taxonomy_version, path_key, top_k)`.
* **Governance/ops:**

  * Endpoints: `GET/POST /fewshots`, `GET /fewshots/coverage` (see §5.3, §6.4) showing which paths have examples and **permutation** coverage.
  * **A/B flags** to compare example sets; track downstream accuracy by set id.
* **No regex/manual overrides:** We rely on the curated few‑shot library instead of regex/keyword rules.
* **Permutation tracking:** Store tested permutations and their linked few‑shot ids (table `permutation_catalog`) to drive targeted testing.

---

## 9) Data, Storage & Retention

* **Postgres is required** in v1. Retention: **forever**. Primary key for retrieval is `article_id`.

* **Tables (initial):**

  * `articles` — canonical text store for future joins

    * `article_id` (PK), `source`, `ts`, `text`, `attrs JSONB`, `ingested_at`
  * `classifications` — results per item

    * `id` (PK), `article_id` (FK→articles.article\_id), `classification JSONB`, `provider`, `model`, `usage JSONB`, `taxonomy_version`, `processed_ts`
    * Indexes: `ix_classifications_article_id`, GIN on `classification`
  * `taxonomy_versions`

    * `taxonomy_version` (PK), `sheet_etag`, `created_at`
  * `few_shots`

    * `id` (PK), `path_key`, `example_input`, `example_label_json JSONB`, `explanation`, `taxonomy_version`, `active BOOL`, `created_by`, `created_at`
    * Indexes: `ix_few_shots_path_key`, `ix_few_shots_active`
  * `permutation_catalog`

    * `id` (PK), `path_key`, `permutation_key` (hash), `status` (e.g., tested/needs\_review), `linked_few_shot_ids INT[]`, `notes`, `last_tested_at`, `metrics JSONB`

* **Write policy:** Upsert by `article_id` for idempotence.

* **Access patterns:** `GET /articles/{article_id}/classifications` returns latest classification rows for that article.

* **Future joins:** Additional datasets can reference `article_id` as the common key.

---

## 10) Quality, Evaluation & Metrics

### 10.1 Functional Quality

* Precision/Recall/F1 on a seed test set (≥100 labeled examples) per tier.
* Human spot‑checks of `reasoning` quality (readable, references signal).

### 10.2 Runtime Metrics (Prometheus)

* **De-scoped for MVP**: Not a priority now. No Prometheus export. Capture latency, token usage, and costs via structured logs only (see §11 Observability).

### 10.3 Success Metrics (Product)

* ≥ 85% top‑path accuracy on curated validation set by end of v1.
* < 1% classification drop due to taxonomy sync errors over 30 days.
* ≤ 2 engineer‑hours/week on ops after month 1.

---

## 11) Non‑Functional Requirements

* **Security**: Service auth via API key or OIDC (service‑to‑service). Google Sheet is **public read‑only**; fetch via unauthenticated export (no OAuth). Secrets in vault. No training on customer data without opt‑in.
* **Scalability**: Horizontal scale for API workers and Kafka consumers; stateless app.
* **Availability**: Target 99.9% for API (business hours SLA acceptable for MVP if needed).
* **Observability**: Structured logging in **JSONL**; compatible with future Elasticsearch + Kibana. Tracing/OTel omitted for now. Metrics can be added later.
* **Internationalization**: Accept UTF‑8 text; optional language detection to improve prompting.

---

## 12) Configuration

Environment variables / config file:

* `PROVIDER_DEFAULT` = `openai|ollama`
* `MODEL_DEFAULT` = `gpt-4o-mini|qwen3:30b|...`
* `OPENAI_API_KEY`, `OLLAMA_BASE_URL`
* `DB_URL` (Postgres)
* `SHEET_ID`, `SHEET_TAB=event_type`
* `SHEET_SYNC_CRON=*/10 * * * *` (or seconds for dev)
* `KAFKA_BROKERS`, `KAFKA_IN_TOPIC=scraped`, `KAFKA_OUT_TOPIC=scraped-classified`, `KAFKA_GROUP_ID`
* `REDIS_URL`
* `CONFIDENCE_THRESHOLD_DEFAULT` (e.g., 0.7); optional per‑tree overrides via DB table `taxonomy_thresholds` (future)
* `REQ_RATE_LIMIT`, `TOKEN_RATE_LIMIT` (optional; **disabled by default**)

---

## 13) Prompting & Output Contracts (Reference)

* System prompt encodes taxonomy and rules: choose 0..N paths, up to 4 tiers; return JSON only; keep reasoning ≤ 240 chars; avoid hallucinated tiers.
* Include light heuristics: penalize assigning many paths unless confidence high.
* Provide few‑shot examples per high‑volume branches (configurable file or DB table).

---

## 14) Testing Strategy

* **Unit tests**: taxonomy parser/validator; API contracts; provider adapter mocks.
* **Integration tests**: live call against sandbox provider; sheet sync against test tab; Kafka round‑trip in docker‑compose.
* **Eval harness**: offline test set to compute precision/recall per tier and confusion matrix.

---

## 15) Rollout Plan & Milestones

### 15.1 MVP (v0)

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

### 15.2 v1

* Persistence of requests/results (Postgres) complete; DLQ for failures; evaluation harness & seed dataset; few‑shot coverage dashboard endpoints.

### 15.3 v2+

* Active learning loop (feedback API), semi‑supervised fine‑tune, improved batching, autoscaling.

---

## 16) Risks & Mitigations

* **Taxonomy drift / sheet edits break structure** → Strict validation + version pinning + alerts; roll back to last good version.
* **Provider outages / cost spikes** → Multi‑provider fallback; local models; budgets + rate limits.
* **Latency variance (local models)** → Adaptive batching + concurrency tuning; model size controls.
* **Reasoning hallucination** → Constrain output schema; validate tiers; truncate reasoning; post‑validate category membership.
* **PII exposure** → Redaction pipeline; configurable storage off switch; access controls.

---

## 17) Decisions & Open Questions

### Decisions captured

* **Multiple category paths**: Enabled by default; `max_paths` default = 3.
* **Kafka topics**: `scraped` → `scraped-classified`; use `-`, avoid `.`.
* **Retention**: Store **forever** in Postgres, keyed by `article_id`. Retrieval endpoint provided.
* **Manual overrides**: No regex/keyword overrides; rely on curated few‑shot examples + permutation coverage.
* **Prometheus metrics**: Deferred; rely on logs for now.
* **Google Sheets auth**: Public read‑only; no OAuth.

### Confidence threshold strategy

* Implement a **global default** (e.g., 0.7) with optional **per‑tree overrides** (higher control). Precedence: `per‑path > per‑tree > global`.
* Control trade‑off: higher threshold → fewer false positives (↑precision) but may reduce recall and require more reprocessing. Per‑tree overrides give better accuracy control without imposing a global cost penalty.

### Still open

* Default values for per‑tree thresholds and where to store them (proposed: DB table `taxonomy_thresholds(path_key, threshold, taxonomy_version, active)`).
* Embedding model choice and `top_k` for few‑shot retrieval.

---

## 18) Appendix

### 18.1 Example Internal Types (Pydantic‑style)

```python
class CategoryPath(BaseModel):
    tier_1: str
    tier_2: str | None = None
    tier_3: str | None = None
    tier_4: str | None = None
    confidence: float
    reasoning: str  # ≤ 240 chars

class ClassificationResult(BaseModel):
    classification: list[CategoryPath]
    provider: Literal['openai','ollama']
    model: str
    usage: dict[str, int]
    taxonomy_version: str
    request_id: str
```

### 18.2 Validation Rules (Taxonomy)

* Tiers must be contiguous from 1 → N (no gaps).
* Paths must exist in current version; unknown tier value → reject.
* Duplicate sibling names disallowed within same parent.
* Normalise case/whitespace; provide `path_key` = `tier1/tier2/tier3/tier4`.

---

**End of PRD (v0.1)**
