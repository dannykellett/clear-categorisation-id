# 17) Decisions & Open Questions

## Decisions captured

* **Multiple category paths**: Enabled by default; `max_paths` default = 3.
* **Kafka topics**: `scraped` → `scraped-classified`; use `-`, avoid `.`.
* **Retention**: Store **forever** in Postgres, keyed by `article_id`. Retrieval endpoint provided.
* **Manual overrides**: No regex/keyword overrides; rely on curated few‑shot examples + permutation coverage.
* **Prometheus metrics**: Deferred; rely on logs for now.
* **Google Sheets auth**: Public read‑only; no OAuth.

## Confidence threshold strategy

* Implement a **global default** (e.g., 0.7) with optional **per‑tree overrides** (higher control). Precedence: `per‑path > per‑tree > global`.
* Control trade‑off: higher threshold → fewer false positives (↑precision) but may reduce recall and require more reprocessing. Per‑tree overrides give better accuracy control without imposing a global cost penalty.

## Still open

* Default values for per‑tree thresholds and where to store them (proposed: DB table `taxonomy_thresholds(path_key, threshold, taxonomy_version, active)`).
* Embedding model choice and `top_k` for few‑shot retrieval.

---
