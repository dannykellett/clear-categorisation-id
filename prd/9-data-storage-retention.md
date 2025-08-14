# 9) Data, Storage & Retention

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
