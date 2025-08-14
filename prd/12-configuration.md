# 12) Configuration

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
