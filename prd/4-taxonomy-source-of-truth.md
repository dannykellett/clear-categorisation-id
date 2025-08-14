# 4) Taxonomy Source of Truth

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
