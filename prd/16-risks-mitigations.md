# 16) Risks & Mitigations

* **Taxonomy drift / sheet edits break structure** → Strict validation + version pinning + alerts; roll back to last good version.
* **Provider outages / cost spikes** → Multi‑provider fallback; local models; budgets + rate limits.
* **Latency variance (local models)** → Adaptive batching + concurrency tuning; model size controls.
* **Reasoning hallucination** → Constrain output schema; validate tiers; truncate reasoning; post‑validate category membership.
* **PII exposure** → Redaction pipeline; configurable storage off switch; access controls.

---
