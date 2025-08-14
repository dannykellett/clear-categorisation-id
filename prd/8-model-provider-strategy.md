# 8) Model & Provider Strategy

* **Abstraction:** Use OpenAI Python SDK interface for both cloud (OpenAI) and local (Ollama) providers.
* **Routing:** Default provider/model via config; per‑request override allowed. Fallback on failure (e.g., try provider B if A 5xx).
* **Prompting:** System prompts align to taxonomy; few‑shot examples loaded per tier where available.
* **Cost**: focus on **visibility** first (usage logging, per‑request token counts). Budget enforcement is configurable but **off by default**.
* **Future:** Fine‑tune/open-source model from stored labeled data to reduce cost & latency.

## 8.1 Few‑shot prompting strategy

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
