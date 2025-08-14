# 13) Prompting & Output Contracts (Reference)

* System prompt encodes taxonomy and rules: choose 0..N paths, up to 4 tiers; return JSON only; keep reasoning ≤ 240 chars; avoid hallucinated tiers.
* Include light heuristics: penalize assigning many paths unless confidence high.
* Provide few‑shot examples per high‑volume branches (configurable file or DB table).

---
