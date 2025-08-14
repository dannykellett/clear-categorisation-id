# 10) Quality, Evaluation & Metrics

## 10.1 Functional Quality

* Precision/Recall/F1 on a seed test set (≥100 labeled examples) per tier.
* Human spot‑checks of `reasoning` quality (readable, references signal).

## 10.2 Runtime Metrics (Prometheus)

* **De-scoped for MVP**: Not a priority now. No Prometheus export. Capture latency, token usage, and costs via structured logs only (see §11 Observability).

## 10.3 Success Metrics (Product)

* ≥ 85% top‑path accuracy on curated validation set by end of v1.
* < 1% classification drop due to taxonomy sync errors over 30 days.
* ≤ 2 engineer‑hours/week on ops after month 1.

---
