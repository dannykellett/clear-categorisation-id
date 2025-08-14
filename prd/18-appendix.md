# 18) Appendix

## 18.1 Example Internal Types (Pydantic‑style)

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

## 18.2 Validation Rules (Taxonomy)

* Tiers must be contiguous from 1 → N (no gaps).
* Paths must exist in current version; unknown tier value → reject.
* Duplicate sibling names disallowed within same parent.
* Normalise case/whitespace; provide `path_key` = `tier1/tier2/tier3/tier4`.

---

**End of PRD (v0.1)**
