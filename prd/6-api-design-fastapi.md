# 6) API Design (FastAPI)

## 6.1 Request/Response — Single

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

## 6.2 Request/Response — Batch

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

## 6.3 Error Codes

* `400` invalid request (missing text, unsupported model, taxonomy unavailable)
* `409` taxonomy version conflict
* `429` budget/throughput limits exceeded
* `500` provider failure / unknown

## 6.4 Retrieval by `article_id`

`GET /articles/{article_id}/classifications`

* Returns latest classification document(s) for the given `article_id` from Postgres.
* Query params: `limit` (default 1), `include_usage` (bool, default true).

---
