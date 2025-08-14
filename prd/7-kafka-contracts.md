# 7) Kafka Contracts

## 7.1 Input Topic (`scraped`)

```json
{
  "id": "uuid",
  "article_id": "string",       // preferred key for persistence
  "text": "string",
  "source": "string",            // e.g., blog, social, press_release
  "ts": "ISO-8601",
  "attrs": {"lang": "en", "author": "..."}
}
```

## 7.2 Output Topic (`scraped-classified`)

```json
{
  "id": "uuid",                  // original id
  "article_id": "string",        // for DB joinability
  "classification": [ { /* as above */ } ],
  "provider": "openai|ollama",
  "model": "string",
  "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
  "taxonomy_version": "tax_v...",
  "processed_ts": "ISO-8601"
}
```

> **Topic naming rule:** use hyphens (`-`), avoid full stops (`.`).

---
