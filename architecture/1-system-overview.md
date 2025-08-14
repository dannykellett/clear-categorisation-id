# 1. System Overview

ClearCategorisationIQ is a **microservice-based classification system** designed to categorize unstructured text against hierarchical taxonomies. The system follows a **reactive architecture** with clear separation of concerns and event-driven processing.

## Core Architecture Patterns
- **API-first design** with FastAPI
- **Event-driven processing** via Kafka
- **Pluggable AI providers** through adapter pattern
- **Configuration-driven taxonomy** from Google Sheets
- **Immutable data store** for audit trail

---
