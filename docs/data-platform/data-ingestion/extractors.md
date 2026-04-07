---
title: Extractors
---

Each data source has its own extractor package under `extract/`. Every package follows the same structure and convention.

## Package Layout

```
extract/
├── table.py              # Shared Pydantic-to-Arrow conversion
├── google_ads/
│   ├── client.py         # Builds an authenticated API client
│   └── extract.py        # Fetch, parse, and convert logic
├── facebook_ads/
│   ├── client.py
│   └── extract.py
└── ...                   # Same pattern for all six sources
```

## How an Extractor Works

Each `extract.py` module defines four things:

1. **`Raw` model** — a frozen Pydantic model that mirrors the API response. Fields map one-to-one with the source payload. No transformation happens here.
2. **`Record` model** — a frozen Pydantic model that holds validated, typed data. Field validators handle conversions: Unix timestamps to dates, cents to dollars, microcents to dollars, nested action arrays to flat fields.
3. **`fetch()` function** — calls the API, handles pagination, and returns a list of `Raw` instances.
4. **`extract()` function** — the entry point. Calls `fetch()`, parses each `Raw` into a `Record`, and passes the records to `to_table()` to produce a PyArrow table.

The flow for every source:

```
API → fetch() → list[Raw] → parse() → list[Record] → to_table() → pa.Table
```

## Shared Table Conversion

The `extract/table.py` module provides a single function used by all extractors:

```python
def to_table(records: list[BaseModel]) -> pa.Table:
    rows = [r.model_dump(mode="json") for r in records]
    return pa.Table.from_pylist(rows)
```

This replaced per-extractor conversion logic. Each extractor defines its schema through the `Record` model; `to_table()` handles serialization uniformly. Using `model_dump(mode="json")` ensures dates and other types serialize to Arrow-compatible formats.

## Client Modules

Each `client.py` exposes a `build_client()` function that takes credentials and returns an authenticated client. Dagster resources in `assets/ingestion/resources.py` call these functions, so extractors stay decoupled from the orchestration layer.

## Adding a New Source

1. Create a new package under `extract/` with `client.py` and `extract.py`.
2. Define `Raw` and `Record` Pydantic models in `extract.py`.
3. Implement `fetch()` and `extract()` following the pattern above.
4. Add a Dagster resource in `assets/ingestion/resources.py` that wraps `build_client()`.
5. Add a partitioned asset in `assets/ingestion/` that calls `extract()` and loads the result.
