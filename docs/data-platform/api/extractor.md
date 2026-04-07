---
title: Extractor API Reference
---

Every extractor module under `extract/<source>/extract.py` exposes the same public interface.

## `extract(client, start_date, end_date) → pa.Table`

Entry point. Fetches raw data from the API, validates each record, and returns a PyArrow table.

| Parameter | Type | Description |
|---|---|---|
| `client` | Source SDK client | Authenticated client from `build_client()` |
| `start_date` | `str` | ISO date string (`YYYY-MM-DD`) for the start of the range |
| `end_date` | `str` | ISO date string (`YYYY-MM-DD`) for the end of the range |
| **Returns** | `pa.Table` | PyArrow table of validated records |

Internally calls `fetch()` then `parse()` for each row, then passes the results to `to_table()`.

## `fetch(client, start_date, end_date) → list[Raw]`

Calls the source API, handles pagination, and returns raw response data as Pydantic models.

| Parameter | Type | Description |
|---|---|---|
| `client` | Source SDK client | Authenticated client |
| `start_date` | `str` | ISO date string |
| `end_date` | `str` | ISO date string |
| **Returns** | `list[Raw]` | List of frozen Pydantic models mirroring the API response |

## `parse(raw) → Record`

Converts a single `Raw` instance into a validated `Record`. Field validators run during construction to handle type conversions (timestamps to dates, cents to dollars, etc.).

| Parameter | Type | Description |
|---|---|---|
| `raw` | `Raw` | A single raw API response model |
| **Returns** | `Record` | Validated, typed record |

## Models

### `Raw`

Frozen Pydantic model (`ConfigDict(frozen=True)`) that mirrors the source API response. Fields use Python primitives — no conversion logic. One `Raw` instance represents one record from the API.

### `Record`

Frozen Pydantic model that holds clean, typed data ready for Arrow conversion. Uses `AliasChoices` to accept both raw field names and final field names, and `field_validator` decorators for conversions. Examples:

- Unix timestamps → `date`
- Integer cents → `float` dollars
- Microcents → `float` dollars
- Nested action arrays → flat integer fields

## Client Module

Each `extract/<source>/client.py` exposes one function:

### `build_client(credentials...) → Client`

Takes source-specific credentials (API key, OAuth tokens, service account path) and returns an authenticated SDK client. Dagster resources call this function; extractor code receives the client as a parameter.

## Shared Utility

### `extract.table.to_table(records) → pa.Table`

Converts a list of Pydantic `BaseModel` instances to a PyArrow table.

| Parameter | Type | Description |
|---|---|---|
| `records` | `list[BaseModel]` | Validated `Record` instances |
| **Returns** | `pa.Table` | PyArrow table; empty table if input is empty |

Serializes each record with `model_dump(mode="json")` to ensure dates and other types produce Arrow-compatible values.
