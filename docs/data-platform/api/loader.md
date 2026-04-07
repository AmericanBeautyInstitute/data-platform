---
title: Loader API Reference
---

The load layer has two modules — `load/gcs` and `load/bigquery` — and a shared config module at `load/config.py`. Each asset calls both loaders in sequence: GCS first, then BigQuery.

## GCS Loader

### `load.gcs.load.load(table, config, client) → str`

Serializes a PyArrow table to Parquet (Snappy compression), uploads it to GCS, and returns the GCS URI.

| Parameter | Type | Description |
|---|---|---|
| `table` | `pa.Table` | PyArrow table to upload |
| `config` | `GCSConfig` | Bucket, source name, partition date, and run ID |
| `client` | `storage.Client` | Authenticated GCS client (from Dagster `GCSResource`) |
| **Returns** | `str` | GCS URI (`gs://bucket/path`) |

### Blob Path Convention

Files are stored at a deterministic path built by `load.gcs.partition.build_gcs_blob_path()`:

```
{source}/date={YYYY-MM-DD}/{source}-{run_id}.parquet
```

The `run_id` (a UUID) makes each upload unique. The `date=` prefix enables partition discovery by downstream tools.

## BigQuery Loader

### `load.bigquery.load.load(gcs_uri, config, client) → int`

Loads a GCS Parquet file into a BigQuery date-partitioned table and returns the row count.

| Parameter | Type | Description |
|---|---|---|
| `gcs_uri` | `str` | GCS URI of the Parquet file |
| `config` | `BigQueryConfig` | Project, dataset, table, partition date, and optional clustering fields |
| `client` | `bigquery.Client` | Authenticated BigQuery client (from Dagster `BigQueryResource`) |
| **Returns** | `int` | Number of rows loaded |

Each load targets a single partition using BigQuery's partition decorator (`project.dataset.table$YYYYMMDD`). The write disposition is `WRITE_TRUNCATE`, so rerunning a partition replaces it without duplicating data. Other partitions remain untouched.

## Configuration Models

Both configs are frozen Pydantic models defined in `load/config.py`.

### `GCSConfig`

| Field | Type | Default | Description |
|---|---|---|---|
| `bucket` | `str` | — | GCS bucket name |
| `source` | `str` | — | Source identifier (e.g., `stripe_charges`) |
| `partition_date` | `date` | — | Date for the partition path |
| `run_id` | `str` | — | UUID identifying this run |

### `BigQueryConfig`

| Field | Type | Default | Description |
|---|---|---|---|
| `project` | `str` | — | GCP project ID |
| `dataset` | `str` | — | BigQuery dataset (e.g., `raw`) |
| `table` | `str` | — | BigQuery table name |
| `partition_date` | `date` | — | Target partition date |
| `partition_field` | `str` | `"date"` | Column used for time partitioning |
| `cluster_fields` | `list[str]` | `[]` | Optional clustering columns |

## Usage in Assets

A typical ingestion asset calls both loaders in sequence:

```python
# 1. Extract
table = stripe_extract.extract(client, date_str, date_str)

# 2. Load to GCS
gcs_uri = gcs_load.load(table, gcs_config, gcs.get_client())

# 3. Load to BigQuery from GCS
rows_loaded = bq_load.load(gcs_uri, bq_config, bigquery.get_client())
```

GCS acts as the durable landing zone. BigQuery loads from GCS rather than directly from memory, so the raw Parquet file persists even if the BigQuery load fails.
