"""BigQuery partition loader."""

from google.cloud import bigquery

from load.config import BigQueryConfig


def load(
    gcs_uri: str,
    config: BigQueryConfig,
    client: bigquery.Client,
) -> int:
    """Loads a GCS parquet file into a BigQuery date partition.

    Truncates and replaces the target partition on each run,
    making repeated loads for the same date idempotent.

    Returns the number of rows loaded.
    """
    job_config = _build_job_config(config)
    partition_ref = _build_partition_ref(config)
    job = client.load_table_from_uri(gcs_uri, partition_ref, job_config=job_config)
    job.result()
    rows_loaded = job.output_rows
    if rows_loaded is None:
        raise ValueError(
            f"BigQuery job reported no row count for "
            f"{config.project}.{config.dataset}.{config.table}"
        )
    return rows_loaded


def _build_job_config(config: BigQueryConfig) -> bigquery.LoadJobConfig:
    """Builds a BigQuery LoadJobConfig for a partitioned parquet load."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=config.partition_field,
        ),
        clustering_fields=config.cluster_fields or None,
        autodetect=False,
    )
    return job_config


def _build_partition_ref(config: BigQueryConfig) -> str:
    """Builds a BigQuery partition decorator reference.

    Format: project.dataset.table$YYYYMMDD

    The partition decorator scopes WRITE_TRUNCATE to a single
    date partition, leaving all other partitions untouched.
    """
    date_str = config.partition_date.strftime("%Y%m%d")
    partition_ref = f"{config.project}.{config.dataset}.{config.table}${date_str}"
    return partition_ref
