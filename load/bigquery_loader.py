"""The BigQuery data Loader."""

from pathlib import Path

import pyarrow as pa
from google.cloud import bigquery

from config.loaders import BigQueryLoadConfig, BigQueryTableConfig
from helpers.bigquery import ensure_table_exists
from load.loader import Loader
from utils.parquet import table_to_parquet_buffer


class BigQueryLoader(Loader):
    """Loads data into BigQuery."""

    def load(self, data: pa.Table | Path | str, config: BigQueryLoadConfig) -> None:
        """Loads data into BigQuery table."""
        table_reference = f"{self.client.project}.{config.dataset_id}.{config.table_id}"

        if config.create_table_if_needed and isinstance(data, pa.Table):
            self._ensure_table_created(data, config)

        job_config = self._build_job_config(config)
        load_job = self._dispatch_load(data, table_reference, job_config)

        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {table_reference}")

    def _ensure_table_created(
        self,
        data: pa.Table,
        config: BigQueryLoadConfig,
    ) -> None:
        """Ensures table exists before loading data."""
        table_config = BigQueryTableConfig(
            dataset_id=config.dataset_id,
            table_id=config.table_id,
            schema=data.schema,
            partition_field=config.partition_field,
            cluster_fields=config.cluster_fields,
        )
        ensure_table_exists(self.client, table_config)

    def _build_job_config(self, config: BigQueryLoadConfig) -> bigquery.LoadJobConfig:
        """Builds BigQuery LoadJobConfig from LoadConfig."""
        return bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, config.source_format),
            write_disposition=config.write_disposition,
            autodetect=True,
        )

    def _dispatch_load(
        self,
        data: pa.Table | Path | str,
        table_reference: str,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """Dispatches to appropriate load method based on data type."""
        if isinstance(data, str) and data.startswith("gs://"):
            return self._load_from_gcs_uri(data, table_reference, job_config)
        elif isinstance(data, Path | str):
            return self._load_from_file(data, table_reference, job_config)
        elif isinstance(data, pa.Table):
            return self._load_from_arrow_table(data, table_reference, job_config)
        else:
            raise TypeError(
                f"Unsupported data type: {type(data)}. "
                "Expected pa.Table, Path, or GCS URI string."
            )

    def _load_from_gcs_uri(
        self,
        gcs_uri: str,
        table_reference: str,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """Loads data from a GCS URI."""
        return self.client.load_table_from_uri(
            gcs_uri, table_reference, job_config=job_config
        )

    def _load_from_file(
        self,
        file_path: Path | str,
        table_reference: str,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """Loads data from a local file path."""
        with Path.open(file_path, "rb") as file:
            return self.client.load_table_from_file(
                file, table_reference, job_config=job_config
            )

    def _load_from_arrow_table(
        self,
        table: pa.Table,
        table_reference: str,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """Loads data from a PyArrow table via in-memory Parquet buffer."""
        parquet_buffer = table_to_parquet_buffer(table)
        return self.client.load_table_from_file(
            parquet_buffer, table_reference, job_config=job_config
        )
