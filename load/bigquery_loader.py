"""The BigQuery data Loader."""

from pathlib import Path
from typing import ClassVar

import pyarrow as pa
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config.loaders import BigQueryLoadConfig, BigQueryTableConfig
from load.loader import Loader
from utils.parquet import table_to_parquet_buffer


class BigQueryLoader(Loader):
    """Loads data into BigQuery."""

    ARROW_TO_BIGQUERY_TYPE_MAP: ClassVar[dict[pa.DataType, str]] = {
        pa.int8(): "INTEGER",
        pa.int16(): "INTEGER",
        pa.int32(): "INTEGER",
        pa.int64(): "INTEGER",
        pa.uint8(): "INTEGER",
        pa.uint16(): "INTEGER",
        pa.uint32(): "INTEGER",
        pa.uint64(): "INTEGER",
        pa.float32(): "FLOAT",
        pa.float64(): "FLOAT",
        pa.string(): "STRING",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
        pa.date64(): "DATE",
    }

    def load(self, data: pa.Table | Path | str, config: BigQueryLoadConfig) -> None:
        """Loads data into BigQuery table."""
        table_reference = f"{self.client.project}.{config.dataset_id}.{config.table_id}"

        if config.create_table_if_needed and isinstance(data, pa.Table):
            self._ensure_table_exists(data, config)

        job_config = self._build_job_config(config)
        load_job = self._dispatch_load(data, table_reference, job_config)

        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {table_reference}")

    def _arrow_to_bigquery_schema(
        self, arrow_schema: pa.Schema
    ) -> list[bigquery.SchemaField]:
        """Converts PyArrow schema to BigQuery schema."""
        bigquery_schema = []
        for field in arrow_schema:
            bigquery_type = self._get_bigquery_type(field.type)
            bigquery_schema.append(
                bigquery.SchemaField(field.name, bigquery_type, mode="NULLABLE")
            )
        return bigquery_schema

    def _build_job_config(self, config: BigQueryLoadConfig) -> bigquery.LoadJobConfig:
        """Builds BigQuery LoadJobConfig from LoadConfig."""
        return bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, config.source_format),
            write_disposition=config.write_disposition,
            autodetect=True,
        )

    def _create_table_object(
        self,
        table_reference: str,
        schema: list[bigquery.SchemaField],
        config: BigQueryTableConfig,
    ) -> bigquery.Table:
        """Creates BigQuery Table object with configuration."""
        table = bigquery.Table(table_reference, schema=schema)

        if config.description:
            table.description = config.description

        if config.partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                field=config.partition_field,
                type_=bigquery.TimePartitioningType.DAY,
            )

        if config.cluster_fields:
            table.clustering_fields = config.cluster_fields

        return table

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

    def _ensure_table_exists(
        self,
        data: pa.Table,
        config: BigQueryLoadConfig,
    ) -> None:
        """Ensures table exists before loading data."""
        table_reference = f"{self.client.project}.{config.dataset_id}.{config.table_id}"
        if self._table_exists(table_reference):
            print(f"✓ Table {table_reference} exists")
            return

        table_config = BigQueryTableConfig(
            dataset_id=config.dataset_id,
            table_id=config.table_id,
            schema=data.schema,
            partition_field=config.partition_field,
            cluster_fields=config.cluster_fields,
        )

        schema = self._prepare_schema(table_config.schema)
        table = self._create_table_object(table_reference, schema, table_config)
        self.client.create_table(table)
        print(f"Created table {table_reference}")

    def _get_bigquery_type(self, arrow_type: pa.DataType) -> str:
        """Maps PyArrow type to BigQuery type string."""
        if pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        return self.ARROW_TO_BIGQUERY_TYPE_MAP.get(arrow_type, "STRING")

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

    def _prepare_schema(
        self,
        schema: list[bigquery.SchemaField] | pa.Schema,
    ) -> list[bigquery.SchemaField]:
        """Converts schema to BigQuery format if needed."""
        if isinstance(schema, pa.Schema):
            return self._arrow_to_bigquery_schema(schema)
        return schema

    def _table_exists(self, table_reference: str) -> bool:
        """Checks if table exists."""
        try:
            self.client.get_table(table_reference)
            return True
        except NotFound:
            return False
