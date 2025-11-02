"""The BigQuery data Loader."""

from pathlib import Path

import pyarrow as pa
from google.cloud import bigquery

from load.loader import Loader
from utils.parquet import table_to_parquet_buffer


class BigQueryLoader(Loader):
    """Loads data into BigQuery."""

    def load(
        self,
        data: pa.Table | Path | str,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
        source_format: str = "PARQUET",
    ) -> None:
        """Loads data into BigQuery table."""
        client = self.client
        table_reference = f"{client.project}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, source_format),
            write_disposition=write_disposition,
            autodetect=True,
        )

        if isinstance(data, str) and data.startswith("gs://"):
            load_job = self._load_from_gcs_uri(data, table_reference, job_config)
        elif isinstance(data, Path | str):
            load_job = self._load_from_file(data, table_reference, job_config)
        elif isinstance(data, pa.Table):
            load_job = self._load_from_arrow_table(data, table_reference, job_config)
        else:
            raise TypeError(
                f"Unsupported data type: {type(data)}. "
                "Expected pa.Table, Path, or GCS URI string."
            )

        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {table_reference}")

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
