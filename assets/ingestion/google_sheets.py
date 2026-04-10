"""Google Sheets ingestion assets."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import GoogleSheetsResource, IngestionConfig
from assets.ingestion.schedules import daily_partitions
from extract.google_sheets import extract as sheets_extract
from load.bigquery import load as bq_load
from load.config import BigQueryConfig, GCSConfig
from load.gcs import load as gcs_load

SHEET_NAMES = ["students", "programs", "inventory"]
DATASET = "raw"


def build_google_sheets_asset(sheet_name: str):
    """Builds a partitioned Dagster asset for a single Google Sheet."""

    @asset(
        name=f"google_sheets_{sheet_name}_raw",
        partitions_def=daily_partitions,
        group_name="ingestion",
    )
    def _asset(
        context: AssetExecutionContext,
        gcs: GCSResource,
        bigquery: BigQueryResource,
        google_sheets: GoogleSheetsResource,
        ingestion_env: IngestionConfig,
    ) -> None:
        """Extracts a Google Sheet and loads it into GCS and BigQuery."""
        partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
        run_id = str(uuid.uuid4())

        client = google_sheets.get_client()
        table = sheets_extract.extract(
            client,
            google_sheets.spreadsheet_id,
            sheet_name,
        )

        if table.num_rows == 0:
            context.log.warning(f"Zero rows extracted for {partition_date}")
            return

        gcs_config = GCSConfig(
            bucket=ingestion_env.bucket,
            source=f"google_sheets_{sheet_name}",
            partition_date=partition_date,
            run_id=run_id,
        )
        gcs_uri = gcs_load.load(table, gcs_config, gcs.get_client())

        bq_config = BigQueryConfig(
            project=ingestion_env.project,
            dataset=DATASET,
            table=f"google_sheets_{sheet_name}",
            partition_date=partition_date,
        )
        rows_loaded = bq_load.load(gcs_uri, bq_config, bigquery.get_client())

        context.add_output_metadata(
            {
                "rows_loaded": rows_loaded,
                "gcs_uri": gcs_uri,
                "partition_date": partition_date.isoformat(),
                "sheet_name": sheet_name,
            }
        )

    return _asset


google_sheets_assets = [build_google_sheets_asset(n) for n in SHEET_NAMES]
