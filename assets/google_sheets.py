"""Google Sheets Dagster asset."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.schedules import daily_partitions
from config.secrets import get_secret
from extract.google_sheets import client as sheets_client
from extract.google_sheets import extract as sheets_extract
from load.bigquery import load as bq_load
from load.config import BigQueryConfig, GCSConfig
from load.gcs import load as gcs_load

SPREADSHEET_ID_SECRET = "GOOGLE_SHEETS_SPREADSHEET_ID"
CREDENTIALS_SECRET = "GOOGLE_SHEETS_CREDENTIALS_PATH"
GCP_PROJECT_SECRET = "GCP_PROJECT_ID"
DATASET = "raw"
TABLE = "google_sheets"
SHEET_NAME = "students"


@asset(partitions_def=daily_partitions)
def google_sheets_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> None:
    """Extracts Google Sheets data and loads it into BigQuery."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    run_id = str(uuid.uuid4())
    project = get_secret(GCP_PROJECT_SECRET)

    client = sheets_client.build_client(get_secret(CREDENTIALS_SECRET))
    table = sheets_extract.extract(
        client,
        get_secret(SPREADSHEET_ID_SECRET),
        SHEET_NAME,
    )

    gcs_config = GCSConfig(
        bucket=get_secret("GCS_BUCKET"),
        source=TABLE,
        partition_date=partition_date,
        run_id=run_id,
    )
    gcs_uri = gcs_load.load(table, gcs_config, gcs.get_client())

    bq_config = BigQueryConfig(
        project=project,
        dataset=DATASET,
        table=TABLE,
        partition_date=partition_date,
    )
    rows_loaded = bq_load.load(gcs_uri, bq_config, bigquery.get_client())

    context.add_output_metadata(
        {
            "rows_loaded": rows_loaded,
            "gcs_uri": gcs_uri,
            "partition_date": partition_date.isoformat(),
        }
    )
