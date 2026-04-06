"""Google Analytics ingestion asset."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, EnvVar, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.schedules import daily_partitions
from extract.google_analytics import client as ga_client
from extract.google_analytics import extract as ga_extract
from extract.google_analytics.extract import ReportConfig
from load.bigquery import load as bq_load
from load.config import BigQueryConfig, GCSConfig
from load.gcs import load as gcs_load

DATASET = "raw"
TABLE = "google_analytics"
REPORT_CONFIG = ReportConfig(
    dimension_names=["date", "sessionSource", "sessionMedium", "country"],
    metric_names=["sessions", "screenPageViews", "bounceRate", "conversions"],
)


@asset(
    name="google_analytics_raw",
    partitions_def=daily_partitions,
    group_name="ingestion",
)
def google_analytics_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> None:
    """Extracts Google Analytics data and loads it into GCS and BigQuery."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    run_id = str(uuid.uuid4())
    date_str = partition_date.isoformat()

    credentials_path = EnvVar("GOOGLE_ANALYTICS_CREDENTIALS_PATH").get_value()
    property_id = EnvVar("GOOGLE_ANALYTICS_PROPERTY_ID").get_value()
    project = EnvVar("GCP_PROJECT_ID").get_value()
    bucket = EnvVar("GCS_BUCKET").get_value()

    client = ga_client.build_client(credentials_path)
    table = ga_extract.extract(
        client,
        property_id,
        date_str,
        date_str,
        REPORT_CONFIG,
    )

    gcs_config = GCSConfig(
        bucket=bucket,
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
            "partition_date": date_str,
        }
    )
