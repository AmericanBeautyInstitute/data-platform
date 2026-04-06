"""Google Ads ingestion asset."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, EnvVar, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.schedules import daily_partitions
from extract.google_ads import client as ads_client
from extract.google_ads import extract as ads_extract
from load.bigquery import load as bq_load
from load.config import BigQueryConfig, GCSConfig
from load.gcs import load as gcs_load

DATASET = "raw"
TABLE = "google_ads"
QUERY = """
    SELECT
        segments.date,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros,
        metrics.conversions
    FROM customer
    WHERE segments.date = '{date}'
"""


@asset(
    name="google_ads_raw",
    partitions_def=daily_partitions,
    group_name="ingestion",
)
def google_ads_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> None:
    """Extracts Google Ads data and loads it into GCS and BigQuery."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    run_id = str(uuid.uuid4())
    date_str = partition_date.isoformat()

    credentials_path = EnvVar("GOOGLE_ADS_CREDENTIALS_PATH").get_value()
    customer_id = EnvVar("GOOGLE_ADS_CUSTOMER_ID").get_value()
    project = EnvVar("GCP_PROJECT_ID").get_value()
    bucket = EnvVar("GCS_BUCKET").get_value()

    client = ads_client.build_client(credentials_path)
    table = ads_extract.extract(
        client,
        customer_id,
        QUERY.format(date=date_str),
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
