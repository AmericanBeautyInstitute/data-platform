"""Google Ads ingestion asset."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import GoogleAdsResource, IngestionConfig
from assets.ingestion.schedules import daily_partitions
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
    google_ads: GoogleAdsResource,
    ingestion_env: IngestionConfig,
) -> None:
    """Extracts Google Ads data and loads it into GCS and BigQuery."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    run_id = str(uuid.uuid4())
    date_str = partition_date.isoformat()

    client = google_ads.get_client()
    table = ads_extract.extract(
        client,
        google_ads.customer_id,
        QUERY.format(date=date_str),
    )

    gcs_config = GCSConfig(
        bucket=ingestion_env.bucket,
        source=TABLE,
        partition_date=partition_date,
        run_id=run_id,
    )
    gcs_uri = gcs_load.load(table, gcs_config, gcs.get_client())

    bq_config = BigQueryConfig(
        project=ingestion_env.project,
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
