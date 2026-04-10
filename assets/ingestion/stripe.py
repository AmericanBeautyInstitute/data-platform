"""Stripe ingestion asset."""

import uuid
from datetime import datetime

from dagster import AssetExecutionContext, asset
from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import IngestionConfig, StripeResource
from assets.ingestion.schedules import daily_partitions
from extract.stripe import extract as stripe_extract
from load.bigquery import load as bq_load
from load.config import BigQueryConfig, GCSConfig
from load.gcs import load as gcs_load

DATASET = "raw"
TABLE = "stripe_charges"


@asset(
    name="stripe_charges_raw",
    partitions_def=daily_partitions,
    group_name="ingestion",
)
def stripe_charges_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
    stripe: StripeResource,
    ingestion_env: IngestionConfig,
) -> None:
    """Extracts Stripe charges and loads them into GCS and BigQuery."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    run_id = str(uuid.uuid4())
    date_str = partition_date.isoformat()

    client = stripe.get_client()
    table = stripe_extract.extract(client, date_str, date_str)

    if table.num_rows == 0:
        context.log.warning(f"Zero rows extracted for {partition_date}")
        return

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
