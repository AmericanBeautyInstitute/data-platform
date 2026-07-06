"""Ingestion layer Dagster job definitions."""

from dagster import AssetSelection, Backoff, Jitter, RetryPolicy, define_asset_job

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    op_retry_policy=RetryPolicy(
        max_retries=3,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.FULL,
    ),
)
