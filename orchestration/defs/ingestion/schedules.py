"""Ingestion layer Dagster schedule definitions."""

from dagster import DailyPartitionsDefinition, build_schedule_from_partitioned_job

from orchestration.defs.ingestion.jobs import ingestion_job

SCHEDULE_NAME = "ingestion_job_schedule"
START_DATE = "2024-01-01"

daily_partitions = DailyPartitionsDefinition(
    start_date=START_DATE,
    timezone="America/New_York",
)

daily_schedule = build_schedule_from_partitioned_job(
    ingestion_job,
    name=SCHEDULE_NAME,
    hour_of_day=6,
    minute_of_hour=0,
)
