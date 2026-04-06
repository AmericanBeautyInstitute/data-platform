"""Dagster schedule definitions."""

from dagster import DailyPartitionsDefinition, ScheduleDefinition

from assets.jobs import all_assets_job

START_DATE = "2024-01-01"

daily_partitions = DailyPartitionsDefinition(
    start_date=START_DATE,
    timezone="America/New_York",
)

daily_schedule = ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
)
