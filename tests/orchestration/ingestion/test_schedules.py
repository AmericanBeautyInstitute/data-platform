"""Tests for Dagster schedule definitions."""

from dagster import DailyPartitionsDefinition, ScheduleDefinition

from orchestration.defs.ingestion.definitions import ingestion_defs
from orchestration.defs.ingestion.schedules import (
    SCHEDULE_NAME,
    START_DATE,
    daily_partitions,
)


def _resolve_daily_schedule() -> ScheduleDefinition:
    """Returns the resolved daily ingestion schedule."""
    repository = ingestion_defs.get_repository_def()
    return repository.get_schedule_def(SCHEDULE_NAME)


def test_daily_partitions_is_correct_type():
    """daily_partitions is a DailyPartitionsDefinition instance."""
    assert isinstance(daily_partitions, DailyPartitionsDefinition)


def test_daily_partitions_start_date():
    """Partitions start from configured start date."""
    assert daily_partitions.start.strftime("%Y-%m-%d") == START_DATE


def test_daily_partitions_timezone_is_new_york():
    """Partitions use America/New_York timezone."""
    assert daily_partitions.timezone == "America/New_York"


def test_daily_schedule_cron_is_6am():
    """Schedule runs at 6am."""
    resolved_schedule = _resolve_daily_schedule()
    assert resolved_schedule.cron_schedule == "0 6 * * *"


def test_daily_schedule_timezone_is_new_york():
    """Schedule uses America/New_York timezone."""
    resolved_schedule = _resolve_daily_schedule()
    assert resolved_schedule.execution_timezone == "America/New_York"
