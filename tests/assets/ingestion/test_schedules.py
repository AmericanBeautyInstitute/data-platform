"""Tests for Dagster schedule definitions."""

from dagster import DailyPartitionsDefinition, ScheduleDefinition

from assets.ingestion.schedules import START_DATE, daily_partitions, daily_schedule


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
    assert daily_schedule.cron_schedule == "0 6 * * *"


def test_daily_schedule_is_schedule_definition():
    """daily_schedule is a ScheduleDefinition instance."""
    assert isinstance(daily_schedule, ScheduleDefinition)


def test_daily_schedule_timezone_is_new_york():
    """Schedule uses America/New_York timezone."""
    assert daily_schedule.execution_timezone == "America/New_York"
