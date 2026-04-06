"""Tests for Dagster job definitions."""

from assets.jobs import all_assets_job


def test_all_assets_job_has_correct_name():
    """Job name is all_assets_job."""
    assert all_assets_job.name == "all_assets_job"
