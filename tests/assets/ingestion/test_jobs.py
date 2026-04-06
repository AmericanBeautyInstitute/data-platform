"""Tests for ingestion layer job definitions."""

from assets.ingestion.jobs import ingestion_job


def test_ingestion_job_has_correct_name():
    """Job name is ingestion_job."""
    assert ingestion_job.name == "ingestion_job"
