"""Tests for ingestion layer resources."""

from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import bigquery_resource, gcs_resource


def test_gcs_resource_is_correct_type():
    """gcs_resource is a GCSResource instance."""
    assert isinstance(gcs_resource, GCSResource)


def test_bigquery_resource_is_correct_type():
    """bigquery_resource is a BigQueryResource instance."""
    assert isinstance(bigquery_resource, BigQueryResource)
