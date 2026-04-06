"""Dagster resources for GCP infrastructure clients."""

from dagster import EnvVar
from dagster_gcp import BigQueryResource, GCSResource

gcs_resource = GCSResource(project=EnvVar("GCP_PROJECT_ID"))
bigquery_resource = BigQueryResource(project=EnvVar("GCP_PROJECT_ID"))
