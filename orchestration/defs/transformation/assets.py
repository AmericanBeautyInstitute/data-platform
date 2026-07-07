"""Dagster assets for the dbt transformation layer."""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from orchestration.defs.transformation.project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Builds every dbt model, running the staging and mart layers."""
    yield from dbt.cli(["build"], context=context).stream()
