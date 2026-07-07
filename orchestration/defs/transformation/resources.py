"""Dagster resources for the dbt transformation layer."""

from dagster_dbt import DbtCliResource

from orchestration.defs.transformation.project import dbt_project

dbt_resource = DbtCliResource(project_dir=dbt_project)
