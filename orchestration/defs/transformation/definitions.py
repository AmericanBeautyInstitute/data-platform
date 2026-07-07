"""Transformation layer Dagster definitions."""

from dagster import Definitions

from orchestration.defs.transformation.assets import dbt_models
from orchestration.defs.transformation.resources import dbt_resource

transformation_defs = Definitions(
    assets=[dbt_models],
    resources={"dbt": dbt_resource},
)
