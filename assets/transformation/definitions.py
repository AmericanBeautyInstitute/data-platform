"""Transformation layer Dagster definitions."""

from dagster import Definitions

from assets.transformation.resources import sqlmesh_resource

transformation_defs = Definitions(
    assets=[],
    resources={"sqlmesh": sqlmesh_resource},
)
