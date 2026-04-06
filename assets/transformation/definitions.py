"""Transformation layer Dagster definitions."""

from dagster import Definitions
from dagster_sqlmesh import SQLMeshDagsterTranslator, sqlmesh_assets

from assets.transformation.resources import sqlmesh_resource


@sqlmesh_assets(
    environment="prod",
    translator=SQLMeshDagsterTranslator(),
)
def sqlmesh_all_models(context, sqlmesh):
    """Materializes all SQLMesh models via the dagster-sqlmesh integration."""
    yield from sqlmesh.run(context)


transformation_defs = Definitions(
    assets=[sqlmesh_all_models],
    resources={"sqlmesh": sqlmesh_resource},
)
