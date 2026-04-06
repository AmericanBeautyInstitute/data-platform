"""Transformation layer Dagster definitions."""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions
from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource, sqlmesh_assets

_SQLMESH_PROJECT_DIR = str(Path(__file__).parent.parent.parent / "transform")

_sqlmesh_config = SQLMeshContextConfig(
    path=_SQLMESH_PROJECT_DIR,
    gateway="bigquery",
)

sqlmesh_resource = SQLMeshResource(config=_sqlmesh_config)


@sqlmesh_assets(environment="prod", config=_sqlmesh_config)
def sqlmesh_all_models(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    """Materializes all SQLMesh models via the dagster-sqlmesh integration."""
    yield from sqlmesh.run(context)


transformation_defs = Definitions(
    assets=[sqlmesh_all_models],
    resources={"sqlmesh": sqlmesh_resource},
)
