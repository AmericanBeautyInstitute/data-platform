"""Transformation layer Dagster resources."""

from dagster import EnvVar
from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource


def _build_sqlmesh_resource() -> SQLMeshResource:
    """Builds a SQLMeshResource from environment variables."""
    return SQLMeshResource(
        config=SQLMeshContextConfig(
            path=EnvVar("SQLMESH_PROJECT_DIR").get_value(),
            gateway=EnvVar("SQLMESH_GATEWAY").get_value(),
        )
    )


sqlmesh_resource = _build_sqlmesh_resource()
