"""Transformation layer Dagster resources."""

from dagster import EnvVar
from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource

sqlmesh_config = SQLMeshContextConfig(
    path=EnvVar("SQLMESH_PROJECT_DIR"),
    gateway=EnvVar("SQLMESH_GATEWAY"),
)

sqlmesh_resource = SQLMeshResource(config=sqlmesh_config)
