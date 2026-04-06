"""Transformation layer Dagster resources."""

import os

from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource

sqlmesh_config = SQLMeshContextConfig(
    path=os.environ["SQLMESH_PROJECT_DIR"],
    gateway=os.environ["SQLMESH_GATEWAY"],
)

sqlmesh_resource = SQLMeshResource(config=sqlmesh_config)
