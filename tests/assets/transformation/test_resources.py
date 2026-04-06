"""Tests for transformation layer resources."""

import os
from unittest.mock import patch

from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource


def test_sqlmesh_resource_is_correct_type():
    """sqlmesh_resource is a SQLMeshResource instance."""
    with patch.dict(
        os.environ,
        {
            "SQLMESH_PROJECT_DIR": "/tmp/sqlmesh",
            "SQLMESH_GATEWAY": "bigquery",
        },
    ):
        config = SQLMeshContextConfig(
            path=os.environ["SQLMESH_PROJECT_DIR"],
            gateway=os.environ["SQLMESH_GATEWAY"],
        )
        resource = SQLMeshResource(config=config)

    assert isinstance(resource, SQLMeshResource)
