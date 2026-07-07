"""The dbt project handle shared by the transformation resources and assets."""

from pathlib import Path

from dagster_dbt import DbtProject

_TRANSFORM_DIR = Path(__file__).parents[3] / "transform"

dbt_project = DbtProject(project_dir=_TRANSFORM_DIR)
dbt_project.prepare_if_dev()
