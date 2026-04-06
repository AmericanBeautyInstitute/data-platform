"""Dagster job definitions."""

from dagster import AssetSelection, define_asset_job

all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.all(),
)
