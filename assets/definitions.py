"""Root Dagster definitions registry."""

from dagster import Definitions

from assets.ingestion.definitions import ingestion_defs
from assets.transformation.definitions import transformation_defs

defs = Definitions.merge(
    ingestion_defs,
    transformation_defs,
)
