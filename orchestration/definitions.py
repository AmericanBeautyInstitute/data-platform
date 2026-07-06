"""Root Dagster definitions, autoloaded from the defs/ folder."""

from pathlib import Path

import dagster as dg

defs = dg.load_from_defs_folder(project_root=Path(__file__).parent)
