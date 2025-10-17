"""The Extractor abstract base class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl


class Extractor(ABC):
    """Abstract base class for extractors."""

    def __init__(
        self,
        credentials_file_path: str | Path,
        source: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the Extractor."""
        self.config = config or {}
        self.credentials_file_path = Path(credentials_file_path)
        self.source = source

    @abstractmethod
    def extract(self) -> pl.DataFrame:
        """Extracts data from the source and returns it in a uniform format."""
        pass
