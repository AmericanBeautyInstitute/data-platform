"""The Loader abstract base class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pyarrow as pa


class Loader(ABC):
    """Abstract base class for loaders."""

    def __init__(
        self,
        credentials_file_path: Path | str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the Loader."""
        self.config = config or {}
        self.credentials_file_path = Path(credentials_file_path)

    @abstractmethod
    def load(
        self,
        data: pa.Table | Path | str,
        destination: str,
    ) -> None:
        """Loads data into the specified destination."""
        pass
