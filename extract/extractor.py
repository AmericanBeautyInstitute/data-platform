"""The Extractor abstract base class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pyarrow as pa


class Extractor(ABC):
    """Abstract base class for extractors."""

    def __init__(
        self,
        credentials_file_path: Path | str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the Extractor."""
        self.config = config or {}
        self.credentials_file_path = Path(credentials_file_path)
        self._client: Any | None = None

    @abstractmethod
    def extract(self, **kwargs) -> pa.Table:
        """Extracts data from the source and returns it in a uniform format."""
        pass

    @property
    def client(self) -> Any:
        """Lazy-loaded authenticated client."""
        if self._client is None:
            self._client = self._authenticate()
        return self._client

    @abstractmethod
    def _authenticate(self) -> Any:
        """Authenticates and returns the client/service object."""
        pass
