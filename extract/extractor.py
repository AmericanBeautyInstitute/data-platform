"""The Extractor abstract base class."""

from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa


class Extractor(ABC):
    """Abstract base class for extractors."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the Extractor."""
        self.config = config or {}
        self._client = client

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pa.Table:
        """Extracts data from the source and returns it in tabular format."""
        pass

    @property
    def client(self) -> Any:
        """Returns the authenticated client."""
        return self._client
