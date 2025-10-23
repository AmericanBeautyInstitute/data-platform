"""The Loader abstract base class."""

from abc import ABC, abstractmethod
from typing import Any


class Loader(ABC):
    """Abstract base class for loaders."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the Loader."""
        self.config = config or {}
        self._client = client

    @abstractmethod
    def load(self, *args: Any, **kwargs: Any) -> None:
        """Loads data into the specified destination."""
        pass

    @property
    def client(self) -> Any:
        """Returns the authenticated client."""
        return self._client
