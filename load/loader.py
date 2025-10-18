"""The Loader abstract base class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


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
    def load(self, *args: Any, **kwargs: Any) -> None:
        """Loads data into the specified destination."""
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
