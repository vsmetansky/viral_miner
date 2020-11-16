from abc import ABC, abstractmethod


class Collector(ABC):
    """Abstract class for data collector."""

    def __init__(self, es):
        self._es = es
        self._url = None
        self._index_name = None

    def collect(self):
        """Loads data, normalizes it and dumps."""
        return self._dump(self._normalize(self._load()))

    @abstractmethod
    def _load(self):
        """Loads data from response."""

    @abstractmethod
    def _normalize(self, loaded):
        """Normalizes loaded data."""

    @abstractmethod
    def _dump(self, normalized):
        """Dumps data to elastic."""
