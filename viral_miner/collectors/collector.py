from abc import ABC
from csv import DictReader
from io import StringIO

from elasticsearch.helpers import async_bulk


class Collector(ABC):
    """Abstract class for data collector."""

    def __init__(self, es, session):
        self._es = es
        self._session = session
        self._url = None
        self._index_name = None

    async def collect(self):
        """Loads data, normalizes it and dumps."""
        await self._dump(self._normalize(await self._load()))

    # TODO maybe remove aiohttp...
    async def _load(self):
        """Loads data from response."""
        async with self._session.get(self._url) as response:
            return await response.text()

    def _normalize(self, loaded):
        """Normalizes loaded data."""
        return DictReader(StringIO(loaded))

    async def _dump(self, normalized):
        """Dumps data to elastic."""
        await async_bulk(self._es, normalized, index=self._index_name)
