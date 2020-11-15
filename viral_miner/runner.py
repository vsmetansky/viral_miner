from aiohttp import ClientSession
from elasticsearch import AsyncElasticsearch

from viral_miner.collectors.covid19 import Covid19Collector


# for now it is able to run Covid19Collector only
class Runner:
    def __init__(self):
        self._session = None
        self._es = None

    async def run(self):
        try:
            await self.start()
            await self.process()
        finally:
            await self.stop()

    async def start(self):
        self._session = ClientSession()
        self._es = AsyncElasticsearch()

    # TODO add date ranges and name filtering
    async def process(self):
        await Covid19Collector(
            es=self._es,
            session=self._session
        ).collect()

    async def stop(self):
        await self._session.close()
        await self._es.close()
