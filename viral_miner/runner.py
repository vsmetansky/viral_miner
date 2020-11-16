import argparse

from elasticsearch import Elasticsearch

from viral_miner.collectors.covid19 import Covid19Collector


# for now it is able to run Covid19Collector only
class Runner:
    def __init__(self):
        self._session = None
        self._es = None

    def run(self):
        try:
            self.start()
            self.process()
        finally:
            self.stop()

    def start(self):
        self._es = Elasticsearch()

    # TODO add name filtering
    def process(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('start', type=str)
        parser.add_argument('end', type=str)
        args = parser.parse_args()

        Covid19Collector(
            es=self._es,
            date_range=[args.start, args.end]
        ).collect()

    def stop(self):
        self._es.close()
