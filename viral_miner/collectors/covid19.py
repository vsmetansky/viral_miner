import logging
import urllib
import uuid

import pandas as pd
from elasticsearch.helpers import bulk

from viral_miner.collectors.collector import Collector

logger = logging.getLogger(__name__)


class Covid19Collector(Collector):
    # TODO generating collectors from templates

    def __init__(self, es, date_range):
        super().__init__(es)
        self._index_name = 'covid19'
        self._date_range = pd.date_range(*date_range)
        self._url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/{}-{}-{}.csv'

    # structure of fetched csv:
    #     "Province_State": "Alabama",
    #     "Country_Region": "US",
    #     "Last_Update": "2020-11-14 05:30:30",
    #     "Lat": "32.3182",
    #     "Long_": "-86.9023",
    #     "Confirmed": "213617",
    #     "Deaths": "3231",
    #     "Recovered": "88038.0",
    #     "Active": "122348.0",
    #     "FIPS": "1.0",
    #     "Incident_Rate": "4356.698758052164",
    #     "Total_Test_Results": "1440875.0",
    #     "People_Hospitalized": "",
    #     "Case_Fatality_Ratio": "1.5125200709681346",
    #     "UID": "84000001",
    #     "ISO3": "USA",
    #     "Testing_Rate": "29386.511012739684",
    #     "Hospitalization_Rate": ""

    def _load(self):
        for ts in self._date_range.array:
            try:
                yield pd.read_csv(self._url.format(ts.month, ts.day, ts.year))
            except urllib.error.HTTPError as e:
                logger.warning(f'[{ts.isoformat()}] Probably GitHub DDOS protection: ' + str(e))

    def _normalize(self, loaded):
        for ts, df in zip(self._date_range.array, loaded):
            df.fillna(0, inplace=True)
            df['timestamp'] = ts.isoformat()
            yield df

    def _dump(self, normalized):
        for df in normalized:
            bulk(self._es, self._df_to_actions(df), index=self._index_name)

    def _df_to_actions(self, df):
        for record in df.to_dict(orient='records'):
            yield {
                '_id': self._get_id(record),
                '_source': record
            }

    def _get_id(self, record):
        return uuid.uuid5(uuid.NAMESPACE_OID, record['Province_State'] + record['timestamp'])
