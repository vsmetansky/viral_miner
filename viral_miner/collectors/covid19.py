from datetime import datetime, timedelta

from viral_miner.collectors.collector import Collector


class Covid19Collector(Collector):
    # TODO generating collectors from templates

    def __init__(self, es, session):
        super().__init__(es, session)
        self._delay = timedelta(days=2)
        self._index_name = 'covid19'
        self._url = self._url_from_template(
            ''
        )

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

    # TODO maybe I have to find something fancier...
    def _normalize(self, loaded):
        raw_data = tuple(super()._normalize(loaded))
        for row in raw_data:
            for k, v in row.items():
                try:
                    row[k] = float(v)
                except ValueError:
                    continue
        return raw_data

    # we fetch data from CSSEGISandData:
    # https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/{}-{}-{}.csv
    def _url_from_template(self, url):
        delivery_date = datetime.today() - self._delay
        return url.format(
            delivery_date.month,
            delivery_date.day,
            delivery_date.year
        )
