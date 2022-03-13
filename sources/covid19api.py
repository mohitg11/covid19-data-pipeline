from datetime import datetime, date, timedelta
import logging

import requests

from source import source
from sources.covid19api_settings import covid19api_base_url
from sources.covid19api_queries import insertIntoStaging


logger = logging.getLogger()


# covid19api class to interact with the covid19api data source
class covid19api(source):
    # initiliaze class as a child of source class
    def __init__(self):
        super().__init__("covid19api")

    # function to get data from the api and write to staging table
    def getData(self,url):
        try:
            taskStartTime = datetime.now()
            logger.info(f"Fetching data from covid19api from {url}")
            json_data = requests.get(url).json()
            for row in json_data:
                row["Date"]=datetime.strptime(row["Date"], "%Y-%m-%dT%H:%M:%SZ")
            taskDuration = (datetime.now() - taskStartTime).total_seconds()
            logger.info(f"Fetched data from covid19api from {url} in {taskDuration} seconds")
            taskStartTime = datetime.now()
            logger.info(f"Writing to cases_data table")
            cnxn = self.connectDB()
            with cnxn.cursor() as cursor:
                cursor.executemany(insertIntoStaging.format(sourceId=self.sourceId), json_data)
                cnxn.commit()
                cursor.close()
            taskDuration = (datetime.now() - taskStartTime).total_seconds()
            logger.info(f"Wrote to cases_data table")
            cnxn.close()
        except Exception as err:
            logger.error(f"Failed to ingest data from {url}")
            raise
        return

    # function to get data between two dates for a country
    def getDataBetween(self,country,fromDate,toDate):
        return self.getData(f"{covid19api_base_url}/country/{country}?from={fromDate}T00:00:00Z&to={toDate}T00:00:00Z")

    # function to get data delta days before sysdate
    def getDataDelta(self,country,delta):
        return self.getData(f"{covid19api_base_url}/live/country/{country}/status/confirmed/date/{date.today()-timedelta(days=delta)}T00:00:00Z")