import argparse
import csv
from datetime import datetime, date, timedelta
import logging
from multiprocessing.pool import ThreadPool as Pool
import os
from typing import Callable, Dict, List, NoReturn, Union
import urllib

import requests
import mysql.connector

import settings as config
import queries

HTTP_OK_CODE = 200


# Generate a jobID - Just milliseconds since 01-01-2020
jobId = int((datetime.now() - datetime(2022, 1, 1, 0, 0, 0)).total_seconds() * 1000)

# Define logging file
logging.basicConfig(
    filename=config.logging_folder + "\\" + config.logging_suffix + date.today().strftime("%Y%m%d") + ".log",
    level=logging.DEBUG,
    format="%(asctime)s " + str(jobId) + " %(levelname)s %(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


# Parse arguments passed to the script
parser = argparse.ArgumentParser(description="DataSync service help menu")
parser.add_argument(
    "--mode",
    "-m",
    default="default",
    choices=["full", "delta", "range", "default"],
    type=str,
    metavar="mode",
    help="Define the load mode, either 'full', 'delta', 'default' (as defined in the parameter db), or an explicit 'range' as defined by the rangeStart and rangeEnd arguments",
)
parser.add_argument(
    "--delta",
    "-d",
    default=4,
    type=int,
    metavar="delta",
    help="Define number of days to go back to load. Defaults to 4 days",
)
parser.add_argument(
    "--rangeStart",
    "-rs",
    default=date.today(),
    type=date.fromisoformat,
    metavar="rangeStart",
    help="Define the start date for the explicit range in ISO 8601 format (YYYY-MM-DD). Defaults to the current day",
)
parser.add_argument(
    "--rangeEnd",
    "-re",
    default=date.today(),
    type=date.fromisoformat,
    metavar="rangeEnd",
    help="Define the end date for the explicit range in ISO 8601 format (YYYY-MM-DD). Defaults to the current day",
)
parser.add_argument(
    "--source",
    "-s",
    type=str,
    metavar="source",
    help="Source to pull data from",
)


# Function to connect to MySQL DB
def connectMySQL(server, db, username, password):
    taskStartTime = datetime.now()
    logger.info(f"Connecting to {db}")
    try:
        conn = mysql.connector.connect(user=username, password=password,host=server, database=db)
    except mysql.connector.Error as err:
        raise
    else:
        taskDuration = (datetime.now() - taskStartTime).total_seconds()
        logger.info(f"Connected to {db} in {taskDuration} seconds")
        return conn


# source class to interact with the sources
class source():
    # initialise database connection parameters and source name
    def __init__(self, config,sourceName):
        self.db_server = config.db_server
        self.db_name = config.db_name
        self.db_username = config.db_username
        self.db_password = config.db_password
        self.sourceName = sourceName
    # function to connect to the database
    def connectDB(self):
        return connectMySQL(self.db_server,self.db_name,self.db_username,self.db_password)
    # function to validate if source exists
    def validateSource(self):
        try:
            cnxn = self.connectDB()
            logger.info(f"Validating if {self.sourceName} source exists")
            with cnxn.cursor() as cursor:
                cursor.execute(queries.selectSource.format(sourceName=self.sourceName))
                if cursor.fetchone():
                    logger.info(f"{self.sourceName} source exists")
                    cursor.close()
                    cnxn.close()
                    return True
                else:
                    logger.warning(f"{self.sourceName} source doesn't exist!")
                    cnxn.close()
                    return False
        except Exception as err:
            raise
    # function to truncate staging table for given source
    def truncateStaging(self):
        try:
            if self.validateSource():
                cnxn = self.connectDB()
                taskStartTime = datetime.now()
                logger.info(f"Truncating {self.sourceName}_data staging table")
                with cnxn.cursor() as cursor:
                    cursor.execute(queries.truncateStaging.format(sourceName=self.sourceName))
                    cnxn.commit()
                    cursor.close()
                cnxn.close()
                taskDuration = (datetime.now() - taskStartTime).total_seconds()
                logger.info(f"Truncated {self.sourceName}_data staging table in {taskDuration} seconds")
        except Exception as err:
            raise
        return

# covid19api class to interact with the covid19api data source
class covid19api(source):
    def mergeDataIntoCases():
        return
    def getDataBetween(self,country,fromDate,toDate):
        self.truncateStaging()
        return
    def getDataAfter(self,country,afterDate):
        return


def main():
    c19api = covid19api(config,"covid19api")
    c19api.getDataBetween("au",date.today(),date.today())


main()





