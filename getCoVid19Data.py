import argparse
from datetime import datetime, date
import logging
from multiprocessing.pool import ThreadPool as Pool
import sys

import mysql.connector

import settings as config
import queries
from casesData import casesData


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
    choices=["full", "delta", "range", "default", "merge"],
    type=str,
    metavar="mode",
    help="Define the load mode, either 'full' (full load from specified source from beginning of time), 'delta' (load number of days prior to sysdate from speified source), 'default' (load as defined in the loads db), or an explicit 'range' (as defined by the rangeStart and rangeEnd arguments from specified source, or merge (merge from the staging table he exisiting data)",
)
parser.add_argument(
    "--delta",
    "-d",
    default=4,
    type=int,
    metavar="delta",
    help="Define number of days to go back from sysdate to load. Defaults to 4 days",
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
    help="Source to pull data from. Ignored if running in default mode",
)


# main function
def main():
    try:
        logger.info("Job Started")
        cases = casesData()
        cases.truncateStaging()
        from sources.covid19api import covid19api
        c19api = covid19api()
        # c19api.getDataBetween("au",date.today(),date.today())
        c19api.getDataDelta("au",4)
        del c19api
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
    else:
        del cases
        sys.exit(0)
    finally:
        logger.info("Job Completed")


main()





