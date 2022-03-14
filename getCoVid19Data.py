import argparse
from datetime import datetime, date
from importlib import import_module
import logging
from multiprocessing.pool import ThreadPool as Pool
import sys

from common import connectDB
import settings as config
import queries
from casesData import casesData


# Generate a jobID - Just milliseconds since 01-01-2020
jobId = int((datetime.now() - datetime(2022, 1, 1, 0, 0, 0)).total_seconds() * 1000)

# Define logging file
logging.basicConfig(
    filename=config.logging_folder + "\\" + config.logging_suffix + date.today().strftime("%Y%m%d") + ".log",
    level=logging.INFO,
    format="%(asctime)s " + str(jobId) + " %(levelname)s %(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


# Parse arguments passed to the script
parser = argparse.ArgumentParser(description="Get Covid-19 cases data data pipeline help menu")
subparser = parser.add_subparsers(
    dest="mode", help="Mode to ingest in", required=True
)
# Source and Country parent parser
sourceCountryParser = argparse.ArgumentParser(add_help=False)
sourceCountryParser.add_argument(
    "--source",
    "-s",
    type=str,
    metavar="source",
    required=True,
    help="Source to pull data from.",
)
sourceCountryParser.add_argument(
    "--country",
    "-c",
    type=str,
    metavar="country",
    required=True,
    help="Country to pull data for.",
)
# Between parent parser
betweenParser = argparse.ArgumentParser(add_help=False)
betweenParser.add_argument(
    "--fromDate",
    "-fd",
    default=date.today(),
    type=date.fromisoformat,
    metavar="fromDate",
    required=True,
    help="Define the start date for the explicit range in ISO 8601 format (YYYY-MM-DD). Defaults to the current day",
)
betweenParser.add_argument(
    "--toDate",
    "-td",
    default=date.today(),
    type=date.fromisoformat,
    metavar="toDate",
    required=True,
    help="Define the end date for the explicit range in ISO 8601 format (YYYY-MM-DD). Defaults to the current day",
)
# Delta parent parser
deltaParser = argparse.ArgumentParser(add_help=False)
deltaParser.add_argument(
    "--delta",
    "-d",
    default=4,
    type=int,
    metavar="delta",
    required=True,
    help="Define number of days to go back from sysdate to load. Defaults to 4 days",
)
# full mode subparser
fullSubparser = subparser.add_parser(
    "full",
    parents=[sourceCountryParser],
    help="Full load from specified source from beginning of time",
)
# default mode subparser
defaultSubparser = subparser.add_parser(
    "default",
    help="Load as defined in the config loads table",
)
# merge mode subparser
mergeSubparser = subparser.add_parser(
    "merge",
    help="Merge from the staging table the exisiting data",
)
# delta mode subparser
deltaSubparser = subparser.add_parser(
    "delta",
    parents=[sourceCountryParser, deltaParser],
    help="Load number of days prior to sysdate from specified source",
)
# between mode subparser
betweenSubparser = subparser.add_parser(
    "between",
    parents=[sourceCountryParser, betweenParser],
    help="Load from fromDate to toDate arguments from specified source",
)

args = parser.parse_args()

# worker function to fetch data
def worker(params):
    loadStartTime = datetime.now()
    result = 0
    try:
        sourceName = params["sourceName"]
        dataSourceClass = getattr(import_module(f"sources.{sourceName}"), sourceName)
        dataSourceObj = dataSourceClass()
        if params["method"] =="between":
            dataSourceObj.getDataBetween(params["country"], params["fromDate"], params["toDate"])
        else:
            dataSourceObj.getDataDelta(params["country"], params["delta"])
        del dataSourceObj
        result = 1
    except Exception as err:
        logger.exception(err)
    finally:
        loadEndTime = datetime.now()
        try:
            cnxn = connectDB()
            with cnxn.cursor() as cursor:
                cursor.execute(queries.insertIntoLoadResult.format(
                    sourceName = params["sourceName"],
                    country = params["country"],
                    method = params["method"],
                    fromDate = 'NULL' if params.get("fromDate") is None else (params["fromDate"]).strftime('%Y%m%d'),
                    toDate = 'NULL' if params.get("toDate") is None else (params["toDate"]).strftime('%Y%m%d'),
                    delta = 'NULL' if params.get("delta") is None else params["delta"],
                    jobId = jobId,
                    result = result,
                    loadStartTime = loadStartTime,
                    loadEndTime = loadEndTime))
                cnxn.commit()
                cursor.close()
        except Exception as err:
            logger.exception(f"Failed to update ingestion result to the database")
    return


# main function
def main():
    jobStartTime = datetime.now()
    result = 0
    try:
        logger.info("Job Started")
        cases = casesData()
        if args.mode != "merge":
            cases.truncateStaging()
            if args.mode == "default":
                pool = Pool(config.pool_size)
                cnxn = connectDB()
                with cnxn.cursor(dictionary=True) as cursor:
                    cursor.execute(queries.getLoadedConfigs)
                    pool.map(worker,cursor.fetchall())
                    cursor.close()
                cnxn.close()
            else:
                if args.mode == "full":
                    worker({"sourceName":args.source, "country":args.country, "method":"between", "fromDate":datetime.strptime("2000-01-01", "%Y-%m-%d"), "toDate":date.today()})
                elif args.mode == "delta":
                    worker({"sourceName":args.source, "country":args.country, "method":"delta", "delta":args.delta})
                elif args.mode == "between":
                    worker({"sourceName":args.source, "country":args.country, "method":"between", "fromDate":args.fromDate, "toDate":args.toDate})
        cases.mergeDataIntoCases(jobId)
        result = 1
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
    else:
        del cases
        sys.exit(0)
    finally:
        jobEndTime = datetime.now()
        try:
            cnxn = connectDB()
            with cnxn.cursor() as cursor:
                cursor.execute(queries.insertIntoJobResult.format(
                    jobId = jobId,
                    result = result,
                    jobStartTime = jobStartTime,
                    jobEndTime = jobEndTime))
                cnxn.commit()
                cursor.close()
        except Exception as err:
            logger.exception(f"Failed to update ingestion result to the database")
        logger.info("Job Completed")


main()

# consider list comprehension
#