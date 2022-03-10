import csv
from datetime import datetime, date, timedelta
import dateutil.parser
import logging
from multiprocessing.pool import ThreadPool as Pool
import os
from typing import Callable, Dict, List, NoReturn, Union
import urllib

import pandas as pd
import requests
from sqlalchemy import create_engine
import mysql.connector

import settings as config
import queries

HTTP_OK_CODE = 200


# Generate a jobID - Just milliseconds since 01-01-2020
jobId = int((datetime.now() - datetime(2020, 1, 1, 0, 0, 0)).total_seconds() * 1000)

# Define logging file
logging.basicConfig(
    filename=config.logging_folder + "\\" + config.logging_suffix + date.today().strftime("%Y%m%d") + ".log",
    level=logging.DEBUG,
    format="%(asctime)s " + str(jobId) + " %(levelname)s %(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


# Function to connect to MySQL DB via SQLAlchemy
def connectMySQL(server, db, username, password):
    taskStartTime = datetime.now()
    logger.info(f"Connecting to {db}")
    try:
        conn = mysql.connector.connect(user=username, password=password,host=server, database=db)
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
    else:
        taskDuration = (datetime.now() - taskStartTime).total_seconds()
        logger.info(f"Connected to {db} in {taskDuration} seconds")
        return conn

cnxn = connectMySQL(config.db_server,config.db_name,config.db_username,config.db_password)
with cnxn.cursor() as cursor:
    cursor.execute("SELECT * FROM staging.covid19api_data")
    result = cursor.fetchall()
    for row in result:
        print(row)



