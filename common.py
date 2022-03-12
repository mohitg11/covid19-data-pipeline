from datetime import datetime
import logging

import mysql.connector

import settings as config


logger = logging.getLogger()


# function to connect to MySQL DB
def connectMySQL(server, db, username, password):
    taskStartTime = datetime.now()
    logger.info(f"Connecting to {db}")
    try:
        conn = mysql.connector.connect(user=username, password=password,host=server, database=db)
    except mysql.connector.Error as err:
        logger.error(f"Couldn't connect to {db}")
        raise
    else:
        taskDuration = (datetime.now() - taskStartTime).total_seconds()
        logger.info(f"Connected to {db} in {taskDuration} seconds")
        return conn


# function to connect to the database
def connectDB():
    return connectMySQL(config.db_server,config.db_name,config.db_username,config.db_password)