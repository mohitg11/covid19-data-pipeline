from datetime import datetime
import logging

import queries
from common import connectDB


logger = logging.getLogger()


# class to interact with the cases data already stored
class casesData():
    # function to truncate staging table for given source
    def truncateStaging(self):
        try:
            cnxn = connectDB()
            taskStartTime = datetime.now()
            logger.info(f"Truncating cases_data staging table")
            with cnxn.cursor() as cursor:
                cursor.execute(queries.truncateStaging)
                cnxn.commit()
                cursor.close()
            cnxn.close()
            taskDuration = (datetime.now() - taskStartTime).total_seconds()
            logger.info(f"Truncated cases_data staging table in {taskDuration} seconds")
        except Exception as err:
            logger.error(f"Failed to truncate cases_data staging table")
            raise
        return

    # function to merge data into cases & locations table
    def mergeDataIntoCases(self,jobId):
        try:
            cnxn = connectDB()
            taskStartTime = datetime.now()
            logger.info(f"Merging locations from cases_data staging table")
            with cnxn.cursor() as cursor:
                cursor.execute(queries.mergeLocations.format(jobId=jobId))
                taskDuration = (datetime.now() - taskStartTime).total_seconds()
                cnxn.commit()
                logger.info(f"Merged locations from cases_data staging table in {taskDuration} seconds")
                taskStartTime = datetime.now()
                logger.info(f"Merging cases data from cases_data staging table")
                cursor.execute(queries.mergeCaseData.format(jobId=jobId))
                taskDuration = (datetime.now() - taskStartTime).total_seconds()
                logger.info(f"Merged cases data from cases_data staging table in {taskDuration} seconds")
                cnxn.commit()
                cursor.close()
            cnxn.close()
        except Exception as err:
            logger.error(f"Failed to merge from cases_data staging table")
            raise
        return