from abc import ABC, abstractmethod
from datetime import datetime
import logging

import queries
from common import connectDB


logger = logging.getLogger()


# abstract class to interact with the covid-19 data sources
class source(ABC):
    # initialise database connection parameters and source name
    def __init__(self, sourceName):
        self.sourceName = sourceName
        return self.validateSource()

    # function to connect to the cases database
    def connectDB(self):
        return connectDB()

    # function to validate if source exists
    def validateSource(self):
        try:
            cnxn = connectDB()
            logger.info(f"Validating if {self.sourceName} source exists")
            with cnxn.cursor() as cursor:
                cursor.execute(queries.selectSource.format(sourceName=self.sourceName))
                if cursor.fetchone():
                    logger.info(f"{self.sourceName} source exists")
                else:
                    logger.warning(f"{self.sourceName} source doesn't exist!")
                    raise Exception(f"{self.sourceName} source doesn't exist!")
                cursor.close()
        except Exception as err:
            logger.error(f"Failed to validate {self.sourceName} source")
            raise
        else:
            cnxn.close()
            return

    # function to get data between two dates for a country
    @abstractmethod
    def getDataBetween(self,country,fromDate,toDate):
        pass

    # function to get data delta days before sysdate
    @abstractmethod
    def getDataDelta(self,country,delta):
        pass