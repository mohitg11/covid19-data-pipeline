insertIntoStaging= """
    INSERT INTO staging.cases_data
    (`sourceId`,`sourceKey`,`country`,`countrycode`,`province`,`lat`,`lon`,`deaths`,`recovered`,`active`,`date`)
    VALUES ({sourceId},%(ID)s, %(Country)s, %(CountryCode)s, %(Province)s, %(Lat)s, %(Lon)s, %(Deaths)s, %(Recovered)s, %(Active)s, %(Date)s)"""