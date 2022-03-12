insertIntoStaging= """
    INSERT INTO staging.cases_data
    (`id`,`country`,`countrycode`,`province`,`lat`,`lon`,`deaths`,`recovered`,`active`,`date`)
    VALUES (%(ID)s, %(Country)s, %(CountryCode)s, %(Province)s, %(Lat)s, %(Lon)s, %(Deaths)s, %(Recovered)s, %(Active)s, %(Date)s)"""