selectSource = """
    SELECT
        *
    FROM sources
    WHERE sourceName = '{sourceName}'"""

truncateStaging = """
    TRUNCATE TABLE staging.cases_data"""