selectSource = """
    SELECT
        *
    FROM sources
    WHERE sourceName = '{sourceName}'"""

truncateStaging = """
    TRUNCATE TABLE staging.{sourceName}_data"""