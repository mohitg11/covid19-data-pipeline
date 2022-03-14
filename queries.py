selectSource = """
    SELECT
        `sourceId`
    FROM `covid_data`.`sources`
    WHERE `sourceName` = '{sourceName}' AND `dateDeleted` IS NULL"""

truncateStaging = """
    TRUNCATE TABLE `staging`.`cases_data`"""

mergeLocations = """
    INSERT INTO `covid_data`.`locations` (
         `country`
        ,`countrycode`
        ,`province`
        ,`lat`
        ,`lon`
        ,`jobId`)
    SELECT *
    FROM (
        SELECT DISTINCT
             `country`
            ,`countryCode`
            ,`province`
            ,`lat`
            ,`lon`
            ,{jobId} AS `jobId`
        FROM `staging`.`cases_data`) AS nl
    ON DUPLICATE KEY UPDATE
         `country`      = nl.`country`
        ,`countryCode`  = nl.`countryCode`
        ,`province`     = nl.`province`
        ,`jobId`        = nl.`jobId`
    ;"""

mergeCaseData = """
    INSERT INTO `covid_data`.`cases` (
        `sourceId`
        ,`sourceKey`
        ,`locationId`
        ,`deaths`
        ,`recovered`
        ,`active`
        ,`date`
        ,`jobId`)
    SELECT *
    FROM (
        SELECT
            cd.`sourceId`
            ,cd.`sourceKey`
            ,l.`locationId`
            ,cd.`deaths`
            ,cd.`recovered`
            ,cd.`active`
            ,cd.`date`
            ,{jobId} AS `jobId`
        FROM `staging`.`cases_data` cd
        INNER JOIN `covid_data`.`locations` l
            ON cd.lat = l.lat AND cd.lon = l.lon) AS nc
    ON DUPLICATE KEY UPDATE
        `locationId`    = nc.`locationId`
        ,`deaths`       = nc.`deaths`
        ,`recovered`    = nc.`recovered`
        ,`active`       = nc.`active`
        ,`date`         = nc.`date`
        ,`jobId`        = nc.`jobId`
    ;"""

getLoadedConfigs = """
    SELECT
        s.`sourceName`
        ,cl.`country`
        ,cl.`method`
        ,cl.`fromDate`
        ,cl.`toDate`
        ,cl.`delta`
    FROM `catalog`.`config_loads` cl
    INNER JOIN `covid_data`.`sources` s ON cl.`sourceId` = s.`sourceId`
    WHERE cl.`dateDeleted` IS NULL;"""

insertIntoLoadResult = """
    INSERT INTO `catalog`.`load_result` (
         `sourceName`
        ,`country`
        ,`method`
        ,`fromDate`
        ,`toDate`
        ,`delta`
        ,`jobId`
        ,`result`
        ,`loadStartTime`
        ,`loadEndTime`)
    VALUES (
         '{sourceName}'
        ,'{country}'
        ,'{method}'
        ,{fromDate}
        ,{toDate}
        ,{delta}
        ,{jobId}
        ,{result}
        ,'{loadStartTime}'
        ,'{loadEndTime}');"""

insertIntoJobResult = """
    INSERT INTO `catalog`.`job_result` (
        `jobId`,
        `result`,
        `jobStartTime`,
        `jobEndTime`)
    VALUES (
        {jobId},
        {result},
        '{jobStartTime}',
        '{jobEndTime}');"""