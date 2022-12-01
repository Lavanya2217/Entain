WITH  CoralGA     AS(  SELECT * FROM {{ source('Coral_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       LadsGA     AS(  SELECT * FROM {{ source( 'Lads_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') )
--        BwinGA  AS(  SELECT * FROM `api-project-786064088220.225824617.ga_sessions_*`           WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"),
--      CheekyGA  AS(  SELECT * FROM `api-project-786064088220.232969778.ga_sessions_*`           WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"),
--        GalaGA  AS(  SELECT * FROM `api-project-786064088220.233143389.ga_sessions_*`           WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"),
--  PartyCasinoGA AS(  SELECT * FROM `api-project-786064088220.233135628.ga_sessions_*`           WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"),
--   PartyPokerGA AS(  SELECT * FROM `api-project-786064088220.234222458.ga_sessions_*`           WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"),
--        FoxyGA  AS(  SELECT * FROM `api-project-786064088220.233118966.ga_sessions_*` t, t.hits WHERE _TABLE_SUFFIX BETWEEN "20210101" AND "20221231"
--                                                                                                  AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy'))

--     SELECT
--         "Party Casino"                                                                                              AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM PartyCasinoGA t, t.hits AS hits
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16

-- UNION ALL
-- SELECT
--         "Party Poker"                                                                                               AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM PartyPokerGA t, t.hits AS hits
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16

-- UNION ALL
--     SELECT
--         "Bwin"                                                                                                      AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM BwinGA t, t.hits AS hits
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16


-- UNION ALL
--     SELECT
--         CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
--                 WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
--              ELSE 'Gala Spins' END                                                                                  AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM GalaGA t, t.hits AS hits
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16


-- UNION ALL

--     SELECT
--         CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END        AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM FoxyGA t, t.hits AS hits
--     where REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy')
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16

-- UNION ALL
--     SELECT
--         "Cheeky Bingo"                                                                                              AS Brand
--         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--         ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
--         ,fullVisitorId
--         ,VisitId
--         ,channelGrouping
--         ,trafficSource.campaign                                                                                     AS campaign
--         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--         ,trafficSource.source                                                                                       AS source
--         ,trafficSource.medium                                                                                       AS medium
--         ,trafficSource.adContent                                                                                    AS adContent
--         ,device.deviceCategory                                                                                      AS device
--         ,trafficSource.keyword                                                                                      AS keyword
--         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
--         ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
--         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--         ,max(hits.page.hostname)                                                                                    AS website
--         , 'GBP'																                                                                      AS currency
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
--         ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
--     FROM CheekyGA t, t.hits AS hits
--     GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16

-- UNION ALL
    SELECT
        "Coral"                                                                                                     AS Brand
        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
        ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
        ,fullVisitorId
        ,VisitId
        ,channelGrouping
        ,trafficSource.campaign                                                                                     AS campaign
        ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
        ,trafficSource.source                                                                                       AS source
        ,trafficSource.medium                                                                                       AS medium
        ,trafficSource.adContent                                                                                    AS adContent
        ,device.deviceCategory                                                                                      AS device
        ,trafficSource.keyword                                                                                      AS keyword
        ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
        ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
        ,max(hits.page.hostname)                                                                                    AS website
        , 'GBP'																                                                                      AS currency
        ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
        ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
    FROM CoralGA t, t.hits AS hits
    GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16

UNION ALL
    SELECT
        "Ladbrokes"                                                                                                 AS Brand
        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
        ,MAX(EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))              AS visitEndTime
        ,fullVisitorId
        ,VisitId
        ,channelGrouping
        ,trafficSource.campaign                                                                                     AS campaign
        ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
        ,trafficSource.source                                                                                       AS source
        ,trafficSource.medium                                                                                       AS medium
        ,trafficSource.adContent                                                                                    AS adContent
        ,device.deviceCategory                                                                                      AS device
        ,trafficSource.keyword                                                                                      AS keyword
        ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
        ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adGroupId
        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
        ,max(hits.page.hostname)                                                                                    AS website
        , 'GBP'																                                                                      AS currency
        ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   ))                                    AS country
        ,max((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  ))                                    AS tracker
    FROM LadsGA t, t.hits AS hits
    GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16
