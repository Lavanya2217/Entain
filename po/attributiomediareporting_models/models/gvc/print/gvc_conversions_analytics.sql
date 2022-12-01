WITH   BwinGA     AS(  SELECT * FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       GalaGA     AS(  SELECT * FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') 
                                                                                                 AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy')),

        BWIN AS(          SELECT "Bwin"                                                                                              AS Brand
                         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                         ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
                         ,t.fullVisitorId
                         ,t.VisitId
                         ,channelGrouping
                         ,trafficSource.campaign                                                                                     AS campaign
                         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
                         ,trafficSource.source                                                                                       AS source
                         ,trafficSource.medium                                                                                       AS medium
                         ,trafficSource.adContent                                                                                    AS adContent
                         ,device.deviceCategory                                                                                      AS device
                         ,trafficSource.keyword                                                                                      AS keyword
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Registration'                                                                                             AS Conversion
                         ,NULL                                                                                                       AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,''                                                                                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM BwinGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
             UNION ALL
                   SELECT
                         "Bwin"                                                                                                      AS Brand
                         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                         ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
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
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Deposit'                                                                                                  AS Conversion
                         ,hits.eventInfo.eventValue                                                                                  AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM BwinGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23

             UNION ALL
                   SELECT "Bwin"                                                                                                     AS Brand
                         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                         ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
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
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Bet'                                                                                                      AS Conversion
                         ,IFNULL(hits.transaction.transactionRevenue/1000000, hits.eventInfo.eventValue)                             AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,hits.transaction.currencyCode                                                                              AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM BwinGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"bet place success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18,19,20,hits.eventInfo.eventValue,21,22,23


--              UNION ALL
--                    SELECT * EXCEPT(rank, eventCategory,  eventLabel, eventAction, balance, variation)
--                             REPLACE(CASE WHEN REGEXP_CONTAINS(eventCategory, "(?i)game launch") THEN ROUND(balance - variation,2) END AS event_value)
--                    FROM(
--                           SELECT *, LEAD(balance) OVER (PARTITION BY fullvisitorid ORDER BY date, event_time) AS variation
--                           FROM(
--                                 SELECT * EXCEPT(launch, newbalance) REPLACE(CASE WHEN newbalance IS NOT NULL THEN newbalance ELSE balance END AS balance)
--                                        , ROW_NUMBER () OVER(PARTITION BY fullvisitorid, date, event_time ORDER BY launch DESC) AS rank

--                                 FROM (SELECT * REPLACE(SAFE_CAST(SPLIT(balance, " ")[SAFE_OFFSET(0)] AS FLOAT64) AS balance)
--                                                ,CASE WHEN REGEXP_CONTAINS(eventCategory, "(?i)game launch") THEN 1 END AS launch
--                                                ,CASE WHEN eventAction = "egv_closegame" AND LEAD(eventAction) OVER (PARTITION BY fullvisitorid, visitid ORDER BY date, event_time) = "egv_closegame"
--                                                      THEN LEAD(SAFE_CAST(balance AS FLOAT64)) OVER (PARTITION BY fullvisitorid, visitid ORDER BY date, event_time) END AS newbalance

--                                       FROM (SELECT "Bwin"                                                                                                      AS Brand
--                                                    ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
--                                                    ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
--                                                    ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
--                                                    ,t.fullVisitorId
--                                                    ,t.VisitId
--                                                    ,channelGrouping
--                                                    ,trafficSource.campaign                                                                                     AS campaign
--                                                    ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
--                                                    ,trafficSource.source                                                                                       AS source
--                                                    ,trafficSource.medium                                                                                       AS medium
--                                                    ,trafficSource.adContent                                                                                    AS adContent
--                                                    ,device.deviceCategory                                                                                      AS device
--                                                    ,trafficSource.keyword                                                                                      AS keyword
--                                                    ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
--                                                    ,'Game Launch'                                                                                              AS Conversion
--                                                    ,hits.eventinfo.eventCategory                                                                               AS eventCategory
--                                                    ,hits.eventinfo.eventAction                                                                                 AS eventAction
--                                                    ,hits.eventinfo.eventLabel                                                                                  AS eventLabel
--                                                    ,NULL                                                                                                       AS event_value
--                                                    ,hits.transaction.transactionID                                                                             AS transactionID
--                                                    ,SPLIT((SELECT value FROM UNNEST(hits.customDimensions) WHERE index =2), " ")[SAFE_OFFSET(0)]               AS balance
--                                                    ,hits.page.hostname                                                                                         AS website
--                                                    ,UPPER(SPLIT((SELECT value FROM UNNEST(hits.customDimensions) WHERE index =2), " ")[SAFE_OFFSET(1)])        AS trans_currency
--                                                    ,hits.page.pagepath                                                                                         AS landing
--                                                    ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
--                                              FROM BwinGA t, t.hits AS hits
--                                              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,19,21,22,23,24,25,26, keyword
--                                                       ,hits.eventinfo.eventCategory
--                                                       ,hits.eventinfo.eventAction
--                                                       ,hits.eventinfo.eventLabel,hits.page.pagepath
--                                              )

--                                       WHERE balance IS NOT NULL) )
--                           WHERE rank = 1
--                          )
--                    WHERE REGEXP_CONTAINS(eventCategory, "(?i)game launch")
             ),
 Gala AS(          SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                               WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                            ELSE 'Gala Spins' END                                                                                    AS Brand
                         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                         ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
                         ,t.fullVisitorId
                         ,t.VisitId
                         ,channelGrouping
                         ,trafficSource.campaign                                                                                     AS campaign
                         ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
                         ,trafficSource.source                                                                                       AS source
                         ,trafficSource.medium                                                                                       AS medium
                         ,trafficSource.adContent                                                                                    AS adContent
                         ,device.deviceCategory                                                                                      AS device
                         ,trafficSource.keyword                                                                                      AS keyword
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Registration'                                                                                             AS Conversion
                         ,NULL                                                                                                       AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,''                                                                                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM GalaGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
             UNION ALL
                   SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                               WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                            ELSE 'Gala Spins' END                                                                                    AS Brand
                         ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                         ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                         ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
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
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Deposit'                                                                                                  AS Conversion
                         ,hits.eventInfo.eventValue                                                                                  AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM GalaGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
                   ),

foxy AS(           SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END        AS Brand
                          ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                          ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                          ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
                          ,t.fullVisitorId
                          ,t.VisitId
                          ,channelGrouping
                          ,trafficSource.campaign                                                                                     AS campaign
                          ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
                          ,trafficSource.source                                                                                       AS source
                          ,trafficSource.medium                                                                                       AS medium
                          ,trafficSource.adContent                                                                                    AS adContent
                          ,device.deviceCategory                                                                                      AS device
                          ,trafficSource.keyword                                                                                      AS keyword
                          ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                          ,'Registration'                                                                                             AS Conversion
                          ,NULL                                                                                                       AS event_value
                          ,hits.transaction.transactionID                                                                             AS transactionID
                          ,hits.page.hostname                                                                                         AS website
                          ,''                                                                                                         AS trans_currency
                          ,hits.page.pagepath                                                                                         AS landing
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                    FROM FoxyGA t, t.hits AS hits
                    WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
              UNION ALL
                    SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END       AS Brand
                          ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                          ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                          ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
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
                          ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                          ,'Deposit'                                                                                                  AS Conversion
                          ,hits.eventInfo.eventValue                                                                                  AS event_value
                          ,hits.transaction.transactionID                                                                             AS transactionID
                          ,hits.page.hostname                                                                                         AS website
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                          ,hits.page.pagepath                                                                                         AS landing
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                    FROM FoxyGA t, t.hits AS hits
                    WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
                   ),

cheeky AS(         SELECT 'Cheeky Bingo'                                                                                              AS Brand
                          ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                          ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                          ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
                          ,t.fullVisitorId
                          ,t.VisitId
                          ,channelGrouping
                          ,trafficSource.campaign                                                                                     AS campaign
                          ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
                          ,trafficSource.source                                                                                       AS source
                          ,trafficSource.medium                                                                                       AS medium
                          ,trafficSource.adContent                                                                                    AS adContent
                          ,device.deviceCategory                                                                                      AS device
                          ,trafficSource.keyword                                                                                      AS keyword
                          ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                          ,'Registration'                                                                                             AS Conversion
                          ,NULL                                                                                                       AS event_value
                          ,hits.transaction.transactionID                                                                             AS transactionID
                          ,hits.page.hostname                                                                                         AS website
                          ,''                                                                                                         AS trans_currency
                          ,hits.page.pagepath                                                                                         AS landing
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                    FROM CheekyGA t, t.hits AS hits
                    WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
              UNION ALL
                    SELECT 'Cheeky Bingo'                                                                                             AS Brand
                          ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
                          ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
                          ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
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
                          ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                          ,'Deposit'                                                                                                  AS Conversion
                          ,hits.eventInfo.eventValue                                                                                  AS event_value
                          ,hits.transaction.transactionID                                                                             AS transactionID
                          ,hits.page.hostname                                                                                         AS website
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                          ,hits.page.pagepath                                                                                         AS landing
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                          ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                    FROM CheekyGA t, t.hits AS hits
                    WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23
                   ),

 alle AS(          SELECT * FROM Bwin
                    UNION ALL
                   SELECT * FROM Gala
                    UNION ALL
                   SELECT * FROM Foxy
                    UNION ALL
                   SELECT * FROM Cheeky
                   ),

 ruru AS(
      SELECT * ,ROW_NUMBER() OVER ( PARTITION BY fullvisitorID, Conversion, Date, Event_time ORDER BY Event_time ASC) AS rank
      FROM(
          SELECT Brand
                ,Date
                ,EXTRACT(HOUR FROM event_time) AS Hour
                ,Event_time
                ,fullVisitorID
                ,ChannelGrouping
                ,LOWER(medium) AS medium
                ,LOWER(source) AS source
                ,device
                ,CAST(VisitId AS INT64)                                               AS VisitId
                ,Campaign
                ,CAST(CustomerID AS STRING)                                           AS CustomerID
                ,Conversion                                                           AS Conversion
                ,0                                                                    AS Lag_days
                ,0                                                                    AS Lag_hours
                ,0                                                                    AS View_conversion
                ,1                                                                    AS Click_conversion
                ,'Web'                                                                AS Conv_medium
                ,'GA'                                                                 AS Dataset
                ,adContent
                ,keyword
              --,CASE WHEN event_value > 100000 THEN NULL ELSE event_value END        AS event_value
                ,event_value                                                          AS event_value
                ,CASE WHEN LENGTH(trans_currency) = 3 THEN UPPER(trans_currency)  END AS currency
                ,transactionID                                                        AS transaction_id
                ,IFNULL(SAFE_CAST(tracker AS INT64),
                 SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=') AND NOT REGEXP_CONTAINS(landing, '(?i)trackerid=a_')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=') AND NOT REGEXP_CONTAINS(landing, '(?i)wm=a_')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})')))
                                END AS INT64) )                                       AS wm_tracking
                ,CampaignId
                ,landing
                ,CASE WHEN LENGTH(country) = 2 THEN country END AS country
                ,CASE WHEN REGEXP_CONTAINS(website, '(?i)gala')    THEN website
                      WHEN REGEXP_CONTAINS(website, 'bpremium.de') THEN 'bpremium.de'
                      WHEN REGEXP_CONTAINS(website, 'premium')     THEN 'premium.com'
                      WHEN REGEXP_CONTAINS(website, '.com')        THEN 'bwin.com'
                      ELSE CONCAT('bwin.', SPLIT(website, 'bwin.')[SAFE_OFFSET(1)])
                 END AS website
                ,EXTRACT(DATE FROM visitStartTime) AS visitStartDate

          FROM alle
          WHERE Conversion IS NOT NULL
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,20,21,22,23,24,25,26,27,28,29,30
        ) )
        
        
 SELECT distinct * FROM ruru       
