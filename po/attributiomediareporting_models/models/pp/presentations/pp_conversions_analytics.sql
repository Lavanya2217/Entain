WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),


 PartyCasino AS(          SELECT "Party Casino"                                                                                      AS Brand
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
                         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Registration'                                                                                             AS Conversion
                         ,NULL                                                                                                       AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,''                                                                                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM PartyCasinoGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,18,19,20,21,22,23,24
             UNION ALL
                   SELECT
                         "Party Casino"                                                                                              AS Brand
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
                         ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
                         ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                         ,'Deposit'                                                                                                  AS Conversion
                         ,hits.eventInfo.eventValue                                                                                  AS event_value
                         ,hits.transaction.transactionID                                                                             AS transactionID
                         ,hits.page.hostname                                                                                         AS website
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                         ,hits.page.pagepath                                                                                         AS landing
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                         ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                   FROM PartyCasinoGA t, t.hits AS hits
                   WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,18,19,20,21,22,23,24
                 ),
PartyPoker AS(          SELECT "Party Poker"                                                                                        AS Brand
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
                        ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
                        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                        ,'Registration'                                                                                             AS Conversion
                        ,NULL                                                                                                       AS event_value
                        ,hits.transaction.transactionID                                                                             AS transactionID
                        ,hits.page.hostname                                                                                         AS website
                        ,''                                                                                                         AS trans_currency
                        ,hits.page.pagepath                                                                                         AS landing
                        ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                        ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                  FROM PartyPokerGA t, t.hits AS hits
                  WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,18,19,20,21,22,23,24
            UNION ALL
                  SELECT
                        "Party Poker"                                                                                               AS Brand
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
                        ,trafficSource.adWordsClickInfo.gclid                                                                       AS gclid
                        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
                        ,'Deposit'                                                                                                  AS Conversion
                        ,hits.eventInfo.eventValue                                                                                  AS event_value
                        ,hits.transaction.transactionID                                                                             AS transactionID
                        ,hits.page.hostname                                                                                         AS website
                        ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=110 )                                         AS trans_currency
                        ,hits.page.pagepath                                                                                         AS landing
                        ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
                        ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
                  FROM PartyPokerGA t, t.hits AS hits
                  WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)depos") AND REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)success")
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,18,19,20,21,22,23,24
                ),
 alle AS(          SELECT * FROM PartyCasino
                    UNION ALL
                   SELECT * FROM PartyPoker
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
                ,regexp_extract(website, 'party.*')     website
                ,EXTRACT(DATE FROM visitStartTime) AS visitStartDate
                ,gclid

          FROM alle
          WHERE Conversion IS NOT NULL
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,20,21,22,23,24,25,26,27,28,29,30,31
        ) )
        
 SELECT distinct * FROM ruru       
