WITH  CoralGA     AS(  SELECT * FROM {{ source('Coral_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       LadsGA     AS(  SELECT * FROM {{ source( 'Lads_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),


         Coral AS(          SELECT "Coral"                                                                                                   AS Brand
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
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                 ,'Registration'                                                                                             AS Conversion
                                 ,NULL                                                                                                       AS event_value
                                 ,hits.transaction.transactionID                                                                             AS transactionID
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                 ,hits.page.pagepath                                                                                         AS landing
                           FROM CoralGA t, t.hits AS hits
                           WHERE hits.eventinfo.eventCategory = "registration" AND hits.eventinfo.eventAction = "attempt" AND hits.eventinfo.eventLabel = "success"
                           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,19,21
                     UNION ALL
                           SELECT
                                 "Coral"                                                                                                     AS Brand
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
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                 ,'Deposit'                                                                                                  AS Conversion
                                 ,IFNULL(hits.eventInfo.eventValue,
                                         SAFE_CAST(MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=81)) AS INT64))          AS event_value
                                 ,hits.transaction.transactionID                                                                             AS transactionID
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                 ,hits.page.pagepath                                                                                         AS landing
                           FROM CoralGA t, t.hits AS hits
                           WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)FTD|deposit") AND hits.eventinfo.eventAction = "attempt" AND hits.eventinfo.eventLabel = "success"
                           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,hits.eventInfo.eventValue,19,21
        
                     UNION ALL
                           SELECT "Coral"                                                                                                    AS Brand
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
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                 ,'Bet'                                                                                                      AS Conversion
                                 ,hits.transaction.transactionRevenue/1000000                                                                AS event_value
                                 ,hits.transaction.transactionID                                                                             AS transactionID
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                 ,hits.page.pagepath                                                                                         AS landing
                           FROM CoralGA t, t.hits AS hits
                           WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"bet") AND hits.eventinfo.eventAction = "place bet" AND hits.eventinfo.eventLabel = "success"
                           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,21
                     ),
         Lads AS(           SELECT "Ladbrokes"                                                                                               AS Brand
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
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                 ,'Registration'                                                                                             AS Conversion
                                 ,NULL                                                                                                       AS event_value
                                 ,hits.transaction.transactionID                                                                             AS transactionID
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                 ,hits.page.pagepath                                                                                         AS landing
                             FROM LadsGA t, t.hits AS hits
                             WHERE (REGEXP_CONTAINS(hits.eventinfo.eventCategory, "registration") AND hits.eventinfo.eventAction = "attempt" AND hits.eventinfo.eventLabel = "success")
                                OR  REGEXP_CONTAINS(hits.page.pagePath,"^/register/success")
                             GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,19,21
        
                     UNION ALL
                             SELECT "Ladbrokes"                                                                                              AS Brand
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
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                 ,'Bet'                                                                                                      AS Conversion
                                 ,hits.transaction.transactionRevenue/1000000                                                                AS event_value
                                 ,hits.transaction.transactionID                                                                             AS transactionID
                                 ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                 ,hits.page.pagepath                                                                                         AS landing
        
                             FROM LadsGA t, t.hits AS hits
                             WHERE REGEXP_CONTAINS(hits.eventinfo.eventCategory,"bet") AND hits.eventinfo.eventAction = "place bet" AND hits.eventinfo.eventLabel = "success"
                             GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,21
        
                            ),
        
         ladsdep  AS(         SELECT * EXCEPT(rank2, dep_num) REPLACE('Deposit' AS Conversion)
                             FROM(
                                 SELECT * EXCEPT(rank), ROW_NUMBER() OVER (PARTITION BY fullvisitorID, Date, EXTRACT(HOUR FROM Event_time), EXTRACT(MINUTE FROM Event_time)
                                                                           ORDER BY dep_num DESC, event_value DESC, Conversion) AS rank2
                                 FROM(
                                      SELECT *, ROW_NUMBER() OVER (PARTITION BY fullvisitorID, Date, campaign, dep_num ORDER BY event_value DESC, Conversion) AS rank
                                      FROM(
                                            SELECT "Ladbrokes" AS Brand
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
                                                  ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                                  ,IFNULL(hits.eventinfo.eventCategory, 'Dep')                                                                AS Conversion
                                                  ,IFNULL(hits.eventInfo.eventValue,
                                                          SAFE_CAST(MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=81)) AS INT64))          AS event_value
                                                          ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=78))                              AS dep_num
                                                  ,hits.transaction.transactionID                                                                             AS transactionID
                                                  ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                                  ,hits.page.pagepath                                                                                         AS landing
                                              FROM LadsGA t, UNNEST(t.hits) AS hits
                                              WHERE
                                                   CAST(Date AS INT64) < 20200512
                                                   AND NOT REGEXP_CONTAINS(hits.page.pagePath, "withdraw")
                                                   AND((REGEXP_CONTAINS(hits.page.pagePath, 'deposit') AND REGEXP_CONTAINS(hits.page.pagePath, 'status=success')
                                                        AND REGEXP_CONTAINS(hits.page.pagePath,'action=deposit'))
                                                    OR (REGEXP_CONTAINS(hits.eventinfo.eventCategory, "eposit") AND hits.eventinfo.eventAction = "attempt" AND hits.eventinfo.eventLabel = "success" )
                                                    OR (REGEXP_CONTAINS(hits.eventinfo.eventCategory, "eposit") AND hits.eventinfo.eventAction = "success" AND hits.eventinfo.eventLabel = "1" )
                                                    OR  REGEXP_CONTAINS(hits.page.pagePath, "deposit/success"))
                                              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,hits.eventInfo.eventValue,20,22
                                            ))
                                      WHERE rank = 1)
                             WHERE rank2 = 1
                       ),
        
         ladsdep2 AS(
                        SELECT *, CASE WHEN Date > '2020-05-12' OR EXTRACT(HOUR FROM Event_time) >= 16 THEN 1 ELSE 0 END AS excl
                        FROM(
                           SELECT "Ladbrokes" AS Brand
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
                                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84))                                      AS referrer
                                ,'Deposit'                                                                                                  AS Conversion
                                ,IFNULL(hits.eventInfo.eventValue,
                                        SAFE_CAST(MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=81)) AS INT64))          AS event_value
                                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=78))                                      AS dep_num
                                ,hits.transaction.transactionID                                                                             AS transactionID
                                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                    AS CD108
                                ,hits.page.pagepath                                                                                         AS landing
                            FROM LadsGA t, UNNEST(t.hits) AS hits
                            WHERE CAST(Date AS INT64) >= 20200512
                                  AND NOT REGEXP_CONTAINS(hits.page.pagePath, "withdraw")
                                  AND (REGEXP_CONTAINS(hits.eventinfo.eventCategory, "(?i)ftd|eposit") AND hits.eventinfo.eventAction = "attempt" AND hits.eventinfo.eventLabel = "success" )
                            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,hits.eventInfo.eventValue,20,22
                            )),
        

        
         ruru AS(
              SELECT *, ROW_NUMBER() OVER ( PARTITION BY fullvisitorID, Conversion, Date, Event_time ORDER BY Event_time ASC) AS rank
              FROM(
                  SELECT Brand
                        ,Date
                        ,EXTRACT(HOUR FROM event_time) AS Hour
                        ,Event_time
                        ,fullVisitorID
                        ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, 'Referral') AND REGEXP_CONTAINS(referrer,'lp/ppc/amp') THEN 'PPC - Other' ELSE ChannelGrouping END AS ChannelGrouping
                        ,medium
                        ,source
                        ,device
                        ,keyword
                        ,CAST(VisitId AS INT64)                                               AS VisitId
                        ,Campaign
                        ,CampaignId
                        ,CAST(CustomerID AS STRING)                                           AS Customer
                        ,Conversion                                                           AS Conversion
                        ,CASE WHEN event_value > 100000 THEN NULL ELSE event_value END        AS event_value
                        ,0                                                                    AS Lag_days
                        ,0                                                                    AS Lag_hours
                        ,0                                                                    AS View_conversion
                        ,1                                                                    AS Click_conversion
                        ,'Web'                                                                AS Conv_medium
                        ,'GA'                                                                 AS Dataset
                        ,adContent
                        ,transactionID                                                        AS transaction_id
                        ,SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                        THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                                    IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                        WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                        THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                                    IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})')))
                                        END AS INT64)                                         AS wm_tracking
                        ,landing
                        ,CD108
                        ,EXTRACT(DATE FROM visitStartTime) AS visitStartDate
        
                  FROM(
                        SELECT * FROM Coral    UNION ALL
                        SELECT * FROM Lads     UNION ALL
                        SELECT * FROM ladsdep  UNION ALL
                        SELECT * EXCEPT(dep_num, excl) FROM ladsdep2 
                        )
                  WHERE Conversion IS NOT NULL
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,23,24,25,26,27,28
                ))
        
        
 SELECT distinct * FROM ruru       
