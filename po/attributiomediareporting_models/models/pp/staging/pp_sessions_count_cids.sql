WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     
          trac  AS (SELECT distinct SAFE_CAST(tracker_id AS INT64) AS tracker
                    FROM {{ref('affiliates_list')}} WHERE tracker_id IS NOT NULL),

  Uni AS(
            SELECT "Party Casino"                                            AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.adwordsClickInfo.campaignId                AS campaignId
                   ,trafficSource.adContent                                  AS adContent
                   ,trafficSource.keyword                                    AS keyword
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,hits.page.pagepath                                       AS landing
                   ,geonetwork.country                                       AS country

            FROM PartyCasinoGA T, t.hits
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

            UNION ALL

            SELECT 'Party Poker'                                             AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.adwordsClickInfo.campaignId                AS campaignId
                   ,trafficSource.adContent                                  AS adContent
                   ,trafficSource.keyword                                    AS keyword
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,hits.page.pagepath                                       AS landing
                   ,geonetwork.country                                       AS country

            FROM PartyPokerGA t, t.hits
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    
      ),

  vie AS(    SELECT * REPLACE(CASE WHEN NOT REGEXP_CONTAINS(Campaign, 'c:|cid') AND REGEXP_CONTAINS(keyword, 'c:|cid') THEN keyword ELSE campaign END AS campaign)
                     ,SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                     THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                                 IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                     WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                     THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                                 IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{6})')))
                                END AS INT64) AS wm_tracking
             FROM uni)

SELECT * REPLACE(CAST(CONCAT(SAFE.SUBSTR(Date, 1,4),'-',SAFE.SUBSTR(Date, 5,2), '-', SAFE.SUBSTR(Date, 7,2)) AS DATE) AS Date, CAST(campaignid AS STRING) AS campaignid,
                 CASE WHEN isocode IS NOT NULL THEN REGEXP_REPLACE(TRIM(LOWER(isocode)), 'gb', 'uk') ELSE Country END AS Country)
         ,CONCAT(fullVisitorId, '_', CAST(visitid AS STRING)) AS sessionid,
          CASE WHEN campaignID IS NOT NULL THEN 'ppc'
               WHEN cid IS NOT NULL AND SAFE_CAST(cid AS INT64) NOT IN (52840,00000) THEN 'cid'
               WHEN (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL) THEN 'name'
          END AS join_type
FROM(
      SELECT *, CASE WHEN REGEXP_CONTAINS(Campaign, 'c:') THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64)
                     WHEN REGEXP_CONTAINS(adcontent,'c:') THEN SAFE_CAST(SUBSTR(SPLIT(adcontent,'c:')[SAFE_OFFSET(1)],1,5) AS INT64)
                     WHEN SAFE_CAST(wm_tracking AS INT64) IN (SELECT distinct tracker FROM trac)
                     THEN SAFE_CAST(wm_tracking AS INT64)
                     END AS cid
      FROM vie)
LEFT JOIN {{ source('files', 'Country_isocode') }} ON country = name