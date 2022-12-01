WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') )




SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
       ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)referral|direct') THEN 1 ELSE 0 END AS transfo
FROM(
        SELECT 'Party Casino' as Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM PartyCasinoGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6

      UNION ALL
        SELECT 'Party Poker' as Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM PartyPokerGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1
