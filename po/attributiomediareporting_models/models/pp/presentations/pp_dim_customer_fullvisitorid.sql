WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') )


SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT 'Party Casino' as Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM PartyCasinoGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1

  UNION ALL

SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT 'Party Poker' as Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM PartyPokerGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1
