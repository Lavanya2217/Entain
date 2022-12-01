WITH   BwinGA     AS(  SELECT 'Bwin' AS Brand,* FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       GalaGA     AS(  SELECT 'Gala' AS Brant,* FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') 
                                                                                                 AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy'))




SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
       ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)referral|direct') THEN 1 ELSE 0 END AS transfo
FROM(
        SELECT Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM BwinGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6

      UNION ALL
        SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                            WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                            ELSE 'Gala Spins' END AS Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM GalaGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6

      UNION ALL
        SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END        AS Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM GalaGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6

      UNION ALL
        SELECT 'Cheeky Bingo'  AS Brand
               ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
               ,fullVisitorId
               ,MAX(visitNumber) AS Number
               ,ChannelGrouping
               ,hits.page.pagePath AS landing
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM CheekyGA t, t.hits AS hits
        GROUP BY 1,2,3,5,6

     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1
