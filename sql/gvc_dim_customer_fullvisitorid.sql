WITH   BwinGA     AS(  SELECT 'Bwin' AS Brand,* FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       GalaGA     AS(  SELECT 'Gala' AS Brant,* FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') 
                                                                                                 AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy'))


SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM BwinGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1

  UNION ALL

SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                    WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                    ELSE 'Gala Spins' END AS Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM GalaGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1

  UNION ALL

SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END        AS Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM FoxyGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1

  UNION ALL

SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT 'Cheeky Bingo'  AS Brand
               ,fullVisitorId
               ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM CheekyGA t, t.hits AS hits
        GROUP BY 1,2
     )
WHERE SAFE_CAST( CustomerID AS INT64) > 1