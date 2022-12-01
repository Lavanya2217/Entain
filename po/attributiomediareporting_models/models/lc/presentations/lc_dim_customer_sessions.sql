SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
       ,CASE WHEN referrer LIKE '%lp/ppc/amp%' AND (ChannelGrouping LIKE '%eferral' OR ChannelGrouping ='Direct') THEN 1 ELSE 0 END AS transfo
FROM(
        SELECT distinct  'Coral' AS Brand
                ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
                ,fullVisitorId
                ,MAX(t.visitNumber) AS Number
                ,ChannelGrouping
                ,hits.page.pagePath AS landing
                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84)) AS referrer
                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM {{ source('Coral_GA', 'ga_sessions_*') }} t, t.hits AS hits
        WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
        GROUP BY 1,2,3,5,6

      UNION ALL

        SELECT distinct 'Ladbrokes' AS Brand
                ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionID
                ,fullVisitorId
                ,MAX(t.visitNumber) AS Number
                ,ChannelGrouping
                ,hits.page.pagePath AS landing
                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=84)) AS referrer
                ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}"))) AS CustomerID
        FROM {{ source('Lads_GA', 'ga_sessions_*') }} t, t.hits AS hits
        WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
        GROUP BY 1,2,3,5,6

      )
WHERE SAFE_CAST( CustomerID AS INT64) > 1  
