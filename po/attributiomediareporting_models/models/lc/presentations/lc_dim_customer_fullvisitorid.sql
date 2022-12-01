SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') AS Date_added,*
FROM(
        SELECT distinct 'Coral' AS Brand
                ,fullVisitorId
                ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}")) AS CustomerID
        --FROM `api-project-786064088220.142849218.ga_sessions_*` t, t.hits AS hits
        FROM {{ source('Coral_GA', 'ga_sessions_*') }} t, t.hits AS hits
        WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
        GROUP BY 1,2,3

      UNION ALL

        SELECT distinct 'Ladbrokes' AS Brand
                ,fullVisitorId
                ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{6}")) AS CustomerID
        --FROM `ladbrokes-big-query.199519353.ga_sessions_*` t, t.hits AS hits
        FROM {{ source('Lads_GA', 'ga_sessions_*') }} t, t.hits AS hits
        WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
        GROUP BY 1,2,3

    )
WHERE SAFE_CAST( CustomerID AS INT64) > 1  