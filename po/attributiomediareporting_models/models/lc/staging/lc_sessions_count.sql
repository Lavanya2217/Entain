WITH  CoralGA     AS(  SELECT * FROM {{ source('Coral_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231"),
       LadsGA     AS(  SELECT * FROM {{ source( 'Lads_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231"),
         excl     AS (SELECT STRING_AGG(excl, '|') FROM {{ source('exclusions_lists_lc', 'exclusion_list') }} ),
     
     
  Uni AS(   
            SELECT "Coral"                                                   AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
            
            FROM CoralGA
            GROUP BY 1,2,3,4,5,6,7,8,9
            
            UNION ALL 
            
            SELECT "Ladbrokes"                                               AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
            
            FROM LadsGA
            GROUP BY 1,2,3,4,5,6,7,8,9
            
      ),
     

nocp AS(    SELECT * EXCEPT(medium, sessions_count)
                     REPLACE( CASE WHEN REGEXP_CONTAINS (source, 'crm' )  THEN 'CRM - Other'
                                   WHEN REGEXP_CONTAINS (medium, 'cpc' ) OR source IN ('search_google', 'search_bing')  THEN 'PPC - Other'
                                   WHEN REGEXP_CONTAINS (source, '(?i)twitter|t.co$|snap|faceboo|youtub|instag')        THEN 'Social - Other' 
                                   WHEN REGEXP_CONTAINS (source, 'googleweblight|mail.')                                THEN 'Referral - Other'
                                   WHEN medium = 'Affiliate'
                                     OR SAFE_CAST(SPLIT (source, '_') [SAFE_OFFSET(0)] AS INT64) IS NOT NULL
                                     OR REGEXP_CONTAINS (source, 'tradedoubler')                                        THEN 'Affiliate'
                                   WHEN source = 'grandstand'                                                           THEN 'CRM - Grandstand'
                                   WHEN REGEXP_CONTAINS (source, 'coral|adbro|gala')                                    THEN 'Direct'
                                   WHEN medium = 'organic'                                             THEN
                                   CASE WHEN source = 'google'                                                          THEN 'Organic - Google'
                                        WHEN source = 'bing'                                                            THEN 'Organic - Bing'
                                        WHEN source = 'yahoo'                                                           THEN 'Organic - Yahoo'
                                        ELSE 'Organic - Other'                                         END
                                   WHEN medium = 'referral'                                            THEN            
                                   CASE WHEN REGEXP_CONTAINS(source, 'yahoo' )                                          THEN 'Organic - Yahoo'
                                        WHEN REGEXP_CONTAINS(source,            
                                             'googlesyndication|ads.google|doubleclick|taboola')                        THEN 'Display - Other'
                                        WHEN REGEXP_CONTAINS(source, 'google')                                          THEN 'Organic - Google'
                                        WHEN REGEXP_CONTAINS(source, 'bing$|bing.com')                                  THEN 'Organic - Bing'
                                        WHEN REGEXP_CONTAINS(source, 'search|yandex|dogpile|duckgo')                    THEN 'Organic - Other'
                                        ELSE 'Referral - Other'                                         END
                                  ELSE 'Direct'
                              END AS cp)
                              ,SUM(sessions_count) AS sessions_count
            
            FROM(
                    SELECT Brand, date, ChannelGrouping AS cp, source, medium, COUNT(0) AS sessions_count
                    FROM Uni 
                    WHERE campaign = '(not set)' AND (NOT REGEXP_CONTAINS(source, (SELECT * FROM excl)) OR REGEXP_CONTAINS(source, 'coral|adbro|gala') OR source IS NULL)
                    GROUP BY 1,2,3,4,5
                )
            GROUP by 1,2,3,4)

SELECT * REPLACE(CAST(CONCAT(SAFE.SUBSTR(Date, 1,4),'-',SAFE.SUBSTR(Date, 5,2), '-', SAFE.SUBSTR(Date, 7,2)) AS DATE) AS Date)
FROM(

    SELECT Brand, date, REGEXP_REPLACE(REGEXP_REPLACE(campaign, '  ', ' '), ' ', '') AS cp
           ,'' AS source, COUNT(0) AS sessions_count, 1 AS populated
    FROM Uni WHERE campaign <> '(not set)'
    GROUP BY 1,2,3
    
    UNION ALL
    
    SELECT *, 0 AS populated  FROM nocp
    )
   
   
