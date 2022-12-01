WITH   BwinGA     AS(  SELECT * FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231" ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231" ),
       GalaGA     AS(  SELECT * FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231" ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231" 
                                                                                              AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy')),
         excl     AS (SELECT STRING_AGG(excl, '|') FROM {{ source('exclusions_lists_lc', 'exclusion_list') }} ),
     


  Unit AS(
            SELECT "Bwin"                                                    AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,trafficSource.keyword                                    AS keyword
                   ,geonetwork.country                                       AS country

            FROM BwinGA
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11

            UNION ALL

            SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                        WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                        ELSE 'Gala Spins' END                                AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,trafficSource.keyword                                    AS keyword
                   ,geonetwork.country                                       AS country

            FROM GalaGA t, t.hits
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11

            UNION ALL

            SELECT "Cheeky Bingo"                                            AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,trafficSource.keyword                                    AS keyword
                   ,geonetwork.country                                       AS country

            FROM CheekyGA
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11

            UNION ALL

            SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' )
                        THEN 'Foxy Bingo' ELSE 'Foxy Casino' END             AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
                   ,trafficSource.keyword                                    AS keyword
                   ,geonetwork.country                                       AS country

            FROM FoxyGA t,t.hits
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11

--             UNION ALL

--             SELECT "Gala Casino"                                             AS Brand
--                    ,date                                                     AS Date
--                    ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
--                    ,fullVisitorId
--                    ,VisitId
--                    ,channelGrouping
--                    ,trafficSource.campaign                                   AS campaign
--                    ,trafficSource.source                                     AS source
--                    ,trafficSource.medium                                     AS medium
--                    ,geonetwork.country                                       AS country

--             FROM CasinoGA
--             GROUP BY 1,2,3,4,5,6,7,8,9,10
      ),


 uni AS( SELECT * REPLACE(CASE WHEN ChannelGrouping = 'Direct' THEN '(notset)' ELSE LOWER(campaign) END AS campaign,
                          CASE WHEN ChannelGrouping = 'Direct' THEN '(Direct)' ELSE source END AS source)
         FROM(
           SELECT t.* REPLACE( CASE WHEN isocode IS NOT NULL THEN REGEXP_REPLACE(TRIM(LOWER(isocode)), 'gb', 'uk')
                                        ELSE Country END AS Country,
                                   CASE WHEN Source = 'referral|facebook'                                                    THEN 'Affiliate'
                                         WHEN (REGEXP_CONTAINS(Source, '(?i)partner') AND NOT REGEXP_CONTAINS(Source, '(?i).com|.net'))
                                           OR (REGEXP_CONTAINS(Campaign,'(?i)partner') AND NOT REGEXP_CONTAINS(source,'(?i)facebook|influencer'))
                                           OR (REGEXP_CONTAINS(source, '(?i)-odds') AND REGEXP_CONTAINS(keyword, '(?i)-odds')) THEN 'Display - Partners'

                                        WHEN REGEXP_CONTAINS(Source, 'grandstand')                                           THEN 'CRM - Grandstand'
                                        WHEN REGEXP_CONTAINS(Source, '(?i)facebook')
                                         AND REGEXP_CONTAINS(Source, '(?i)crm')                                              THEN 'CRM - Social'
                                        WHEN REGEXP_CONTAINS (medium, '(?i)mail') AND campaign IS NOT NULL                   THEN 'CRM - Email'
                                        WHEN REGEXP_CONTAINS (Source, '(?i)crm' )                          THEN
                                        CASE WHEN medium = 'email'                                                           THEN 'CRM - Email'
                                             WHEN medium = 'push'                                                            THEN 'CRM - Push'
                                             ELSE 'CRM - Other'                                            END
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
                                   END AS channelgrouping)
            FROM (SELECT * REPLACE(CASE WHEN keyword <> '(not set)' AND campaign = '(not set)' AND medium = '(not set)' THEN keyword ELSE campaign END AS campaign,
                                   CASE WHEN REGEXP_CONTAINS(source, '(?i)yahoo') THEN 'Yahoo' ELSE source END AS source
                                  )
                  FROM unit) t
            LEFT JOIN {{ source('files', 'Country_isocode') }}  ON country = name)
           ),

nocp AS(    SELECT * EXCEPT( medium, sessions_count)
                   ,SUM(sessions_count) AS sessions_count

            FROM(
                    SELECT Brand, date, ChannelGrouping AS cp, LOWER(source) AS source, medium, LOWER(REGEXP_REPLACE(REGEXP_REPLACE(campaign, '  ', ' '), ' ', '')) AS Campaign
                          ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)direct|referral|organic') THEN country ELSE NULL END AS country
                          ,COUNT(0) AS sessions_count
                    FROM Uni
                    WHERE (campaign = '(notset)' AND (NOT REGEXP_CONTAINS(source, (SELECT * FROM excl)) OR source IS NULL))
                       OR (REGEXP_CONTAINS(ChannelGrouping, '(?i)crm') OR REGEXP_CONTAINS(source, '(?i)crm') OR REGEXP_CONTAINS(campaign, '(?i)crm') )
                    GROUP BY 1,2,3,4,5,6,7
                )
            GROUP by 1,2,3,4,5,6)

SELECT * REPLACE(CAST(CONCAT(SAFE.SUBSTR(Date, 1,4),'-',SAFE.SUBSTR(Date, 5,2), '-', SAFE.SUBSTR(Date, 7,2)) AS DATE) AS Date,
                 CASE WHEN cp = 'Organic - Google' THEN 'Google'
                      WHEN cp = 'Organic - Bing'   THEN 'Bing'
                      WHEN cp = 'Organic - Yahoo'  THEN 'Yahoo'
                      ELSE source END AS source)
FROM(

    SELECT Brand, date, LOWER(REGEXP_REPLACE(REGEXP_REPLACE(campaign, '  ', ' '), ' ', '')) AS cp
           ,'' AS source, campaign, CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)direct|referral|organic') THEN country ELSE NULL END AS country
           ,COUNT(0) AS sessions_count, 1 AS populated
    FROM Unit
    WHERE campaign <> '(notset)' OR (NOT REGEXP_CONTAINS(ChannelGrouping, 'crm') AND NOT REGEXP_CONTAINS(source, 'crm') AND NOT REGEXP_CONTAINS(campaign, '(?i)crm') )

    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT *, 0 AS populated  FROM nocp
    )