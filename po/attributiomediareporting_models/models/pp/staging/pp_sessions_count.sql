WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
             excl     AS (SELECT STRING_AGG(excl, '|') FROM {{ source('exclusions_lists_pp', 'exclusion_list_Referrals') }} ),
     


  
  Unit AS(
            SELECT "Party Casino"                                            AS Brand
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

            FROM PartyCasinoGA
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11

            UNION ALL

            SELECT 'Party Poker'                                             AS Brand
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

            FROM PartyPokerGA t, t.hits
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
      ),


 uni AS( SELECT * REPLACE(CASE WHEN ChannelGrouping = 'Direct' THEN '(notset)' ELSE LOWER(campaign) END AS campaign,
                          CASE WHEN ChannelGrouping = 'Direct' THEN '(Direct)' ELSE source END AS source)
         FROM(
           SELECT t.* REPLACE( CASE WHEN isocode IS NOT NULL THEN REGEXP_REPLACE(TRIM(LOWER(isocode)), 'gb', 'uk')
                                        ELSE Country END AS Country
                                    , CASE
                                       WHEN REGEXP_CONTAINS(Source, '(?i)connected_tv|sky_adsmart')                                                         THEN 'Offline'
                                       WHEN REGEXP_CONTAINS(Source, '(?i)n365|outbrain|taboola')                                                                   THEN 'Display - Direct'
                                       WHEN (REGEXP_CONTAINS(Source, '(?i)vod|youtube|cpv') AND NOT REGEXP_CONTAINS(Source, '(?i)vodafone'))
                                           OR (REGEXP_CONTAINS(Campaign, '(?i)vod|youtube| yt|_yt_|cpv') AND NOT REGEXP_CONTAINS(Campaign, '(?i)vodafone'))       THEN 'Display - VOD'

                                       WHEN (REGEXP_CONTAINS(Campaign, '(?i)prog') AND NOT REGEXP_CONTAINS(Campaign, '(?i)programma'))
                                           OR (medium = 'cpm' AND REGEXP_CONTAINS(Campaign, '(?i)prog')) OR REGEXP_CONTAINS(Campaign, '(?i)programmatic')
                                           OR REGEXP_CONTAINS(Source, '(?i)dv360|appnexus|tradedesk|verizon|amobee')                                                      THEN 'Display - Programmatic'

                                       WHEN (REGEXP_CONTAINS(Source, '(?i)partner') AND NOT REGEXP_CONTAINS(Source, '(?i).com|.net'))
                                           OR (REGEXP_CONTAINS(Campaign,'(?i)partner') AND NOT REGEXP_CONTAINS(source,'(?i)facebook|influencer') and NOT REGEXP_CONTAINS(Campaign,'(?i)dcm|twitch'))
                                           OR (REGEXP_CONTAINS(source, '(?i)-odds') OR REGEXP_CONTAINS(keyword, '(?i)-odds')
                                           OR REGEXP_CONTAINS(campaign, '(?i)_odds|-odds'))
                                           OR (REGEXP_CONTAINS(source, '(?i)engageya|forza|missmarcadores|Appnetworks|dating_apps|appsflyer'))
                                           or (REGEXP_CONTAINS(Campaign,'(?i)engageya|forza|missmarcadores'))                                                      THEN 'Display - Partners'

                                       WHEN (REGEXP_CONTAINS(Source, '(?i)display') AND REGEXP_CONTAINS(Source, '(?i)other'))
                                           OR REGEXP_CONTAINS(Source, '(?i)dcm') OR REGEXP_CONTAINS(Campaign, '(?i)dcm')
                                           OR (REGEXP_CONTAINS(Campaign, '(?i)display') AND NOT REGEXP_CONTAINS(Campaign, '(?i)cpc') )                             THEN 'Display - Other'

                                       WHEN REGEXP_CONTAINS(Campaign, '(?i)uac|apple|-asa-') OR REGEXP_CONTAINS(Source, '(?i)uac|apple_ads') THEN
                                           CASE
                                               WHEN REGEXP_CONTAINS(Campaign, '(?i)Competitor')
                                                   OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'comp'                                      THEN 'UAC - Competitor'
                                               WHEN REGEXP_CONTAINS(Campaign,'(?i)Generic|search_non_brand|-gen-|non brand') OR Campaign LIKE '%|gen|%'
                                                   OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'gen'                                       THEN 'UAC - Generic'
                                               WHEN REGEXP_CONTAINS(Campaign, '(?i)Brand')
                                                   OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'bnd'                                       THEN 'UAC - Brand'
                                                                                                                                                                   ELSE 'UAC - Other'
                                       END

                                       WHEN Source = 'referral|facebook'                                                                                           THEN 'Affiliate'
                                       WHEN REGEXP_CONTAINS(Source, '(?i)grandstand')                                                                              THEN 'CRM - Grandstand'
                                       WHEN REGEXP_CONTAINS(campaign, '(?i)facebook') AND REGEXP_CONTAINS(campaign, '(?i)crm')                                     THEN 'CRM - Social'
                                       WHEN REGEXP_CONTAINS (medium, '(?i)mail') AND Campaign IS NOT NULL                                                          THEN 'CRM - Email'
                                       WHEN REGEXP_CONTAINS (Source, '(?i)crm' ) OR medium = 'push'       THEN
                                           CASE
                                               WHEN medium in ('email','inbox')                                                                                    THEN 'CRM - Email'
                                               WHEN medium = 'push'                                                                                                THEN 'CRM - Push'
                                                                                                                                                                   ELSE 'CRM - Other'
                                         END

                                       WHEN medium IN ('cpc', 'b', 'e', 'p', 'be', 'bb')
                                           OR (REGEXP_CONTAINS(Source, '(?i)oogle|ppc|bing') AND (NOT REGEXP_CONTAINS(Campaign, 'not set|notset') OR Campaign IS NULL))
                                           OR (REGEXP_CONTAINS(Source,'(?i)search_') AND NOT REGEXP_CONTAINS(Source, '(?i)coral|adbro|gala|pineapple|support'))
                                           OR (REGEXP_CONTAINS(ChannelGrouping, 'blue|Direct') AND SUBSTR(Campaign,1,1) = '3')   THEN
                                               CASE
                                                    WHEN REGEXP_CONTAINS(Campaign, r'(?i)Competitor|comp|com\_|com\+')
                                                        OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'comp')                 THEN 'PPC - Competitor'
                                                    WHEN REGEXP_CONTAINS(Campaign, r'(?i)Generic|search_non_brand|non brand|gen\_|gen\+|\|gen\|')
                                                        OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'gen' )                 THEN 'PPC - Generic'
                                                    WHEN REGEXP_CONTAINS(Campaign, r'(?i)Brand|brd\_|brd\+')
                                                        OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'bnd' )                 THEN 'PPC - Brand'
                                                                                                                                                                   ELSE 'PPC - Other'
                                           END

                                       WHEN REGEXP_CONTAINS(Source, '(?i)amp.org|ampproject.org') AND REGEXP_CONTAINS(Source, r'(?i) Gala|oral|adbrokes')          THEN 'PPC - Other'
                                       WHEN (REGEXP_CONTAINS(SPLIT(Source,'|')[SAFE_OFFSET(0)],'(?i)twitter|t.co$|snap|faceboo|youtub|instag|social|twitch')
                                            AND NOT REGEXP_CONTAINS(Source, '(?i)social.bet|social.bwin'))
                                            OR REGEXP_CONTAINS(Campaign,'(?i)twitter|snap|faceboo|youtub|instag|social')  THEN
                                               CASE WHEN Campaign <> '(not set)'                                                                                   THEN 'Social - Paid'
                                                                                                                                                                   ELSE 'Social - Organic'
                                          END

                                       WHEN medium = 'organic' THEN
                                          CASE WHEN REGEXP_CONTAINS(Source,'(?i)google')                                                                           THEN 'Organic - Google'
                                               WHEN REGEXP_CONTAINS(Source,'(?i)bing'  )                                                                           THEN 'Organic - Bing'
                                               WHEN REGEXP_CONTAINS(Source,'(?i)yahoo' )                                                                           THEN 'Organic - Yahoo'
                                                                                                                                                                   ELSE 'Organic - Other'
                                          END

                                       WHEN medium = 'Affiliate'
                                           OR SAFE_CAST(SPLIT (Source,'_') [SAFE_OFFSET(0)] AS INT64) IS NOT NULL
                                           OR REGEXP_CONTAINS(Source,'(?i)tradedoubler')                                                                           THEN 'Affiliate'
                                       WHEN REGEXP_CONTAINS(Campaign, '(?i)display') OR REGEXP_CONTAINS(Source, '(?i)display') THEN
                                           CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)prog')                                                                         THEN 'Display - Programmatic'
                                                                                                                                                                   ELSE 'Display - Other'
                                       END

                                       WHEN (ChannelGrouping IN ('Direct', 'Referral', '(Other)', 'blue') OR medium = 'referral')
                                       AND NOT REGEXP_CONTAINS(Source, 'mail')    THEN
                                            CASE
                                               WHEN REGEXP_CONTAINS(Source, '(?i)yahoo' )                                                                          THEN 'Organic - Yahoo'
                                               WHEN REGEXP_CONTAINS(Source,'(?i)googlesyndication|ads.google|doubleclick')                                         THEN 'Display - Other'
                                               WHEN REGEXP_CONTAINS(Source, '(?i)bing$|bing.com')                                                                  THEN 'Organic - Bing'
                                               WHEN REGEXP_CONTAINS(Source, '(?i)search|yandex|dogpile|duckgo')                                                    THEN 'Organic - Other'
                                               WHEN REGEXP_CONTAINS(Source, (SELECT * FROM excl))                                                                  THEN 'Direct'
                                               WHEN REGEXP_CONTAINS(Source,'(?i)googleweblight|mail.')                                                             THEN 'Referral - Other'
                                               WHEN medium = '(none)' AND Source = '(direct)'                                                                      THEN 'Direct'
                                               WHEN Campaign LIKE '%IOS|FIB%'                                                                                      THEN 'Other'
                                                                                                                                                                   ELSE 'Referral - Other'
                                             END

                                                                                                                                                                   ELSE 'Direct'
                                   END AS ChannelGrouping
                                 )
            FROM (SELECT * REPLACE(CASE WHEN keyword <> '(not set)' AND campaign = '(not set)' AND medium = '(not set)' THEN keyword ELSE campaign END AS campaign)
                  FROM unit) t
            LEFT JOIN {{ source('files', 'Country_isocode') }} ON country = name)
           ),

nocp AS(    SELECT * EXCEPT( medium, sessions_count)
                   ,SUM(sessions_count) AS sessions_count

            FROM(
                    SELECT Brand, date, ChannelGrouping AS cp, LOWER(source) AS source, medium, LOWER(REGEXP_REPLACE(REGEXP_REPLACE(campaign, '  ', ' '), ' ', '')) AS Campaign
                          ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)direct|referral|organic') THEN country ELSE NULL END AS country
                          ,COUNT(0) AS sessions_count
                    FROM Uni
                    WHERE (campaign = '(not set)' AND (NOT REGEXP_CONTAINS(source, (SELECT * FROM excl)) OR source IS NULL))
                       OR (REGEXP_CONTAINS(ChannelGrouping, '(?i)crm') OR REGEXP_CONTAINS(source, '(?i)crm') OR REGEXP_CONTAINS(campaign, '(?i)crm') )
                    GROUP BY 1,2,3,4,5,6,7
                )
            GROUP by 1,2,3,4,5,6)

SELECT * REPLACE(CAST(CONCAT(SAFE.SUBSTR(Date, 1,4),'-',SAFE.SUBSTR(Date, 5,2), '-', SAFE.SUBSTR(Date, 7,2)) AS DATE) AS Date)
FROM(

    SELECT Brand, date, LOWER(REGEXP_REPLACE(REGEXP_REPLACE(campaign, '  ', ' '), ' ', '')) AS cp
           ,'' AS source, campaign, CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)direct|referral|organic') THEN country ELSE NULL END AS country
           ,COUNT(0) AS sessions_count, 1 AS populated
    FROM Unit
    WHERE campaign <> '(not set)' OR (NOT REGEXP_CONTAINS(ChannelGrouping, 'crm') AND NOT REGEXP_CONTAINS(source, 'crm') AND NOT REGEXP_CONTAINS(campaign, '(?i)crm') )

    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT *, 0 AS populated  FROM nocp
    )
