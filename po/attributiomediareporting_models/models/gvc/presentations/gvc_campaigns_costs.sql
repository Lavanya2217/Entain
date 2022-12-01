WITH

offl AS(      SELECT Brand, CAST(date AS STRING) AS Date, 'Offline' AS ChannelGrouping, Channel AS Publisher,
                      '' AS campaign_id, '' AS campaign_name, UPPER(currency) AS currency, Spend,
                       0 AS Clicks, Reach AS Impressions, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2021-01-01' AS DATE) AS Start, 'offline_costs' AS dataset, country AS campaign, '' AS code,
               FROM {{ source('offline_marketing_ukgaming', 'Offline_costs') }}
               WHERE REGEXP_CONTAINS(Brand, '(?i)Bwin|Gala|foxy')
                 AND Date >= '2021-01-01'),

affi AS(      SELECT Brand_aff, CAST(date_aff AS STRING) AS Date, 'Affiliate' AS ChannelGrouping, "" AS Publisher,
                     tracker_id AS campaign_id, tracker_id AS campaign_name, 'GBP' AS currency, Costs AS Spend,
                       0 AS Clicks, 0 AS Impressions, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2021-01-01' AS DATE) AS Start, 'aff_costs' AS dataset, '' AS campaign, '' AS code,
               FROM {{ref('costs_affiliates')}}
               WHERE REGEXP_CONTAINS(Brand_aff,'(?i)gala|cheek|Bwin')
                 AND EXTRACT(YEAR FROM Date_aff) >= 2021
                 AND ABS(Costs)>0 ),


rety AS(      SELECT * REPLACE(REGEXP_REPLACE(Publisher, 'DCMM_', '') AS Publisher), REGEXP_REPLACE(LOWER(campaign_name), ' ', '') AS campaign
              FROM(
                  SELECT Brand
                         ,FORMAT_DATE("%Y-%m-%d", CAST(Date AS DATE)) as Date
                         ,CASE WHEN REGEXP_CONTAINS(Publisher, 'DCMM_') OR (NOT REGEXP_CONTAINS(Brand, 'Bwin') AND ChannelGrouping = 'Display - Partners') THEN ChannelGrouping
                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)partner')                                                           THEN 'Display - Partners'
                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod|youtube|cpv') OR REGEXP_CONTAINS(publisher, '(?i)vod|youtube')  THEN 'Display - VOD'
                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)prog') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)programma_')     THEN 'Display - Programmatic'

                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)uac|apple') OR REGEXP_CONTAINS(Publisher, '(?i)uac|apple') THEN
                               CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'UAC - Competitor'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'),  campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                      OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'UAC - Generic'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'UAC - Brand'
                                    ELSE 'UAC - Other' END

                               WHEN Publisher IN ('Facebook', 'Snapchat', 'Twitter') THEN
                                    CASE WHEN REGEXP_CONTAINS(Campaign_name, '(?i)crm') THEN 'CRM - Social' ELSE 'Social - Paid' END
                               WHEN Publisher IN ('DV360', 'AppNexus', 'TradeDesk') OR REGEXP_CONTAINS(campaign_name, '(?i)prog') THEN 'Display - Programmatic'
                               WHEN REGEXP_CONTAINS(Publisher, '(?i)google|bing') THEN
                               CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'PPC - Competitor'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                      OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'PPC - Generic'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'PPC - Brand'
                                    ELSE 'PPC - Other' END
                               WHEN Publisher = 'DCM' AND NOT REGEXP_CONTAINS(campaign_name, 'c:|cid') THEN 'Display - Other'
                               ELSE 'Display - Other'
                            END  AS ChannelGrouping
                         ,CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)uac') THEN 'Google_UAC' ELSE Publisher END AS Publisher
                         ,campaign_id
                         ,CASE WHEN REGEXP_CONTAINS(Campaign_name, '(?i)dv360|appnex') AND REGEXP_CONTAINS(Campaign_name, '(?i)ID_') THEN
                                    SPLIT(Campaign_name, 'ID_')[SAFE_OFFSET(0)] ELSE Campaign_name END AS Campaign_name
                         ,currency                    AS Currency
                         ,ROUND(SUM(spend),2)         AS Spend
                         ,ROUND(SUM(clicks))          AS Clicks
                         ,ROUND(SUM(impressions))     AS Impressions
                        -- ,ROUND(SUM(conversions))     AS Conversions
                         ,End_date, start, dataset

              FROM (SELECT * FROM {{ref('gvc_campaigns_fivetran_costs')}})
              WHERE Brand IN (SELECT distinct Brand FROM {{ref('gvc_conversions_analytics')}})
               AND  EXTRACT(YEAR FROM Date) > 2020
              GROUP BY 1,2,3,4,5,6,7,11,12,13)
              ),

    rank AS ( SELECT *, REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(Campaign_name, '|')[SAFE_OFFSET(1)], '_', SPLIT(Campaign_name, '|')[SAFE_OFFSET(18)]), '  ', ' '), ' ', '') AS code
              FROM rety
              WHERE REGEXP_CONTAINS(Publisher, '(?i)nexus|360')
              ),

    prog AS ( SELECT a.Brand, a.Date, a.ChannelGrouping, a.Publisher, a.campaign_id, a.Campaign_name, a.Currency
                     ,SUM(b.Spend)       AS Spend
                     ,SUM(b.Clicks)      AS Clicks
                     ,SUM(b.Impressions) AS Impressions
                     ,MAX(b.End_Date)    AS End_Date
                     ,MIN(b.start)       AS start
                     ,dataset            AS dataset
                     , a.campaign, a.code
              FROM(
                   SELECT *, ROW_NUMBER () OVER (PARTITION BY Brand, Date, Code ORDER BY Spend DESC) AS rank
                   FROM rank
                   WHERE code IS NOT NULL) a
              JOIN(
                   SELECT Brand, Date, code, ROUND(SUM(spend),2) AS Spend, ROUND(SUM(clicks)) AS Clicks, ROUND(SUM(impressions)) AS Impressions, --ROUND(SUM(Conversions)) AS Conversions,
                          MAX(End_date) AS End_date, MIN(start) AS start
                   FROM rank
                   WHERE code IS NOT NULL
                   GROUP BY 1,2,3)      b
              ON a.code = b.code AND a.Date = b.Date and a.brand=b.brand
              WHERE rank = 1
              GROUP BY 1,2,3,4,5,6,7,13,14,15
             ),

    ver AS(   SELECT * REPLACE( REGEXP_REPLACE(campaign, '  | ', '')  AS Campaign_name)
                     , CASE WHEN REGEXP_CONTAINS(Publisher,'(?i)nexus|360') THEN code ELSE REGEXP_REPLACE(LOWER(campaign), ' ', '') END AS cpname
                     , CASE WHEN REGEXP_CONTAINS(publisher, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR Campaign IS NULL)
                             AND (NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)VOD|UAC') OR ChannelGrouping IS NULL)
                            THEN 1 END AS google_excl
              FROM(

                    SELECT *, '' AS code FROM rety WHERE (NOT REGEXP_CONTAINS(Publisher, '(?i)nexus|360') OR Publisher IS NULL)
                    UNION ALL
                    SELECT * FROM prog
                    UNION ALL
                    SELECT * FROM rank WHERE code IS NULL
                    UNION ALL
                    SELECT * FROM offl
                    UNION ALL
                    SELECT * FROM affi
                    )
              WHERE CAST(date AS DATE)< CURRENT_DATE()
             )

-- SELECT c.* REPLACE(IFNULL(c.campaign_name, a.campaign) AS campaign_name)
-- FROM ver c
-- LEFT JOIN ( SELECT *
--             FROM `api-project-786064088220.AttributionMediaReporting_LCG.Conversions_x_Campaign`
--             WHERE REGEXP_CONTAINS(ChannelGrouping,'artners') AND cid> 4995000) a
-- ON CAST(a.cid AS STRING) = c.campaign_id

SELECT * REPLACE(CASE WHEN Brand = 'Bwin' AND REGEXP_CONTAINS(ChannelGrouping, '(?i)partner')
                      THEN COALESCE( CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)forza')               THEN 'Forza App'
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)dpg|360|dcm|vod|uim') THEN UPPER(Publisher)
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)dbm')                 THEN 'DBM'
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)E2-networks')         THEN 'E2network'
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)sky sport|spiegel')   THEN INITCAP(REGEXP_REPLACE(publisher, ' (de)', ''))
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)onefootball_odds')    THEN 'Onefootball-odds'
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)bild|kicker|kicktipp|sport1|spiegel|transfermarkt')
                                           AND NOT REGEXP_CONTAINS(Publisher, '(?i)odds|.de')        THEN CONCAT(INITCAP(Publisher), '.de')
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i)-')                   THEN CONCAT(INITCAP(SPLIT(Publisher, '-')[SAFE_OFFSET(0)]), '-', SPLIT(Publisher, '-')[SAFE_OFFSET(1)])
                                          WHEN REGEXP_CONTAINS(Publisher, '(?i).gr|.de|.bg|.at|.com')THEN CONCAT(INITCAP(SPLIT(Publisher, '.')[SAFE_OFFSET(0)]), '.', SPLIT(Publisher, '.')[SAFE_OFFSET(1)])
                                          ELSE INITCAP(REGEXP_REPLACE(Publisher, ' (de)', ''))
                                          END, Publisher)
                      ELSE Publisher END AS Publisher)

        ,CASE WHEN google_excl IS NOT NULL             THEN 'adwords'
              WHEN channelgrouping = 'Affiliate'       THEN 'aff'
              WHEN cid IS NOT NULL                     THEN 'cid'
              ELSE 'name' END AS join_type
FROM(

        SELECT c.* REPLACE(CASE WHEN c.currency = 'GBP' THEN ROUND(spend,2)
                                WHEN Exchange_rate IS NOT NULL THEN ROUND(spend * CAST(Exchange_rate AS FLOAT64),2) END AS spend,
                           'GBP' AS currency,
                           CASE WHEN REGEXP_CONTAINS(campaign, '(?i)gvid') THEN 'Youtube'
                                WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)vod') AND REGEXP_CONTAINS(publisher, '(?i)Google') THEN 'Google' ELSE Publisher END AS Publisher,
                           CAST(c.Date AS DATE) AS Date)
               ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)c:') AND google_excl IS NULL THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64)
                     WHEN channelgrouping = 'Display - Partners' AND brand != 'Bwin'
                       OR channelgrouping = 'Affiliate' THEN SAFE_CAST(campaign_id AS INT64) END AS cid
               ,spend AS original_spend
               ,c.currency AS original_currency

        FROM ver c
        LEFT JOIN {{ref('dim_exchange_rates')}} xr
               ON c.currency = xr.currency AND SAFE_CAST(c.Date AS DATE)= xr.Date
  )
