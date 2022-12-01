WITH

costs AS(     SELECT  Brand
                      ,d.Brand AS Account
                      ,s.Date AS Date
                      ,ChannelGrouping
                      ,CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)faceb') THEN 'Facebook' ELSE REGEXP_REPLACE(Publisher, '_ads', '') END AS Publisher
                      ,x_Campaign_id AS Campaign_id, Campaign_name AS Campaign_name, spend, clicks,impressions,  d.date as start, end_date
                      ,currency
              FROM {{ref('gvc_costs_fivetran_fact')}} s
              JOIN (SELECT distinct * FROM {{ref('pp_dim_campaigns')}} )d
              ON d.campaign_id = s.x_campaign_id AND s.date BETWEEN d.Date AND end_date
              WHERE s.Date >= '2021-01-01'

          UNION ALL

              SELECT 'Party Poker'                                                      AS Brand
                    ,'Party Poker'                                                      AS Account
                     ,DATE(b.calendar_date)                                             AS Date
                     ,'Social - Paid'                                                   AS Channelgrouping
                     ,'Twitch'                                                          AS Publisher
                     ,''                                                                AS campaign_id
                     ,LOWER(CONCAT(Country, '_', channel))                              AS Campaign_name
                     ,ROUND(SUM(SAFE_CAST(total_cost AS FLOAT64)),2)                    AS spend
                     ,NULL                                                              AS clicks
                     ,NULL                                                              AS impressions
                     ,DATE(b.calendar_date)                                             AS start
                     ,DATE(b.calendar_date)                                             AS end_date
                     ,Currency                                                          AS Currency

              FROM (SELECT Month__01_MM_YYYY_,Channel,	  Country,Currency,Total_Cost FROM {{ source('Party_Offline_data_2', 'PP_Twitch_Monthly_Costs') }}
                    UNION ALL
                    SELECT Month__01_MM_YYYY_,Player_name,Country,Currency,Total_Cost FROM {{ source('Party_Offline_data_2', 'PP_Team_Online_Spend') }} )a

              LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_CALENDAR') }}  b
              ON a.Month__01_MM_YYYY_=b.start_of_this_month_date
              WHERE Month__01_MM_YYYY_ >= '2021-01-01'
              GROUP BY 1,2,3,4,5,6,7,13
         ),

    dcm_clicks
     AS(      SELECT Brand, CAST(date AS STRING) AS Date, 'Display - Other' AS channelgrouping
                      ,CASE WHEN REGEXP_CONTAINS(site_dcm, '(?i)forza') OR REGEXP_CONTAINS(Campaign, '(?i)forza') THEN 'Forza App'
                            WHEN REGEXP_CONTAINS(Campaign, '(?i)odds') THEN CONCAT(SPLIT(REGEXP_REPLACE(LOWER(site_dcm), ' (de)',''),'.')[SAFE_OFFSET(0)], '-odds')
                            ELSE SPLIT(REGEXP_REPLACE(LOWER(site_dcm), ' (de)',''),'.')[SAFE_OFFSET(0)] END AS Publisher
                      ,campaign_id, campaign AS campaign_name, '' AS currency
                      , NULL AS spend, click AS clicks, impression AS impressions
                      ,CAST('2022-12-31' AS DATE) AS End_date, CAST('2021-01-01' AS DATE) AS Start, "" AS campaign, '' AS code
                     -- ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)c:') THEN SAFE_CAST(SUBSTR(SPLIT(Campaign, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64) END AS cid
              FROM (SELECT *
                    FROM {{ref('gvc_dcm_clicks')}}
                    WHERE REGEXP_CONTAINS(brand, '(?i)party') OR REGEXP_CONTAINS(site_dcm, '(?i)party')  )
               ),

offl AS(      SELECT CASE WHEN REGEXP_CONTAINS(Brand, '(?i)partycasino') THEN 'Party Casino'
                          WHEN REGEXP_CONTAINS(Brand, '(?i)partypoker') THEN 'Party Poker' END AS brand
                    , CAST(date AS STRING) AS Date, 'Offline' AS ChannelGrouping, Channel AS Publisher,
                      '' AS campaign_id, '' AS campaign_name, UPPER(currency) AS currency, Spend,
                       0 AS Clicks, Reach AS Impressions, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2021-01-01' AS DATE) AS Start, country AS campaign, '' AS code,
               FROM {{ source('offline_marketing_ukgaming', 'Offline_costs') }}
               WHERE REGEXP_CONTAINS(Brand, '(?i)party')
                 AND Date >= '2021-01-01'),

affi AS(      SELECT Brand_aff, CAST(date_aff AS STRING) AS Date, 'Affiliate' AS ChannelGrouping, "" AS Publisher,
                     tracker_id AS campaign_id, tracker_id AS campaign_name, 'GBP' AS currency, Costs AS Spend,
                       0 AS Clicks, 0 AS Impressions, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2021-01-01' AS DATE) AS Start, '' AS campaign, '' AS code,
               FROM {{ref('costs_affiliates')}}
               WHERE REGEXP_CONTAINS(Brand_aff,'(?i)party')
                 AND EXTRACT(YEAR FROM Date_aff) >= 2021
                 AND ABS(Costs)>0 ),


rety AS(      SELECT * REPLACE(REGEXP_REPLACE(Publisher, 'DCMM_', '') AS Publisher), REGEXP_REPLACE(LOWER(campaign_name), ' ', '') AS campaign
              FROM(
                  SELECT Brand
                         ,FORMAT_DATE("%Y-%m-%d", CAST(Date AS DATE)) as Date
                         ,ChannelGrouping
                         ,Publisher
                         ,campaign_id
                         ,CASE WHEN REGEXP_CONTAINS(Campaign_name, '(?i)dv360|appnex') AND REGEXP_CONTAINS(Campaign_name, '(?i)ID_') THEN
                                    SPLIT(Campaign_name, 'ID_')[SAFE_OFFSET(0)] ELSE Campaign_name END AS Campaign_name
                         ,currency                    AS Currency
                         ,ROUND(SUM(spend),2)         AS Spend
                         ,ROUND(SUM(clicks))          AS Clicks
                         ,ROUND(SUM(impressions))     AS Impressions
                         ,End_date, start

                  FROM costs
                  GROUP BY 1,2,3,4,5,6,7,11,12)
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
                     , a.campaign, a.code
              FROM(
                   SELECT *, ROW_NUMBER () OVER (PARTITION BY Brand, Date, Code ORDER BY Spend DESC) AS rank
                   FROM rank
                   WHERE code IS NOT NULL) a
              JOIN (
                   SELECT Brand, Date, code, ROUND(SUM(spend),2) AS Spend, ROUND(SUM(clicks),-1) AS Clicks, ROUND(SUM(impressions)) AS Impressions, --ROUND(SUM(Conversions)) AS Conversions,
                          MAX(End_date) AS End_date, MIN(start) AS start
                   FROM rank
                   WHERE code IS NOT NULL
                   GROUP BY 1,2,3)      b
              ON a.code = b.code AND a.Date = b.Date and a.brand=b.brand
              WHERE rank = 1
              GROUP BY 1,2,3,4,5,6,7,13,14
             ),

    ver AS(   SELECT * REPLACE( REGEXP_REPLACE(campaign, '  | ', '')  AS Campaign_name)
                     , CASE WHEN REGEXP_CONTAINS(Publisher,'(?i)nexus|360') THEN code ELSE REGEXP_REPLACE(LOWER(campaign), ' ', '') END AS cpname
                     , CASE WHEN REGEXP_CONTAINS(publisher, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR Campaign IS NULL)
                             AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
                            THEN 1 END AS google_excl
              FROM(

                    SELECT *, '' AS code FROM rety WHERE (NOT REGEXP_CONTAINS(Publisher, '(?i)nexus|360') OR Publisher IS NULL)
                    UNION ALL
--                    SELECT * FROM prog
--                    UNION ALL
                    SELECT * FROM rank WHERE code IS NULL
                    UNION ALL
                    SELECT * FROM offl
                    UNION ALL
                    SELECT * FROM affi
                    UNION ALL
                    SELECT * FROM dcm_clicks
                    )
              WHERE CAST(date AS DATE)< CURRENT_DATE()
             )


SELECT c.* REPLACE(CASE WHEN c.currency = 'GBP' THEN ROUND(spend,2)
                        WHEN Exchange_rate IS NOT NULL THEN ROUND(spend * CAST(Exchange_rate AS FLOAT64),2) END AS spend,
                   'GBP' AS currency,
                   CAST(c.Date AS DATE) AS Date)
       ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)c:') AND google_excl IS NULL THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64)
             WHEN channelgrouping = 'Affiliate' THEN SAFE_CAST(campaign_id AS INT64) END AS cid
       ,spend AS original_spend
       ,c.currency AS original_currency
       ,CASE WHEN google_excl IS NOT NULL             THEN 'adwords'
             WHEN REGEXP_CONTAINS(Campaign, '(?i)c:') THEN 'cid'
             WHEN channelgrouping = 'Affiliate'       THEN 'aff'
             ELSE 'name' END AS join_type


FROM ver c
LEFT JOIN {{ref('dim_exchange_rates')}} xr
       ON c.currency = xr.currency AND SAFE_CAST(c.Date AS DATE)= xr.Date
