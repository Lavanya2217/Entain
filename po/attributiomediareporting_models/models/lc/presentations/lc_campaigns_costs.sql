WITH cp_costs
      AS(
              SELECT CASE WHEN REGEXP_CONTAINS(d.Brand, '(?i)gala bingo' ) THEN 'Gala Bingo'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)gala spins' ) THEN 'Gala Spins'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)gala casino') THEN 'Gala Casino'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)coral'   )    THEN 'Coral'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)uk|adbro')    THEN 'Ladbrokes'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)cheeky')      THEN 'Cheeky Bingo'
                          WHEN REGEXP_CONTAINS(d.Brand, '(?i)foxy')        THEN
                          CASE WHEN REGEXP_CONTAINS(d.Brand,'(?i)bingo|bngo') THEN 'Foxy Bingo' ELSE 'Foxy Casino' END
                          ELSE d.Brand
                          END AS Brand
                      ,d.Brand AS Account
                      ,s.Date AS Date
                      ,REGEXP_REPLACE(Publisher, '_ads', '') AS Publisher
                      ,x_Campaign_id AS Campaign_id, Campaign_name AS Campaign_name, spend, clicks,impressions, NULL AS conversions, d.date as start, end_date
              FROM {{ref('lc_costs_fivetran_fact')}} s
              JOIN (SELECT distinct * FROM {{ref('lc_dim_campaigns')}}
                    WHERE (NOT REGEXP_CONTAINS(brand, '(?i)party|bwin|kes.be|belgium|mgm|BETDAQ|Yougiochi|Casino Gratis|Sportingbet') OR Brand IS NULL) )d
                ON d.campaign_id = s.x_campaign_id AND s.date BETWEEN d.Date AND end_date --DATE_ADD(end_date, INTERVAL 5 DAY)
              WHERE s.Date >= '2020-01-01'
        ),

dmcc AS(      SELECT CASE WHEN REGEXP_CONTAINS(campaign, '(?i)foxy')        THEN
                          CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)bingo|bngo') THEN 'Foxy Bingo' ELSE 'Foxy Casino' END
                          WHEN REGEXP_CONTAINS(campaign, '(?i)gala') THEN
                          CASE WHEN REGEXP_CONTAINS(campaign, '(?i)spins')  THEN 'Gala Spins'
                               WHEN REGEXP_CONTAINS(campaign, '(?i)casino') THEN 'Gala Casino'
                               ELSE 'Gala Bingo' END
                          WHEN REGEXP_CONTAINS(campaign, '(?i)coral')            THEN 'Coral'
                          WHEN REGEXP_CONTAINS(campaign, '(?i)ladbrokes|labdro') THEN 'Ladbrokes'
                      END AS brand
                     ,CAST(Date AS STRING) AS date
                     ,CASE WHEN REGEXP_CONTAINS(campaign, '(?i)vod')     THEN 'Display - VOD'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)display') THEN 'Display - Other'
                      ELSE 'Display - Others' END AS Channelgrouping
                     ,IFNULL(CASE WHEN REGEXP_CONTAINS(campaign, '(?i)vod') THEN 'VOD' ELSE SPLIT(Campaign, '|')[SAFE_OFFSET(13)] END, 'DCM') AS Publisher
                     ,campaign_id
                     ,campaign as campaign_name
                     ,'GBP' AS currency
                     ,ROUND(IFNULL(cpc_cost,IFNULL(cpm_cost,Flat_Cost_Click)),2) AS Spend
                     ,SUM(Click)               AS clicks
                     ,SUM(Impression)          AS Impressions
                     ,0                        AS conversions
                     ,date                     AS End_Date
                     ,date                     AS start
                     ,REGEXP_REPLACE(LOWER(campaign), ' ', '') AS campaign
                     ,REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(campaign, '|')[SAFE_OFFSET(1)], '_', SPLIT(campaign, '|')[SAFE_OFFSET(18)]), '  ', ' '), ' ', '') AS code

              FROM {{ref('lc_costs_dcm')}}
              WHERE cpc_cost>0 OR cpm_cost>0 OR Flat_Cost_Click>0
              GROUP BY 1,2,3,4,5,6,7,8,12),


offl AS(      SELECT Brand, CAST(date AS STRING) AS Date, 'Offline' AS ChannelGrouping, Channel AS Publisher,
                      '' AS campaign_id, '' AS campaign_name, 'GBP' AS currency, Spend,
                       0 AS Clicks, Reach AS Impressions, 0 AS Conversions, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2020-01-01' AS DATE) AS Start, '' AS campaign, '' AS code,
               FROM {{ref('lc_costs_offline')}}),

affi AS(      SELECT Brand_aff AS Brand, CAST(date_aff AS STRING) AS Date, 'Affiliate' AS ChannelGrouping, "" AS Publisher,
                     tracker_id AS campaign_id, tracker_id AS campaign_name, 'GBP' AS currency, ROUND(SUM(Costs),2) AS Spend,
                       0 AS Clicks, 0 AS Impressions, 0 AS Conversion, CAST('2022-12-31' AS DATE) AS End_date,
                       CAST('2021-01-01' AS DATE) AS Start, '' AS campaign, '' AS code,
               FROM {{ref('costs_affiliates')}}
               WHERE Brand_aff IN ('Ladbrokes', 'Coral')
                 AND Costs>0
                 AND CAST(tracker_id AS INT64) IN (SELECT distinct trackerid  FROM {{ source('exclusions_lists_lc', 'exclusion_list_Affiliates_final') }}  WHERE trackerid IS NOT NULL)
               GROUP BY 1,2,3,4,5
               ),





rety AS(      SELECT *, REGEXP_REPLACE(LOWER(campaign_name), ' ', '') AS campaign
              FROM(
                  SELECT Brand
                         ,FORMAT_DATE("%Y-%m-%d", CAST(Date AS DATE)) as Date
                         ,CASE WHEN Publisher IN (SELECT distinct partner FROM {{ source('display_partner', 'display_partner_spend_table') }})
                                 OR  Publisher IN ('Taboola', 'Verizon', 'Outbrain')                                           THEN 'Display - Partners'
                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod')                                                  THEN 'Display - VOD'
                               WHEN REGEXP_CONTAINS(campaign_name, '(?i)uac|apple') OR REGEXP_CONTAINS(publisher, '(?i)uac|apple_ads|apple') THEN
                               CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'UAC - Competitor'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'),  campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                      OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'UAC - Generic'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'UAC - Brand'
                                    ELSE 'UAC - Other'                                                  END

                               WHEN Publisher IN ('Facebook', 'Snapchat', 'Twitter') THEN
                                    CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)crm')
                                         THEN 'CRM - Social'
                                         ELSE 'Social - Paid' END

                               WHEN Publisher IN ('DV360', 'AppNexus', 'TradeDesk')           THEN 'Display - Programmatic'
                               WHEN REGEXP_CONTAINS(Publisher, '(?i)google|bing') THEN
                               CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'PPC - Competitor'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                      OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'PPC - Generic'
                                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                      OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'PPC - Brand'
                                    ELSE 'PPC - Other'    END
                            END  AS ChannelGrouping
                         ,Publisher
                         ,campaign_id
                         ,CASE WHEN REGEXP_CONTAINS(Campaign_name, '(?i)dv360|appnex') AND REGEXP_CONTAINS(Campaign_name, '(?i)ID_') THEN
                                    SPLIT(Campaign_name, 'ID_')[SAFE_OFFSET(0)] ELSE Campaign_name END AS Campaign_name
                         ,'GBP'                       AS Currency
                         ,ROUND(SUM(spend),2)         AS Spend
                         ,ROUND(SUM(clicks))          AS Clicks
                         ,ROUND(SUM(impressions))     AS Impressions
                         ,ROUND(SUM(conversions))     AS Conversions
                         ,End_date, start

              FROM cp_costs
              WHERE 1=1
               AND  (EXTRACT(MONTH FROM Date) = 11 OR EXTRACT(MONTH FROM Date) =12 OR EXTRACT(YEAR FROM Date) > 2019)
              GROUP BY 1,2,3,4,5,6,12,13)
              ),

    rank AS ( SELECT *, REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(Campaign_name, '|')[SAFE_OFFSET(1)], '_', SPLIT(Campaign_name, '|')[SAFE_OFFSET(18)]), '  ', ' '), ' ', '') AS code
              FROM rety
              WHERE REGEXP_CONTAINS(Publisher, '(?i)nexus|360')
              ),

    prog AS ( SELECT a.Brand, a.Date, a.ChannelGrouping, a.Publisher, a.campaign_id, a.Campaign_name, a.Currency
                     ,SUM(b.Spend)       AS Spend
                     ,SUM(b.Clicks)      AS Clicks
                     ,SUM(b.Impressions) AS Impressions
                     ,SUM(b.Conversions) AS Platform_registrations
                     ,MAX(b.End_Date)    AS End_Date
                     ,MIN(b.start)       AS start
                     , a.campaign, a.code
              FROM(
                   SELECT *, ROW_NUMBER () OVER (PARTITION BY Brand, Date, Code ORDER BY Spend DESC) AS rank
                   FROM rank
                   WHERE code IS NOT NULL) a
              JOIN(
                   SELECT Brand, Date, code, ROUND(SUM(spend),2) AS Spend, ROUND(SUM(clicks),-1) AS Clicks, ROUND(SUM(impressions)) AS Impressions, ROUND(SUM(Conversions)) AS Conversions,
                          MAX(End_date) AS End_date, MIN(start) AS start
                   FROM rank
                   WHERE code IS NOT NULL
                   GROUP BY 1,2,3)      b
              ON a.code = b.code AND a.Date = b.Date and a.brand=b.brand
              WHERE rank = 1
              GROUP BY 1,2,3,4,5,6,7,14,15
             ),

    ver AS(   SELECT * REPLACE( REGEXP_REPLACE(campaign, '  | ', '')  AS Campaign_name)
                     , CASE WHEN REGEXP_CONTAINS(Publisher,'(?i)nexus|360') THEN code ELSE REGEXP_REPLACE(LOWER(campaign), ' ', '') END AS cpname
                     , CASE WHEN REGEXP_CONTAINS(publisher, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR Campaign IS NULL)
                             AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
                            THEN 1 END AS google_excl
              FROM(

                    SELECT *, '' AS code, 'fivetran_lcg' AS dataset FROM rety WHERE (NOT REGEXP_CONTAINS(Publisher, '(?i)nexus|360') OR Publisher IS NULL)
                    UNION ALL
                    SELECT *, 'fivetran_lcg' AS dataset FROM prog
                    UNION ALL
                    SELECT *, 'fivetran_lcg' AS dataset FROM rank WHERE code IS NULL
                    UNION ALL
                    SELECT *, 'offline_costs_lcg' AS dataset FROM offl
                    UNION ALL
                    SELECT *, 'dcm_costs_lcg' AS dataset FROM dmcc
                    UNION ALL
                    SELECT *, 'aff_costs_lcg' AS dataset FROM affi
                    )
             )

-- SELECT c.* REPLACE(IFNULL(c.campaign_name, a.campaign) AS campaign_name,
--                    INITCAP(publisher) AS publisher)
-- FROM ver c
-- LEFT JOIN ( SELECT distinct cid, campaign
--             FROM  `api-project-786064088220.AttributionMediaReporting_LCG.Conversions_x_Campaign`
--             WHERE REGEXP_CONTAINS(ChannelGrouping,'artners') AND cid> 4995000) a
-- ON CAST(a.cid AS STRING) = c.campaign_id

select * from ver

