WITH cp_costs
      AS(     SELECT  Brand
                      ,d.Brand AS Account
                      ,s.Date AS Date
                      ,CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)faceb|snap|twitter|tinder|twitch|teads') THEN 'Social - Paid' ELSE ChannelGrouping END AS ChannelGrouping
                      ,CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)faceb') THEN 'Facebook' ELSE REGEXP_REPLACE(REGEXP_REPLACE(Publisher, '_ads', ''), 'display-', '') END AS Publisher
                      ,x_Campaign_id AS Campaign_id, Campaign_name AS Campaign_name, spend, clicks,impressions,  d.date as start, end_date
                      ,currency
                      ,'fivetran_gvc' AS dataset
              FROM {{ref('gvc_costs_fivetran_fact')}} s
              JOIN (SELECT distinct * FROM {{ref('gvc_dim_campaigns')}} )d
                ON d.campaign_id = s.x_campaign_id AND s.date BETWEEN d.Date AND end_date
              WHERE s.Date >= '2021-01-01'
                AND( (Brand = 'Bwin' AND REGEXP_CONTAINS(x_publisher, '(?i)dv360') )
                      OR (Brand IN (SELECT distinct Brand FROM {{ref('gvc_conversions_analytics')}})
                          AND NOT REGEXP_CONTAINS(x_publisher, '(?i)dv360') )
                  )
        ),

     galafoxy_costs
     AS(     SELECT brand, '' AS Account, CAST(date AS DATE) AS Date, ChannelGrouping, Publisher, campaign_id, Campaign_name, spend, clicks, impressions, start, end_date, currency, dataset
             FROM {{ref('lc_campaigns_costs')}}
             WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND Date >= '2021-01-01'

           UNION ALL

             SELECT distinct CASE WHEN brand = 'FoxyGames' THEN 'Foxy Casino' ELSE REGEXP_REPLACE(REGEXP_REPLACE( Brand , 'Gala', 'Gala '), 'Foxy' , 'Foxy ') END AS brand
                            , '' AS Account, start_date AS Date, 'Display - Partners' AS Channelgrouping
                            ,CASE WHEN REGEXP_CONTAINS(partner_name, '(?i)wakeapp') THEN 'Wakeapp' ELSE INITCAP(partner_name) END AS Publisher
                            ,wmid AS campaign_id, campaign_name, spend, NULL AS clicks, NULL AS impressions, start_date AS start, End_date, 'GBP' AS currency ,'gaming_partners' AS dataset
             FROM {{ref('lc_costs_ukgaming_partners_consolidated')}} 
             WHERE start_date>= '2021-01-01' AND wmid IS NOT NULL AND spend IS NOT NULL
               AND (campaign_name <> 'null' OR spend >0)
         ),

    dcm_clicks
     AS(      SELECT * EXCEPT(site_dcm), NULL AS spend
                      ,CASE WHEN REGEXP_CONTAINS(site_dcm, '(?i)forza') OR REGEXP_CONTAINS(Campaign, '(?i)forza') THEN 'Forza App'
                            WHEN REGEXP_CONTAINS(Campaign, '(?i)odds') THEN CONCAT(SPLIT(REGEXP_REPLACE(LOWER(site_dcm), ' (de)',''),'.')[SAFE_OFFSET(0)], '-odds')
                            ELSE SPLIT(REGEXP_REPLACE(LOWER(site_dcm), ' (de)',''),'.')[SAFE_OFFSET(0)] END AS Publisher
                      ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)c:') THEN SAFE_CAST(SUBSTR(SPLIT(Campaign, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64) END AS cid
              
              FROM (SELECT * REPLACE('Bwin' AS Brand,
                                      SPLIT(REGEXP_REPLACE(LOWER(site_dcm), ' (de)',''),'.')[SAFE_OFFSET(0)] AS site_dcm)
                    FROM {{ref('gvc_dcm_clicks')}} 
                    WHERE (brand ='Bwin' OR REGEXP_CONTAINS(site_dcm, '(?i)bwin') )
                     AND NOT REGEXP_CONTAINS(site_dcm, '(?i)google|msn|yahoo|ask|code serving|sportingbet|facebook|channel 4|bwin.com|trade desk|xandr|verizon|appnexus|social media|amazon|twitter|aalen|sizmek|Reach Publishing|cru web|Financial-Spread|quantcast|realmadrid|criteo|adform|bettingexpert|bwin news uk|dplay|NL Mail'))
               ),

    partners_costs
     AS(      SELECT * REPLACE(SAFE_CAST(cid AS INT64) AS cid,
                               REGEXP_REPLACE(REGEXP_REPLACE(partner_name, 'e2network', 'e2 online'), '_odds', '-odds') AS partner_name)
              FROM `api-project-786064088220.AttributionMediaReporting_GVC_Dev.Partners_costs`
              WHERE Brand = 'Bwin' AND partner_name IS NOT NULL),


    partners
    AS(      SELECT IFNULL(c.Brand, 'Bwin')                          AS Brand
                   ,IFNULL(c.Brand, 'Bwin')                          AS Account
                   ,IFNULL(c.date, p.date)                           AS Date
                   ,'Display - Partners'                             AS ChannelGrouping
                   ,IFNULL(Publisher, partner_name)                  AS Publisher
                   ,IFNULL(Campaign_id,
                          CONCAT('C:',CAST(SAFE_CAST(p.cid AS INT64) AS STRING))) AS Campaign_id
                   ,IFNULL(c.Campaign, p.Campaign_name)              AS Campaign_name
                   ,IFNULL(weighted_spend_per_day, spend)            AS spend
                   ,IFNULL(c.click, p.clicks)                        AS clicks
                   ,impression
                   ,date AS start, date AS end_date
                   ,UPPER(p.Currency)                                AS currency
                   ,'gvc_partners'                                   AS dataset

             FROM (SELECT * FROM dcm_clicks WHERE cid IS NOT NULL ) c
             FULL OUTER JOIN (SELECT * FROM partners_costs WHERE SAFE_CAST(cid AS INT64) IS NOT NULL AND campaign_name IS NOT NULL ) p
             USING(date, cid)

        UNION ALL
             SELECT IFNULL(c.Brand, 'Bwin')                          AS Brand
                   ,IFNULL(c.Brand, 'Bwin')                          AS Account
                   ,IFNULL(c.date, p.date)                           AS Date
                   ,'Display - Partners'                             AS ChannelGrouping
                   ,IFNULL(Publisher, CONCAT( partner_name)) AS Publisher
                   ,IFNULL(Campaign_id, CAST(SAFE_CAST(p.cid AS INT64) AS STRING)) AS Campaign_id
                   ,IFNULL(c.campaign,
                           IFNULL(p.Campaign_name, 'not set'))       AS Campaign_name
                   ,IFNULL(weighted_spend_per_day, spend)            AS spend
                   ,IFNULL(c.click, p.clicks)                        AS clicks
                   ,impression
                   ,IFNULL(c.date, p.date)                           AS start
                   ,IFNULL(c.date, p.date)                           AS end_date
                   ,UPPER(p.Currency)                                AS currency
                   ,'gvc_partners'                                   AS dataset

             FROM (SELECT * FROM dcm_clicks WHERE cid IS NULL ) c
             FULL OUTER JOIN (SELECT * FROM partners_costs WHERE SAFE_CAST(cid AS INT64) IS NULL OR campaign_name IS NULL)  p
             ON c.date = p.date AND publisher = partner_name
             )



SELECT * REPLACE(REGEXP_REPLACE(Publisher,'display-','') AS Publisher)
FROM(

      SELECT * FROM cp_costs
      WHERE (Brand <> 'Bwin' AND  NOT REGEXP_CONTAINS(Publisher, '(?i)apple|bing') )
         OR (ChannelGrouping <> 'Display - Partners' AND brand ='Bwin')

      UNION ALL

      SELECT * FROM partners

      UNION ALL

      SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)dbm|dcm|360|vod') THEN UPPER(Publisher)
                            WHEN REGEXP_CONTAINS(Publisher, '(?i)dbm') THEN 'DBM' ELSE Publisher END AS Publisher)
      FROM galafoxy_costs
    )
