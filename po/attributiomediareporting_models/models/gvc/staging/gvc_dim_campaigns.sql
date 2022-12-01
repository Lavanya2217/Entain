WITH
  google_ads    AS( SELECT distinct account_descriptive_name                           AS Brand
                         ,DATE(date)                                                 AS date
                         ,CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod|uac')
                          THEN 'Google_UAC' ELSE 'Google_Ads' END                    AS Publisher
                         ,CAST(campaign_id AS STRING)                                AS campaign_id
                         ,ANY_VALUE(campaign_name)                                   AS campaign_name
                         ,SUM(cost)                                                  AS spend
                         ,SUM(clicks)                                                AS clicks
                         ,SUM(impressions)                                           AS impressions

                  FROM (SELECT * FROM {{ source('gvc_google_ads_campaign_performance_party_gaming', 'fivetran_datasets') }} 
                        UNION ALL
                        SELECT * FROM {{ source('gvc_google_ads_campaign_performance_mcc_partypoker', 'fivetran_datasets') }}
                        UNION ALL
                        SELECT * FROM {{ source('gvc_google_ads_campaign_performance_bwin_mcc', 'fivetran_datasets') }} )

                  WHERE DATE(date) >= '2021-01-01'
                  GROUP BY 1,2,3,4

               UNION ALL

                  SELECT distinct account_name                                       AS Brand
                         ,DATE(day)                                                  AS date
                         ,'Google_Ads'                                               AS Publisher
                         ,CAST(campaign_id AS STRING)                                AS campaign_id
                         ,ANY_VALUE(campaign)                                        AS campaign_name
                         ,SUM(cost)                                                  AS spend
                         ,SUM(SAFE_CAST(clicks AS INT64))                            AS clicks
                         ,SUM(SAFE_CAST(impr_ AS INT64))                             AS impressions
                  FROM {{ source('gvc_fivetran_email', 'google_ads_performancemax_gr') }}
                  WHERE DATE(day) >= '2021-01-01'
                  GROUP BY 1,2,3,4


                 ),

dv360         AS( SELECT distinct advertiser                                                                AS Brand
                         ,CAST(CONCAT(SUBSTR(date,1,4),"-",SUBSTR(date,6,2),"-",SUBSTR(date,9,2)) AS DATE)  AS Date
                         ,'DV360'                                                                           AS Publisher
                         ,CAST(line_item_id AS STRING)                                                      AS campaign_id
                         ,line_item                                                                         AS campaign_name
                         ,total_media_cost_advertiser_currency                                              AS spend
                         ,clicks                                                                            AS clicks
                         ,impressions                                                                       AS impressions

                  FROM {{ source('gvc_google_display_and_video_360', 'fivetran_datasets') }} 
                  WHERE SUBSTR(Date, 1,4) = '2021'
                ),

appnexus      AS( SELECT distinct advertiser_name                                   AS Brand
                         ,DATE(day)                                                 AS Date
                         ,'AppNexus'                                                AS Publisher
                         ,CAST(line_item_id AS STRING)                              AS campaign_id
                         ,line_item_name                                            AS Campaign_name
                         ,cost_buying_currency                                      AS spend
                         ,clicks                                                    AS clicks
                         ,imps                                                      AS impressions

                  FROM {{ source('gvc_fivetran_email', 'appnexus') }} 
                  WHERE Day >= '2021-01-01'
                 ),

facebook_ads  AS( SELECT distinct CASE WHEN account_name IS NOT NULL THEN account_name
                                       ELSE h.name END AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Facebook_ads'                                            AS Publisher
                         ,CAST(campaign_id AS STRING)                               AS campaign_id
                         ,campaign_name                                             AS campaign_name
                         ,spend                                                     AS spend
                         ,clicks                                                    AS clicks
                         ,impressions                                               AS impressions
                  FROM {{ source('gvc_facebook2', 'fivetran_datasets') }}  F
                  LEFT JOIN {{ source('gvc_facebook_ad_account', 'account_history') }}  H
                         ON account_id = id
                  WHERE DATE(date) >= '2021-01-01'

              UNION ALL

                  SELECT distinct CASE WHEN account_name IS NOT NULL THEN account_name
                                       ELSE h.name END AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Facebook_ads'                                            AS Publisher
                         ,CAST(campaign_id AS STRING)                               AS campaign_id
                         ,campaign_name                                             AS campaign_name
                         ,spend                                                     AS spend
                         ,clicks                                                    AS clicks
                         ,impressions                                               AS impressions
                  FROM {{ source('gvc_facebook_additional', 'fivetran_datasets') }}  F
                  LEFT JOIN {{ source('gvc_facebook_ad_account', 'account_history') }}  H
                         ON account_id = id
                  WHERE account_id != 548349136005374
                    AND DATE(date) >= '2021-01-01'
                ),

apple_ads     AS( SELECT * EXCEPT(rank)
                           REPLACE(CASE WHEN rank = 1 THEN '2021-01-01' ELSE DATE END AS Date,
                                   CASE WHEN REGEXP_CONTAINS(Campaign_name, 'Competitors - needtracker|c:52167') AND Date < '2021-01-16'
                                        THEN '5039117@@@5039117-de competitors-bwin-sprts-de-asa-cpc-gen-appcompetitors--ios---agencyesv-de-c:52167'
                                        WHEN REGEXP_CONTAINS(Campaign_name, 'c:93454') AND Date < '2021-01-15'
                                        THEN '5039106@@@5039106-autbwinbrandsportsbettingapp-bwin-sprts-at-asa-cpc-gen-sportwetten--ios---agencyesv-de-c:93454'
                                        ELSE campaign_name END AS campaign_name)

                  FROM(
                        SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY date) AS rank
                        FROM(
                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name

                            FROM {{ source('gvc_apple_search_ads_bwin_masteracc', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_bwin_masteracc', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name

                            FROM {{ source('gvc_apple_search_ads_gvc_services_ltd', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_gvc_services_ltd', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name

                            FROM {{ source('gvc_apple_search_ads_moblinglobalzoomltd', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_moblinglobalzoomltd', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name

                            FROM {{ source('gvc_apple_search_ads_ppuk', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_ppuk', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name

                            FROM {{ source('gvc_apple_search_ads_partycasino', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_partycasino', 'organization') }}      a
                                ON c.organization_id = a.id
                            WHERE organization_id IN (1547750, 1547760)

                            )

                )),

bing_ads      AS( SELECT * EXCEPT(rank) REPLACE(CASE WHEN rank =1 AND EXTRACT(YEAR FROM Date) > 2020 THEN '2021-01-01' ELSE Date END AS Date)
                  FROM(
                        SELECT * EXCEPT(rank) REPLACE(CAST(campaign_id AS STRING) AS campaign_id)
                                 ,ROW_NUMBER() OVER( PARTITION BY campaign_id ORDER BY date) AS rank
                        FROM(
                              SELECT * EXCEPT(cid, length), ROW_NUMBER() OVER( PARTITION BY campaign_id, DATE ORDER BY cid DESC, length DESC) AS rank
                              FROM(
                                  SELECT distinct a.name                                            AS Brand
                                     ,DATE(c.modified_time)                                         AS Date
                                     ,'Bing_ads'                                                    AS Publisher
                                     ,c.id                                                          AS campaign_id
                                     ,c.name                                                        AS campaign_name
                                     ,IF(REGEXP_CONTAINS(c.name, 'c:'),1,NULL)                      AS cid
                                     ,LENGTH(c.name)                                                AS length

                                   FROM {{ source('gvc_bing_ads', 'campaign_history') }}  c
                                   JOIN {{ source('gvc_bing_ads', 'account_history') }}   a ON c.account_id = a.id
                                   GROUP BY 1,2,4,5)
                            )WHERE rank=1)
                 ),

twitter_ads   AS(  SELECT * EXCEPT(rank) REPLACE( CASE WHEN rank = 1 THEN '2021-01-01' ELSE Date END AS Date)
                   FROM(
                            SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY Date) AS rank
                            FROM(  SELECT distinct a.name                                            AS Brand
                                          ,DATE(c.updated_at)                                        AS Date
                                          ,'Twitter_ads'                                             AS Publisher
                                          ,c.id                                                      AS campaign_id
                                          ,c.name                                                    AS campaign_name

                                   FROM {{ source('gvc_twitter_ads_final', 'campaign_history') }}  c
                                   JOIN {{ source('gvc_twitter_ads_final', 'account_history') }}   a ON c.account_id = a.id ))
                ),

snapchat_ads  AS( SELECT distinct a.name                                            AS Brand
                         ,DATE(c.updated_at)                                        AS Date
                         ,'Snapchat_ads'                                            AS Publisher
                         ,c.id                                                      AS campaign_id
                         ,c.name                                                    AS campaign_name

                       FROM {{ source('snapchat_ads', 'campaign_history') }}        c
                       JOIN {{ source('snapchat_ads', 'ad_account_history') }} a ON a.id = c.ad_account_id
                       WHERE a.organization_id IN ('f304b68f-8a14-472e-afd7-2b586ac99865')
                        --AND c.name NOT IN( '4945014@@@a_719537b_1690c_@@@ @@@ @@@', '4945014@@@a_719538b_1690c_@@@ @@@ @@@')
                ),

tradedesk    AS(  SELECT distinct Advertiser                                        AS Brand
                         ,date                                                      AS Date
                         ,'TradeDesk'                                               AS Publisher
                         ,campaign_id                                               AS campaign_id
                         ,Campaign                                                  AS campaign_name
                  FROM {{ source('gvc_tradedesk_', 'raw_data_V2') }}
                ),

verizon      AS(  SELECT advertiser                       AS Brand
                         ,day                             AS date
                         ,'Verizon'                       AS Publisher
                         ,campaign_id                     AS campaign_id
                         ,IFNULL(campaign, campaign_name) AS campaign_name
                         ,SUM(advertiser_spending)        AS spend
                         ,SUM(clicks)                     AS clicks
                         ,SUM(impressions)                AS impressions


                  FROM (SELECT * REPLACE(CAST(CONCAT(SUBSTR(DAY,7,4),"-",SUBSTR(DAY,1,2),"-",SUBSTR(DAY,4,2)) AS DATE) AS Day)
                        FROM {{ source('gvc_fivetran_email', 'verizon_dsp') }} )
                  WHERE day >='2021-01-01'
                    AND advertiser IS NOT NULL
                  GROUP BY 1,2,3,4,5),


final        AS(  SELECT CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)youtube') THEN 'Youtube'
                               WHEN (REGEXP_CONTAINS(Publisher, '(?i)google') AND REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)gads') )
                               OR REGEXP_CONTAINS(campaign_name, '(?i)bwinbulgaria|bwin bulgaria') THEN 'DCM'
                              ELSE Publisher END AS Publisher
                         , Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name,
                         Date,
                         CAST(IFNULL(DATE_ADD(LEAD(Date,1) OVER (PARTITION BY Brand, campaign_id ORDER BY Date), INTERVAL -1 DAY), '2022-12-31') AS Date) AS End_date
                  FROM(
                        SELECT * FROM apple_ads     UNION ALL
                        SELECT * FROM bing_ads      UNION ALL
                        SELECT * FROM twitter_ads   UNION ALL
                        SELECT * FROM snapchat_ads  UNION ALL
                        SELECT * FROM tradedesk)

                  UNION ALL

                  SELECT Publisher, Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name, MIN(Date) AS Date, MAX(Date) AS End_date
                  FROM(
                        SELECT * REPLACE(CASE WHEN campaign_name = '4930553-brand t1-bwin-pkr-dk-gads-cpc-bnd-poker--tweb---agencyesv-da-c:98715'
                                              THEN REGEXP_REPLACE(Campaign_name, '-poker-', '-bwin-') ELSE campaign_name END AS campaign_name)
                        FROM google_ads             UNION ALL
                        SELECT * FROM facebook_ads  UNION ALL
                        SELECT * FROM dv360         UNION ALL
                        SELECT * FROM verizon       UNION ALL
                        SELECT * FROM appnexus )
                  GROUP BY 1,2,3,4
                  )

, vee AS(
        SELECT * EXCEPT(Date, end_date),IFNULL(DATE_ADD(LAG(end_date) OVER (PARTITION BY Brand, campaign_id ORDER BY date), INTERVAL 1 DAY), date) AS date, end_date
                , CASE WHEN REGEXP_CONTAINS(campaign_name, 'cid:')
                       THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                       WHEN REGEXP_CONTAINS(campaign_name, 'c:')
                       THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                  END AS cid,
                ROW_NUMBER() OVER (PARTITION BY Publisher, brand, campaign_id ORDER BY end_date DESC) AS newest_rank
        FROM(
                SELECT  Publisher
                       ,CASE WHEN REGEXP_CONTAINS(brand, '(?i)sportingbet|services')     THEN 'Sportingbet'
                             WHEN REGEXP_CONTAINS(brand, '(?i)partycasino|party casino|Party Gaming') THEN 'Party Casino'
                             WHEN REGEXP_CONTAINS(campaign_name, '(?i)partypoker|party poker')
                               OR REGEXP_CONTAINS(brand, '(?i)partypoker|party poker')   THEN 'Party Poker'
                             WHEN REGEXP_CONTAINS(brand, '(?i)vistabet')                 THEN 'Vistabet'
                             WHEN REGEXP_CONTAINS(brand, '(?i)Foxy Bingo')               THEN 'Foxy Bingo'
                             WHEN REGEXP_CONTAINS(campaign_name, '(?i)foxy casino|foxycasino')
                               OR REGEXP_CONTAINS(brand, '(?i)foxy casino|foxycasino')   THEN 'Foxy Casino'
                             WHEN REGEXP_CONTAINS(brand, '(?i)bwin|BChamp')
                               OR REGEXP_CONTAINS(campaign_name, '(?i)bwin')             THEN 'Bwin'
                             WHEN REGEXP_CONTAINS(brand, '(?i)gioco digitale|giocodigitale')
                               OR REGEXP_CONTAINS(campaign_name, '(?i)gioco digitale|giocodigitale') THEN 'Gioco Digitale'
                             ELSE brand
                        END AS Brand

                       ,campaign_id, campaign_name, MIN(Date) AS Date, MAX(End_Date) AS End_Date,
                       CASE WHEN REGEXP_CONTAINS(Publisher, 'DCMM_') THEN
                            CASE WHEN REGEXP_CONTAINS(Publisher, '(?i)youtube')                                                             THEN 'Display - VOD'
                                 WHEN REGEXP_CONTAINS(Publisher, '(?i)tinder|twitch|teads')                                                 THEN 'Social - Paid'
                                 WHEN REGEXP_CONTAINS(Publisher, '(?i)amazon|tabool|viber|dv360')                                           THEN 'Display - Programmatic'
                                 WHEN REGEXP_CONTAINS(Publisher, '(?i)3liga|dfb|bvb|fc koln|berlin|eurocup|jupiler|bwin news de|st pauli')
                                   OR REGEXP_CONTAINS(campaign_name, '(?i)bwinbulgaria|bwin bulgaria')                                      THEN 'Display - Other'
                                 ELSE 'Display - Partners' END

                            WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod')                                                                  THEN 'Display - VOD'
                            WHEN REGEXP_CONTAINS(campaign_name, '(?i)partner')                                                              THEN 'Display - Partners'
                            WHEN REGEXP_CONTAINS(campaign_name, '(?i)prog') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)programma_')        THEN 'Display - Programmatic'
                            WHEN REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)cpc')            THEN 'Display - Others'
                            WHEN REGEXP_CONTAINS(campaign_name, '(?i)uac|apple') OR REGEXP_CONTAINS(Publisher, '(?i)uac|apple') THEN
                            CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                   OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'UAC - Competitor'
                                 WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                   OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand|-gen-|non brand') OR campaign_name LIKE '%|gen|%'                     THEN 'UAC - Generic'
                                 WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                   OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'UAC - Brand'
                                 ELSE 'UAC - Other'                                                  END
                            WHEN REGEXP_CONTAINS(Publisher, '(?i)twitter|facebook|snapchat')
                              OR REGEXP_CONTAINS(campaign_name, '(?i)twitter|facebook|snapchat')  THEN
                            CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)crm')                         THEN 'CRM - Social'
                                 ELSE 'Social - Paid' END
                            WHEN REGEXP_CONTAINS(Publisher, '(?i)dv360|AppNexus|trade')                 THEN 'Display - Programmatic'
                            WHEN REGEXP_CONTAINS(Publisher, '(?i)bing|google')                    THEN
                            CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)gads')                                  THEN 'Display - Other'
                                 WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                   OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'PPC - Competitor'
                                 WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                   OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand|non brand') OR campaign_name LIKE '%|gen|%'                           THEN 'PPC - Generic'
                                 WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                   OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'PPC - Brand'
                                 ELSE 'PPC - Other' END
                            ELSE 'Display - Other'
                            END AS ChannelGrouping

                FROM final
                WHERE campaign_id IS NOT NULL
                GROUP BY 1,2,3,4,7
            )),


uk_gaming AS( SELECT partner_name AS Publisher,
                      CASE WHEN brand = 'FoxyGames' THEN 'Foxy Casino' ELSE REGEXP_REPLACE(REGEXP_REPLACE( Brand , 'Gala', 'Gala '), 'Foxy' , 'Foxy ') END AS brand
                      ,wmid AS campaign_id, campaign_name, 'Display - Partners' AS Channelgrouping, start_date AS Date, end_date
                      ,SAFE_CAST(SUBSTR(SPLIT(campaign_name, 'c:')[SAFE_OFFSET(1)],1,5) AS INT64) AS cid, ROW_NUMBER() OVER (PARTITION BY Publisher, brand, wmid ORDER BY end_date DESC) AS newest_rank
               FROM {{ref('lc_costs_ukgaming_partners_consolidated')}}
               WHERE start_date>= '2021-01-01' AND wmid IS NOT NULL AND spend IS NOT NULL
                 AND (campaign_name <> 'null' OR spend >0)

              UNION ALL

               SELECT *
               FROM  {{ref('lc_dim_campaigns')}}
               WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky')
             )





SELECT f.* REPLACE(CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)youtube') THEN 'Youtube' ELSE Publisher END AS Publisher,
                   CASE WHEN Campaign_id = '6235237301958' THEN '2021-06-30' ELSE end_date END AS end_date
                   ), x_currency AS currency
FROM vee f
LEFT JOIN `gvc-fivetran-prod.dim_campaign.dim_campaign`   d
ON d.x_campaign_id = f.campaign_id

UNION ALL

SELECT *, 'GBP' AS currency
FROM uk_gaming
