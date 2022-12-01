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

                  FROM {{ source('google_ads', 'campaign_stats') }}
                  WHERE DATE(date) >= '2018-01-01'
                  GROUP BY 1,2,3,4
                 ),

dv360         AS( SELECT distinct advertiser                                                                AS Brand
                         ,date                                                                              AS Date
                         ,'DV360'                                                                           AS Publisher
                         ,CAST(CASE WHEN advertiser IN ('Coral', 'Ladbrokes')
                         THEN line_item_id ELSE campaign_id END AS STRING)                                  AS campaign_id
                         ,CASE WHEN advertiser IN ('Coral', 'Ladbrokes')
                         THEN line_item ELSE campaign END                                                   AS campaign_name
                         ,media_cost_advertiser_currency                                                    AS spend
                         ,clicks                                                                            AS clicks
                         ,impressions                                                                       AS impressions

                  FROM {{ source('google_display_and_video_360', 'dv_360_ladbrokes_il') }}
                  WHERE EXTRACT(YEAR FROM Date)> 2018
                ),

appnexus      AS( SELECT distinct advertiser_name                                   AS Brand
                         ,DATE(day)                                                 AS Date
                         ,'AppNexus'                                                AS Publisher
                         ,CAST(line_item_id AS STRING)                              AS campaign_id
                         ,line_item_name                                            AS Campaign_name
                         ,cost_buying_currency                                      AS spend
                         ,clicks                                                    AS clicks
                         ,imps                                                      AS impressions

                  FROM {{ source('fivetran_email', 'appnexus_2') }}
                  WHERE Day >= '2018-01-01'
                 ),

facebook_ads  AS( SELECT distinct account_name                                      AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Facebook_ads'                                            AS Publisher
                         ,CAST(campaign_id AS STRING)                               AS campaign_id
                         ,campaign_name                                             AS campaign_name
                         ,spend                                                     AS spend
                         ,clicks                                                    AS clicks
                         ,impressions                                               AS impressions

                  FROM {{ source('facebook_ads', 'facebook_ads') }}
                  WHERE account_id NOT IN (356109348327430,306652936664049,2339350642966890,654541498311519,2146541318755957) /*remove US market*/
                    AND account_id NOT IN (1041406069944019) /*remove ladbrokes DE*/
                    AND DATE(date) >= '2018-01-01'
                ),

apple_ads     AS( SELECT * EXCEPT(rank)
                           REPLACE(CASE WHEN rank = 1 THEN '2020-01-01' ELSE DATE END AS Date,
                                   CASE WHEN NOT REGEXP_CONTAINS(campaign_name, 'cid|c:') THEN campaign_name ELSE
                                   CASE WHEN REGEXP_CONTAINS(campaign_name, 'c-wm:')
                                        THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign_name,'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(1)],"c-wm:")[SAFE_OFFSET(0)], "_c-wm:",
                                             REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign_name,'c-wm:')[SAFE_OFFSET(1)],
                                             STRPOS(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],"c-wm:"),100)),"_","|")), "  ", ""), " ", "")
                                        WHEN REGEXP_CONTAINS(campaign_name, '_c__')
                                        THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],'c__')[SAFE_OFFSET(0)],"c_|",
                                             REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],
                                             STRPOS(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],"c__")+3,100)),"_","|")), "  ", ""), " ", "")
                                        ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(1)],'wm:[0-9]{7}') [SAFE_OFFSET(0)],
                                             REGEXP_REPLACE(TRIM(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(2)] )    ,"_","|")) , "  ", ""), " ", ""), 'c_wm', 'c-wm')

                                        END
                                   END AS campaign_name)
                  FROM(
                        SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY date) AS rank
                        FROM(
                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name


                            FROM {{ source('apple_search_ads', 'campaign_history') }}        c
                            JOIN {{ source('apple_search_ads', 'organization') }} a ON c.organization_id = a.id))
                ),

apple_ads2    AS( SELECT * EXCEPT(rank)
                           REPLACE(CASE WHEN rank = 1 THEN '2020-01-01' ELSE DATE END AS Date,CASE WHEN NOT REGEXP_CONTAINS(campaign_name, 'cid|c:') THEN campaign_name ELSE
                                   CASE WHEN REGEXP_CONTAINS(campaign_name, 'c-wm:')
                                        THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign_name,'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(1)],"c-wm:")[SAFE_OFFSET(0)], "_c-wm:",
                                             REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign_name,'c-wm:')[SAFE_OFFSET(1)],
                                             STRPOS(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],"c-wm:"),100)),"_","|")), "  ", ""), " ", "")
                                        WHEN REGEXP_CONTAINS(campaign_name, '_c__')
                                        THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],'c__')[SAFE_OFFSET(0)],"c_|",
                                             REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],
                                             STRPOS(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'),'@@@')[SAFE_OFFSET(1)],"c__")+3,100)),"_","|")), "  ", ""), " ", "")
                                        ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(0)],"@@@",
                                             SPLIT(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(1)],'wm:[0-9]{7}') [SAFE_OFFSET(0)],
                                             REGEXP_REPLACE(TRIM(SPLIT(campaign_name,'@@@')[SAFE_OFFSET(2)] )    ,"_","|")) , "  ", ""), " ", ""), 'c_wm', 'c-wm')
                                        END
                                    END AS campaign_name)

                  FROM(
                       SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY date) AS rank
                       FROM(
                              SELECT distinct a.name                                            AS Brand
                                     ,DATE(c.modification_time)                                 AS Date
                                     ,'Apple_ads'                                               AS Publisher
                                     ,CAST(c.id AS STRING)                                      AS campaign_id
                                     ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                    AS campaign_name

                              FROM {{ source('apple_search_ads_cashcade', 'campaign_history') }}        c
                              JOIN {{ source('apple_search_ads_cashcade', 'organization') }} a ON c.organization_id = a.id))
                 ),

bing_ads      AS( SELECT distinct a.name                                            AS Brand
                         ,DATE(c.modified_time)                                     AS Date
                         ,'Bing_ads'                                                AS Publisher
                         ,CAST(c.id AS STRING)                                      AS campaign_id
                         ,ANY_VALUE(c.name)                                         AS campaign_name


                    FROM {{ source('bing_ads', 'campaign_history') }}        c
                    JOIN {{ source('bing_ads', 'account_history') }} a  ON a.id = c.account_id
                    GROUP BY 1,2,4
                 ),

twitter_ads   AS(  SELECT * EXCEPT(rank) REPLACE( CASE WHEN rank = 1 THEN '2020-01-01' ELSE Date END AS Date)
                   FROM(
                            SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY Date) AS rank
                            FROM(  SELECT distinct a.name                                            AS Brand
                                          ,DATE(c.updated_at)                                        AS Date
                                          ,'Twitter_ads'                                             AS Publisher
                                          ,c.id                                                      AS campaign_id
                                          ,c.name                                                    AS campaign_name

                                   FROM {{ source('twitter_ads', 'campaign_history') }}        c
                                   JOIN {{ source('twitter_ads', 'account_history') }} a  ON a.id = c.account_id))                                   
                ),

snapchat_ads  AS( SELECT distinct a.name                                            AS Brand
                         ,DATE(c.updated_at)                                        AS Date
                         ,'Snapchat_ads'                                            AS Publisher
                         ,c.id                                                      AS campaign_id
                         ,c.name                                                    AS campaign_name

                       FROM {{ source('snapchat_ads', 'campaign_history') }}        c
                       JOIN {{ source('snapchat_ads', 'ad_account_history') }} a ON a.id = c.ad_account_id
                       WHERE a.organization_id <> 'e8ad859d-90c1-4ffe-872f-9f953c9e504f'
                        AND c.name NOT IN( '4945014@@@a_719537b_1690c_@@@ @@@ @@@', '4945014@@@a_719538b_1690c_@@@ @@@ @@@')
                ),

taboola       AS( SELECT distinct CASE WHEN account_id = 1247587 THEN 'Coral'
                                       WHEN account_id = 1247585 THEN 'Ladbrokes'
                                       WHEN account_id = 1262569 THEN 'Gala Casino'
                                       WHEN account_id = 1258992 THEN 'Gala Bingo'
                                       ELSE 'Gala Spins'  END AS Brand
                                  ,DATE(start_date)                                                AS Date
                                  ,'Taboola'                                                       AS Publisher
                                  ,CAST(ID AS STRING)                                              AS campaign_id
                                  ,name                                                            AS campaign_name
                  FROM {{ source('taboola', 'campaign') }}
                ),

outbrain      AS( SELECT distinct CASE WHEN name LIKE '%-cor-%' THEN 'Coral' ELSE 'Ladbrokes' END AS Brand
                                  ,DATE(creation_time)                                            AS Date
                                  ,'Outbrain'                                                     AS Publisher
                                  ,ID                                                             AS campaign_id
                                  ,name                                                           AS campaign_name
                  FROM {{ source('outbrain', 'campaign_history') }}
                ),

tradedesk     AS( SELECT distinct Advertiser                                                      AS Brand
                                  ,DATE(Date)                                                     AS Date
                                  ,'TradeDesk'                                                    AS Publisher
                                  ,Campaign_ID                                                    AS campaign_id
                                  ,IF(Advertiser IN ('Coral', 'Ladbrokes'), Ad_Group, campaign)   AS campaign_name
                                  ,Advertiser_Cost__Adv_Currency_                                 AS spend
                                  ,clicks                                                         AS clicks
                                  ,impressions                                                    AS impressions


                  FROM {{ source('tradedesk_lcg', 'tradedesk_lcg') }}
                ),


   partners   AS( SELECT distinct Brand                                             AS Brand
                         ,DATE_DT                                                   AS Date
                         ,Partner                                                   AS Publisher
                         ,CAST(Tracker_ID AS STRING)                                AS campaign_id
                         ,TRIM(REGEXP_REPLACE(IFNULL(campaign_name,
                                              CONCAT(CONCAT(Tracker_id, '_', Partner, '_'),
                                              LOWER(CONCAT(LEFT(Brand,3), '_', country, '_',
                                                     CASE WHEN Product_type LIKE '%#%'  THEN ''
                                                          WHEN Product_type LIKE 'Spo%' THEN 'sprts'
                                                          WHEN Product_type LIKE 'Cas%' THEN 'casi'
                                                          WHEN Product_type LIKE 'Bin%' THEN 'bngo' END,
                                                     '_', IF(app_Type = '0', 'WEB', LEFT(app_Type,3)), '_', type)))),
                               ' ', '')) AS campaign_name

                       FROM {{ source('display_partner', 'display_partner_spend_table') }}    c

                ),




final        AS(  SELECT Publisher, Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name,
                         Date,
                         CAST(IFNULL(DATE_ADD(LEAD(Date,1) OVER (PARTITION BY Brand, campaign_id ORDER BY Date), INTERVAL -1 DAY), '2022-12-31') AS Date) AS End_date
                  FROM(
                        SELECT * FROM apple_ads     UNION ALL
                        SELECT * FROM apple_ads2    UNION ALL
                        SELECT * FROM bing_ads      UNION ALL
                        SELECT * FROM twitter_ads   UNION ALL
                        SELECT * FROM snapchat_ads  UNION ALL
                        SELECT * FROM taboola       UNION ALL
                        SELECT * FROM outbrain      --UNION ALL
                        -- SELECT * FROM partners 
                        )

                  UNION ALL

                  SELECT Publisher, Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name, MIN(Date) AS Date, MAX(Date) AS End_date
                  FROM(
                        SELECT * FROM google_ads    UNION ALL
                        SELECT * FROM facebook_ads  UNION ALL
                        SELECT * FROM dv360         UNION ALL
                        SELECT * FROM appnexus      UNION ALL
                        SELECT * FROM tradedesk)
                  GROUP BY 1,2,3,4
                  )


SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Channelgrouping, '(?i)partner') AND NOT REGEXP_CONTAINS(publisher, '(?i)taboola|outbrain') AND cid IS NOT NULL THEN SAFE_CAST(cid AS STRING) ELSE campaign_id END AS campaign_id)
FROM(
      SELECT * EXCEPT(Date, end_date),IFNULL(DATE_ADD(LAG(end_date) OVER (PARTITION BY Brand, campaign_id ORDER BY date), INTERVAL 1 DAY), date) AS date, end_date
              , CASE WHEN REGEXP_CONTAINS(campaign_name, 'cid:')
                     THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                     WHEN REGEXP_CONTAINS(campaign_name, 'c:')
                     THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                END AS cid,
              ROW_NUMBER() OVER (PARTITION BY Publisher, brand, campaign_id ORDER BY end_date DESC) AS newest_rank
      FROM(
              SELECT CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod') THEN 'VOD' ELSE Publisher END AS Publisher,
                     CASE WHEN REGEXP_CONTAINS(Brand, '(?i)gala bingo' ) THEN 'Gala Bingo'
                          WHEN REGEXP_CONTAINS(Brand, '(?i)gala spins' ) THEN 'Gala Spins'
                          WHEN REGEXP_CONTAINS(Brand, '(?i)gala casino') THEN 'Gala Casino' ELSE Brand END AS Brand,
                     campaign_id, campaign_name, MIN(Date) AS Date, MAX(End_Date) AS End_Date,
                     CASE WHEN --Publisher IN (SELECT distinct Publisher FROM partners)
                             Publisher IN ('Outbrain', 'Taboola')                                   THEN 'Display - Partners'
                          WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod')                              THEN 'Display - VOD'
                          WHEN REGEXP_CONTAINS(campaign_name, '(?i)uac|apple') OR REGEXP_CONTAINS(publisher, '(?i)uac|apple_ads') THEN
                          CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                 OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'UAC - Competitor'
                               WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'),  campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                 OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'UAC - Generic'
                               WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                 OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'UAC - Brand'
                               ELSE 'UAC - Other'                                                  END
                          WHEN REGEXP_CONTAINS(Publisher, '(?i)twitter|facebook|snapchat')
                            OR REGEXP_CONTAINS(campaign_name, '(?i)twitter|facebook|snapchat')  THEN
                          CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)crm')                         THEN 'CRM - Social'
                               ELSE 'Social - Paid' END
                          WHEN REGEXP_CONTAINS(Publisher, '(?i)dv360|AppNexus|tradedesk')             THEN 'Display - Programmatic'
                          WHEN REGEXP_CONTAINS(Publisher, '(?i)bing|google')                          THEN
                          CASE WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                                 OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'PPC - Competitor'
                               WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                                 OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand') OR campaign_name LIKE '%|gen|%'                                     THEN 'PPC - Generic'
                               WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                                 OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'PPC - Brand'
                               ELSE 'PPC - Other' END
                          END AS ChannelGrouping

              FROM (SELECT *, CASE WHEN campaign_id IN('1371347418', '1371347421') AND End_date = '2020-07-18' THEN 1 END AS exclusion
                    FROM final)
              WHERE campaign_id IS NOT NULL AND exclusion IS NULL
              GROUP BY 1,2,3,4,7
          )
)
