WITH
sevenstars      AS( SELECT distinct 'Party Casino'                                      AS Brand
                         ,DATE(date)                                                AS Date
                         ,Platform                                                  AS Publisher
                         ,'pc'||'_'||regexp_replace(lower(Platform),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign),r'| |_|-|\.','')   AS campaign_id -- no ID, use name
                         ,Campaign                                                  AS Campaign_name
                         ,Currency                                                  AS Currency
                  FROM{{ source('gvc_offline_marketing', 'PartyCasino_Brand_7stars') }}
                  where CAST(DATE(date) AS DATE) >='2021-01-01'
                  and (Spend>0 or Link_Clicks>0 or impressions>0)
                 ),

amobee      AS( SELECT distinct 'Party Casino'                                      AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Amobee'                                                  AS Publisher
                         ,CAST(line_item_id AS STRING)                              AS campaign_id
                         ,line_item_name                                            AS Campaign_name
                         ,Currency                                                  AS Currency
                  FROM {{ source('gvc_tomorrow', 'Amobee') }} 
                  WHERE date >= '2021-01-01' AND total_cost>0 
                 ),
                 
apple_ads     AS( SELECT * EXCEPT(rank)
                           REPLACE(CASE WHEN rank = 1 THEN '2021-01-01' ELSE DATE END AS Date)
                  FROM(
                        SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY date) AS rank
                        FROM(
                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name
                                   ,budget_currency                                            AS Currency

                            FROM {{ source('gvc_apple_search_ads_bwin_masteracc', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_bwin_masteracc', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name
                                   ,budget_currency                                            AS Currency

                            FROM {{ source('gvc_apple_search_ads_gvc_services_ltd', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_gvc_services_ltd', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name
                                   ,budget_currency                                            AS Currency

                            FROM {{ source('gvc_apple_search_ads_partycasino', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_partycasino', 'organization') }}      a
                                ON c.organization_id = a.id
                       UNION ALL

                            SELECT distinct a.name                                             AS Brand
                                   ,DATE(c.modification_time)                                  AS Date
                                   ,'Apple_ads'                                                AS Publisher
                                   ,CAST(c.id AS STRING)                                       AS campaign_id
                                   ,REGEXP_REPLACE(c.name,'c_wm:','c-wm:')                     AS campaign_name
                                   ,budget_currency                                            AS Currency

                            FROM {{ source('gvc_apple_search_ads_ppuk', 'campaign_history') }}  c
                            JOIN {{ source('gvc_apple_search_ads_ppuk', 'organization') }}      a
                                ON c.organization_id = a.id
                            )
                    WHERE REGEXP_CONTAINS(brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                      OR  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker')
                )),

appnetworks    AS( SELECT 'Party Casino'                                          AS Brand
                        ,DATE(date)                                               AS Date
                        ,CONCAT('appnetworks_',regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.',''))    AS Publisher
                        ,CASE
                            WHEN REGEXP_CONTAINS(Campaign_c__AppsFlyer,'c:')
                                THEN SUBSTR(SPLIT(Campaign_c__AppsFlyer, 'c:')[SAFE_OFFSET(1)],1,5)
                          -- create id using brand, publisher and campaign name
                            ELSE 'pc'||'_'||regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign_c__AppsFlyer),r'| |_|-|\.','')
                            END                                                   AS campaign_id -- no ID, check for CID else use created id
                        ,MAX(Campaign_c__AppsFlyer)                               AS Campaign_name -- max to account for multiple names per CID+Date
                        ,'GBP'                                                    AS Currency
                 FROM  {{ source('gvc_tomorrow', 'APPSFLYER_APPNETWORKS') }}
                 WHERE date >= '2021-01-01'
                 AND (Clicks__AppsFlyer>0 OR Impressions__AppsFlyer>0)
                 and regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.','') not in (select id from `api-project-786064088220.AttributionMediaReporting_PP.App_Networks_Exclusion_List`)
                 GROUP BY 1,2,3,4,6
              UNION ALL
                SELECT 'Party Casino'                                          AS Brand
                    ,case
                        when app.Term='daily' then app.Start_Date
                        when app.Term<>'daily' and cal.calendar_date<>app.End_Date then cal.calendar_date
                        else null
                    end as Date
                    ,CONCAT('appnetworks_',regexp_replace(lower(case when regexp_contains(Partner_name,'(?i)mooko') then 'mooko' else Partner_name end),r'_int| |_|-|\.','')) as Publisher
                    ,case
                        when REGEXP_CONTAINS(Campaign_name,'c:') then SUBSTR(SPLIT(Campaign_name, 'c:')[SAFE_OFFSET(1)],1,5)
                        -- create id using brand, publisher and campaign name
                        else 'pc'||'_'||regexp_replace(lower(case when regexp_contains(Partner_name,'(?i)mooko') then 'mooko' else Partner_name end),r'_int| |_|-|\.','')
                                                            ||'_'||regexp_replace(lower(Campaign_name),r'| |_|-|\.','')
                    end                                               as campaign_id -- no ID, check for CID else use created id
                    ,MAX(Campaign_name)                               AS Campaign_name -- max to account for multiple names per CID+Date
                    ,upper(trim(currency))                            AS Currency
                FROM {{ source('gvc_partners_costs', 'PartyCasino_AppNetworks') }} app
                join {{ source('DWPRODVIEWSMSTR', 'DIM_CALENDAR') }} cal on cal.calendar_date>=app.Start_Date and cal.calendar_date<=app.End_Date
                WHERE app.Start_Date >= '2019-01-01'
                GROUP BY 1,2,3,4,6
              UNION ALL
                 SELECT distinct 'Party Poker'                                      AS Brand
                         ,CASE WHEN app.Term='daily' THEN app.Start_Date
                               WHEN app.Term<>'daily' AND cal.calendar_date<>app.End_Date THEN cal.calendar_date
                               ELSE NULL
                          END                                                       AS Date
                         ,CONCAT('appnetworks_',regexp_replace(lower(Partner_name),r'_int| |_|-|\.',''))                       AS Publisher
                         ,CASE
                            WHEN REGEXP_CONTAINS(Campaign_name,'c:')
                                THEN SUBSTR(SPLIT(Campaign_name, 'c:')[SAFE_OFFSET(1)],1,5)
                          -- create id using brand, publisher and campaign name
                            ELSE 'pp'||'_'||regexp_replace(lower(Partner_name),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign_name),r'| |_|-|\.','')
                            END                                                     AS campaign_id -- no ID, check for CID else use created id
                         ,MAX(Campaign_name)                                        AS Campaign_name -- max to account for multiple names per CID+Date
                         ,currency                                                  AS Currency
                  FROM {{ source('PartyPoker_Offline', 'Partypoker_Appnetwork_Spend') }}  app
                  JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_CALENDAR') }} cal 
                    ON cal.calendar_date>=app.Start_Date and cal.calendar_date<=app.End_Date
                  WHERE app.Start_Date >= '2021-01-01'
                  GROUP BY 1,2,3,4,6
                ),
appnexus      AS( SELECT * FROM (
                        SELECT distinct advertiser_name                                    AS Brand
                                ,DATE(day)                                                 AS Date
                                ,'AppNexus'                                                AS Publisher
                                ,CAST(line_item_id AS STRING)                              AS campaign_id
                                ,line_item_name                                            AS Campaign_name
                                ,buying_currency                                           AS Currency
                        FROM {{ source('gvc_fivetran_email', 'appnexus') }}
                        WHERE Day >= '2021-01-01'
                    UNION ALL
                        SELECT distinct 'Party Casino'                                     AS Brand
                                ,DATE(date)                                                AS Date
                                ,'AppNexus'                                                AS Publisher
                                ,CAST(line_item_id AS STRING)                              AS campaign_id
                                ,line_item_name                                            AS Campaign_name
                                ,currency                                                  AS Currency
                        FROM {{ source('gvc_tomorrow', 'AppNexus') }} 
                        WHERE date >= '2021-01-01'
                    )
                    WHERE (REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                       OR  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                 ),

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
                                     ,CAST(c.id AS STRING)                                          AS campaign_id
                                     ,c.name                                                        AS campaign_name
                                     ,IF(REGEXP_CONTAINS(c.name, 'c:'),1,NULL)                      AS cid
                                     ,LENGTH(c.name)                                                AS length
                                     ,currency_code                                                 AS Currency

                                   FROM {{ source('gvc_bing_ads', 'campaign_history') }}  c
                                   JOIN {{ source('gvc_bing_ads', 'account_history') }}   a ON c.account_id = a.id
                                   GROUP BY 1,2,4,5,6,7,8)
                            )WHERE rank =1 AND (REGEXP_CONTAINS(brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                                               OR  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                    )
                 ),

connected_tv      AS( SELECT distinct 'Party Casino'                               AS Brand
                        ,DATE(date)                                                AS Date
                        ,'Connected TV'                                            AS Publisher
                        ,CAST(line_item_id AS STRING)                              AS campaign_id
                        ,line_item_name                                            AS Campaign_name
                        ,currency                                                  AS Currency
                      FROM {{ source('gvc_tomorrow', 'Connected_TV') }}
                      WHERE date >= '2021-01-01'
                     ),


dv360         AS( SELECT distinct advertiser                                                                AS Brand
                         ,CAST(CONCAT(SUBSTR(date,1,4),"-",SUBSTR(date,6,2),"-",SUBSTR(date,9,2)) AS DATE)  AS Date
                         ,'DV360'                                                                           AS Publisher
                         ,CAST(line_item_id AS STRING)                                                      AS campaign_id
                         ,line_item                                                                         AS campaign_name
                         ,advertiser_currency                                                               AS Currency
                  FROM {{ source('gvc_google_display_and_video_360', 'fivetran_datasets') }}
                  WHERE CAST(CONCAT(SUBSTR(date,1,4),"-",SUBSTR(date,6,2),"-",SUBSTR(date,9,2)) AS DATE)>='2021-01-01'
                  AND (REGEXP_CONTAINS(advertiser, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                    OR REGEXP_CONTAINS(line_item, '(?i)partycasino|party casino|partypoker|party poker'))
                ),

engageya      AS( SELECT distinct 'Party Casino'                                   AS Brand
                        ,DATE(date)                                                AS Date
                        ,'Engageya'                                                AS Publisher
                        ,CAST(Campaign_Id AS STRING)                               AS campaign_id
                        ,Campaign_name                                             AS Campaign_name
                        ,'USD'                                                     AS Currency
                 FROM {{ source('gvc_tomorrow', 'EngageYa') }}
                 WHERE date >= '2021-01-01'
                ),

facebook_ads  AS(SELECT * FROM(
                SELECT distinct CASE WHEN account_name IS NOT NULL THEN account_name
                                       ELSE h.name END AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Facebook_ads'                                            AS Publisher
                         ,IFNULL(CAST(campaign_id AS STRING), ad_id)                AS campaign_id
                         ,IFNULL(campaign_name, ad_name)                            AS campaign_name
                         ,currency                                                  AS Currency
                  FROM {{ source('gvc_facebook', 'fivetran_datasets') }}  F
                  LEFT JOIN {{ source('gvc_facebook_ad_account', 'account_history') }}  H
                         ON account_id = id
                  WHERE DATE(date) >= '2021-01-01'
              UNION ALL
                  SELECT distinct CASE WHEN account_name IS NOT NULL THEN account_name
                                       ELSE h.name END AS Brand
                         ,DATE(date)                                                AS Date
                         ,'Facebook_ads'                                            AS Publisher
                         ,IFNULL(CAST(campaign_id AS STRING), ad_id)                AS campaign_id
                         ,IFNULL(campaign_name, ad_name)                            AS campaign_name
                         ,currency                                                  AS Currency

                  FROM {{ source('gvc_facebook_additional', 'fivetran_datasets') }}  F
                  LEFT JOIN {{ source('gvc_facebook_ad_account', 'account_history') }}  H
                         ON account_id = id
                  WHERE account_id != 548349136005374
                    AND DATE(date) >= '2021-01-01'
                )
                WHERE (REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                   OR  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                ),
forza      AS( SELECT distinct 'Party Casino'                                       AS Brand
                        ,DATE(date)                                                 AS Date
                        ,'Forza'                                                    AS Publisher
                        ,'00_forza'                                                 AS campaign_id
                        , ''                                                        AS Campaign_name
                        ,'GBP'                                                      AS Currency
                 FROM {{ source('gvc_tomorrow', 'Forza') }}  
                 WHERE date >= '2021-01-01'
                ),
  google_ads    AS( SELECT * FROM(
                    SELECT distinct account_descriptive_name                         AS Brand
                         ,DATE(date)                                                 AS date
                         ,CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod|uac')
                          THEN 'Google_UAC' ELSE 'Google_Ads' END                    AS Publisher
                         ,CAST(campaign_id AS STRING)                                AS campaign_id
                         ,ANY_VALUE(campaign_name)                                   AS campaign_name
                         ,account_currency_code                                      AS Currency
                         FROM (SELECT account_descriptive_name,date,campaign_name,campaign_id,account_currency_code FROM {{ source('gvc_google_ads_campaign_performance_mcc_partypoker', 'fivetran_datasets') }}
                               UNION ALL
                               SELECT account_descriptive_name,date,campaign_name,campaign_id,account_currency_code FROM {{ source('gvc_google_ads_campaign_performance_mcc_partycasino', 'fivetran_datasets') }}
                               UNION ALL
                               SELECT account_descriptive_name,date,campaign_name,campaign_id,account_currency_code FROM {{ source('gvc_google_ads_campaign_performance_party_casino', 'fivetran_datasets') }}
                               UNION ALL
                               SELECT account_descriptive_name,date,campaign_name,campaign_id,account_currency_code FROM {{ source('gvc_google_ads_campaign_performance_party_gaming', 'fivetran_datasets') }}
                               UNION ALL
                               SELECT account_descriptive_name,date,campaign_name,campaign_id,account_currency_code FROM {{ source('gvc_google_ads_campaign_performance_bwin_mcc', 'fivetran_datasets') }}
                        )

                  WHERE DATE(date) >= '2021-01-01'
                  GROUP BY 1,2,3,4,6
                    )
                    WHERE (REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                       OR  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                 ),
 hubble      AS( SELECT distinct 'Party Casino'                                      AS Brand
                          ,DATE(date)                                                AS Date
                          ,Platform                                                  AS Publisher
                          ,'pc'||'_'||regexp_replace(lower(Platform),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign),r'| |_|-|\.','')   AS campaign_id -- no ID, use name
                          ,Campaign                                                  AS Campaign_name
                          ,Currency                                                  AS Currency
                   FROM {{ source('gvc_offline_marketing', 'PartyCasino_Brand_Hubble') }} 
                   where CAST(DATE(date) AS DATE) >='2021-01-01'
                   and (Spend>0 or Link_Clicks>0 or impressions>0)
                  ),
 missmarcadores   AS( SELECT distinct 'Party Casino'                           AS Brand
                         ,DATE(date)                                           AS Date
                         ,'MissMarcadores'                                     AS Publisher
                         ,'00_marcadores'                                      AS campaign_id
                         , ''                                                  AS Campaign_name
                         ,'GBP'                                                AS Currency
                  FROM {{ source('gvc_tomorrow', 'MissMarcadores') }}  
                  WHERE date >= '2021-01-01'
                 ),
 n365      AS( SELECT distinct 'Party Casino'                                  AS Brand
                         ,DATE(date)                                           AS Date
                         ,'N365'                                               AS Publisher
                         ,CAST(LineItemID AS STRING)                           AS campaign_id
                         ,LineItem                                             AS Campaign_name
                         ,Currency                                             AS Currency
                  FROM {{ source('gvc_tomorrow', 'N365') }}
                  WHERE date >= '2021-01-01'
           UNION ALL
               SELECT distinct 'Party Poker'                                   AS Brand
                      ,DATE(date)                                              AS Date
                      ,'N365'                                                  AS Publisher
                      , CAST(Campaign_Id AS STRING)                            AS campaign_id
                      ,Campaign_name_                                          AS Campaign_name
                      ,Currency                                                AS Currency
               FROM {{ source('PartyPoker_Offline', 'N365_Data3') }}
               WHERE date >= '2021-01-01' AND spend >0
              ),

smadex      AS( SELECT distinct 'Party Poker'                                  AS Brand
                       , CAST(LEFT(date_time,10) AS DATE)                      AS Date
                       ,'Smadex'                                               AS Publisher
                       ,CAST(Campaign_ID AS STRING)                            AS campaign_id
                       ,Campaign_Name                                          AS Campaign_name
                       ,Currency                                               AS Currency
                FROM {{ source('gvc_smadex', 'historical_data') }} 
                WHERE CAST(left(date_time,10) AS DATE) >= '2021-01-01'
                ),
/*
sky_adsmart  AS( SELECT distinct 'Party Poker'                                  AS Brand
                        ,SAFE_CAST(start_Date AS date)                          AS Date
                        ,'Sky_Adsmart'                                          AS Publisher
                        ,''                                                     AS campaign_id
                        ,''                                                     AS campaign_name
                        ,'GBP'                                                  AS Currency
                      FROM {{ source('gvc_offline_marketing', 'PartyPoker_UK') }}
                      where SAFE_CAST(start_Date AS date) >= '2021-01-01'
               ),
*/
snapchat_ads  AS( SELECT distinct a.name                                               AS Brand
                            ,DATE(c.updated_at)                                        AS Date
                            ,'Snapchat_ads'                                            AS Publisher
                            ,CAST(c.id AS STRING)                                      AS campaign_id
                            ,c.name                                                    AS campaign_name
                            ,Currency                                                  AS Currency

                  FROM {{ source('snapchat_ads', 'campaign_history') }}        c
                  JOIN {{ source('snapchat_ads', 'ad_account_history') }} a ON a.id = c.ad_account_id
                  WHERE REGEXP_CONTAINS(a.name, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                     OR REGEXP_CONTAINS(c.name, '(?i)partycasino|party casino|partypoker|party poker')
                    ),

taboola      AS(  SELECT distinct 'Party Casino'                                        AS Brand
                         ,DATE(date)                                                    AS Date
                         ,'Taboola'                                                     AS Publisher
                         ,CAST(Campaign_ID AS STRING)                                   AS campaign_id
                         ,Campaign_Name                                                 AS Campaign_name
                         ,Currency                                                      AS Currency
                  FROM {{ source('gvc_tomorrow', 'Taboola') }} 
                  WHERE date >= '2021-01-01'
              ),
tradedesk    AS(  SELECT distinct Advertiser                                           AS Brand
                            ,date                                                      AS Date
                            ,'TradeDesk'                                               AS Publisher
                            ,CAST(Campaign_ID AS STRING)                               AS campaign_id
                            ,Campaign                                                  AS campaign_name
                            ,Advertiser_Currency_Code                                  AS Currency
                   FROM {{ source('gvc_tradedesk_', 'raw_data_V2') }}
                   WHERE date >= '2021-01-01'
                     AND REGEXP_CONTAINS(advertiser, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                      OR REGEXP_CONTAINS(campaign,   '(?i)partycasino|party casino|partypoker|party poker')
                ) ,

twitter_ads   AS(  SELECT * EXCEPT(rank) REPLACE( CASE WHEN rank = 1 THEN '2021-01-01' ELSE Date END AS Date)
                   FROM(
                        SELECT *, ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY Date) AS rank
                        FROM(
                            SELECT distinct a.name                                           AS Brand
                                  ,DATE(c.updated_at)                                        AS Date
                                  ,'Twitter_ads'                                             AS Publisher
                                  ,CAST(c.id AS STRING)                                      AS campaign_id
                                  ,c.name                                                    AS campaign_name
                                  ,Currency                                                  AS Currency
                           FROM {{ source('gvc_twitter_ads_final', 'campaign_history') }}  c
                           JOIN {{ source('gvc_twitter_ads_final', 'account_history') }}   a ON c.account_id = a.id 
                           WHERE DATE(c.updated_at) >= '2021-01-01'
                         )
                        where (REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                            or  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                       )
                  ),
verizon      AS(  SELECT * FROM(
                    SELECT advertiser                                               AS Brand
                         ,day                                                       AS date
                         ,'Verizon'                                                 AS Publisher
                         ,CAST(campaign_id AS STRING)                               AS campaign_id
                         ,campaign                                                  AS campaign_name
                         ,'USD'                                                     AS Currency

                  FROM (SELECT * REPLACE(CAST(CONCAT(SUBSTR(DAY,7,4),"-",SUBSTR(DAY,1,2),"-",SUBSTR(DAY,4,2)) AS DATE) AS Day)
                        FROM {{ source('gvc_fivetran_email', 'verizon_dsp') }})
                  WHERE day >='2021-01-01'
                    AND campaign IS NOT NULL
                  GROUP BY 1,2,3,4,5
                  )
                  where (REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming|partypoker|party poker')
                    or  REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino|partypoker|party poker'))
                ),
viber      AS( SELECT distinct 'Party Casino'                                       AS Brand
                        ,DATE(date)                                                 AS Date
                        ,'Viber'                                                    AS Publisher
                        , LineItem                                                  AS campaign_id -- no ID
                        , LineItem                                                  AS Campaign_name
                        ,Currency                                                   AS Currency
               FROM {{ source('gvc_tomorrow', 'Viber') }} 
               WHERE date >= '2021-01-01'
              ),



final        AS(  SELECT CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)youtube| yt|_yt_') THEN 'Youtube'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)guac') THEN 'Google_UAC'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)snap') THEN 'Snapchat_ads'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)twitter') THEN 'Twitter_ads'
                              WHEN (REGEXP_CONTAINS(Publisher, '(?i)google') AND REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)gads') )
                                  THEN 'DCM'
                              ELSE Publisher END AS Publisher
                         , Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name,
                         Date,
                         CAST(IFNULL(DATE_ADD(LEAD(Date,1) OVER (PARTITION BY Brand, Publisher, campaign_id ORDER BY Date), INTERVAL -1 DAY), '2022-12-31') AS Date) AS End_date
                         , Currency
                  FROM(
                        SELECT * FROM apple_ads     UNION ALL
                        SELECT * FROM bing_ads      UNION ALL
                        SELECT * FROM twitter_ads   UNION ALL
                        SELECT * FROM snapchat_ads
                    )
                  UNION ALL
                  SELECT CASE WHEN REGEXP_CONTAINS(campaign_name, '(?i)youtube| yt|_yt_') THEN 'Youtube'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)guac') THEN 'Google_UAC'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)snap') THEN 'Snapchat_ads'
                              WHEN REGEXP_CONTAINS(campaign_name, '(?i)twitter') THEN 'Twitter_ads'
                              WHEN (REGEXP_CONTAINS(Publisher, '(?i)google') AND REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)gads') )
                              THEN 'DCM'
                              ELSE Publisher END AS Publisher
                          , Brand, campaign_id,
                         CASE WHEN REGEXP_CONTAINS(publisher, '(?i)oogl|bing') AND REGEXP_CONTAINS(campaign_name, ' - ') AND REGEXP_CONTAINS(campaign_name, 'cid')
                              THEN REGEXP_REPLACE(REGEXP_REPLACE(campaign_name, ' - ', ' _ '), '@@@z|@@@Z|@@Z','')
                              ELSE REGEXP_REPLACE(campaign_name, '@@@z|@@@Z|@@Z','')
                         END AS campaign_name, MIN(Date) AS Date, MAX(Date) AS End_date
                         , Currency
                  FROM(
                        SELECT * FROM sevenstars        UNION ALL
                        SELECT * FROM amobee			      UNION ALL
                        SELECT * FROM appnetworks       UNION ALL
                        SELECT * FROM appnexus          UNION ALL
                        SELECT * FROM connected_tv      UNION ALL
                        SELECT * FROM dv360             UNION ALL
                        SELECT * FROM engageya          UNION ALL
                        SELECT * FROM facebook_ads      UNION ALL
                        SELECT * FROM forza             UNION ALL
                        SELECT * FROM google_ads        UNION ALL
                        SELECT * FROM hubble            UNION ALL
                        SELECT * FROM missmarcadores    UNION ALL
                        SELECT * FROM n365              UNION ALL
                        --SELECT * FROM sky_adsmart       UNION ALL
                        SELECT * FROM smadex            UNION ALL
                        SELECT * FROM taboola           UNION ALL
                        SELECT * FROM tradedesk         UNION ALL
                        --SELECT * FROM twitch            UNION ALL
                        SELECT * FROM verizon           UNION ALL
                        SELECT * FROM viber
                        )
                  GROUP BY 1,2,3,4,7
                  )


SELECT a.* EXCEPT(Date, end_date)
        REPLACE (case when a.ChannelGrouping = 'PPC - Other' and b.ChannelGrouping is not null then b.ChannelGrouping else a.ChannelGrouping end as ChannelGrouping
                , case when REGEXP_CONTAINS(Publisher,'appnetworks_') then INITCAP(lower(REGEXP_REPLACE(Publisher, 'appnetworks_', ''))) else INITCAP(lower(Publisher)) end as Publisher)
        ,IFNULL(DATE_ADD(LAG(end_date) OVER (PARTITION BY Brand, Publisher, campaign_id ORDER BY date), INTERVAL 1 DAY), date) AS date, end_date
        , CASE WHEN REGEXP_CONTAINS(campaign_name, 'cid:')
               THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
               WHEN REGEXP_CONTAINS(campaign_name, 'c:')
               THEN SAFE_CAST(SUBSTR(SPLIT(REGEXP_REPLACE(campaign_name, '-', '|'), 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
          END AS cid
        , ROW_NUMBER() OVER (PARTITION BY Publisher, brand, campaign_id ORDER BY end_date DESC) AS newest_rank
        , CASE WHEN publisher in ('Forza','MissMarcadores',/*'Sky_Adsmart',*/'Twitch') THEN 'publisher' ELSE 'campaign' END AS join_lvl
FROM(
        SELECT  Publisher
               ,CASE WHEN REGEXP_CONTAINS(Brand, '(?i)partycasino|party casino|Party Gaming') OR REGEXP_CONTAINS(campaign_name, '(?i)partycasino|party casino') THEN 'Party Casino'
                     WHEN REGEXP_CONTAINS(Brand, '(?i)partypoker|party poker')                OR REGEXP_CONTAINS(campaign_name, '(?i)partypoker|party poker')   THEN 'Party Poker'
                     END AS brand
                ,campaign_id, campaign_name, MIN(Date) AS Date, MAX(End_Date) AS End_Date
                 , CASE
                    WHEN REGEXP_CONTAINS(Publisher, '(?i)connected_tv|sky_adsmart')                                                         THEN 'Offline'
                    WHEN REGEXP_CONTAINS(campaign_name, '(?i)vod') or REGEXP_CONTAINS(Publisher, '(?i)youtube')                                    THEN 'Display - VOD'
                    WHEN REGEXP_CONTAINS(Publisher, 'DCMM_')                                                                                       THEN 'Display - Partners'
                    WHEN REGEXP_CONTAINS(Publisher, '(?i)n365|outbrain|taboola')                                                                   THEN 'Display - Direct'
                    WHEN (REGEXP_CONTAINS(campaign_name, '(?i)prog') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)programma_'))
                        or REGEXP_CONTAINS(Publisher, '(?i)dv360|appnexus|tradedesk|amobee')                                                           THEN 'Display - Programmatic'
                    WHEN REGEXP_CONTAINS(Publisher, '(?i)dating_apps|appsflyer|engageya|forza|missmarcadores|appnetworks')
                        or (REGEXP_CONTAINS(campaign_name, '(?i)partner')
                              AND NOT REGEXP_CONTAINS(campaign_name,'(?i)dcm|twitter|facebook|snapchat|twitch|bing|google|uac|apple')
                              AND NOT REGEXP_CONTAINS(Publisher,'(?i)dcm|twitter|facebook|snapchat|twitch|bing|google|uac|apple'))                 THEN 'Display - Partners'
                    WHEN REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)cpc')                           THEN 'Display - Other'
                    WHEN REGEXP_CONTAINS(campaign_name, '(?i)uac|apple') OR REGEXP_CONTAINS(Publisher, '(?i)uac|apple') THEN
                    CASE
                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                        OR REGEXP_CONTAINS(campaign_name, '(?i)Competitor')                                                                                  THEN 'UAC - Competitor'
                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                        OR REGEXP_CONTAINS(campaign_name,'(?i)Generic|search_non_brand|-gen-|non brand') OR campaign_name LIKE '%|gen|%'                     THEN 'UAC - Generic'
                    WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                        OR REGEXP_CONTAINS(campaign_name, '(?i)Brand')                                                                                       THEN 'UAC - Brand'
                    ELSE 'UAC - Other'
                    END
                    WHEN REGEXP_CONTAINS(Publisher, '(?i)twitter|facebook|snapchat|twitch') OR REGEXP_CONTAINS(campaign_name, '(?i)twitter|facebook|snapchat')  THEN
                    CASE
                        WHEN REGEXP_CONTAINS(campaign_name, '(?i)crm')                                                                                        THEN 'CRM - Social'
                        ELSE 'Social - Paid'
                    END
                    WHEN REGEXP_CONTAINS(Publisher, '(?i)bing|google')                    THEN
                    CASE
                        WHEN REGEXP_CONTAINS(campaign_name, '(?i)display') AND NOT REGEXP_CONTAINS(campaign_name, '(?i)gads')                                 THEN 'Display - Other'
                        WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'comp')
                        OR REGEXP_CONTAINS(campaign_name, r'(?i)Competitor|comp|com\_|com\+')                                                                 THEN 'PPC - Competitor'
                        WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'gen' )
                        OR REGEXP_CONTAINS(campaign_name, r'(?i)Generic|search_non_brand|non brand|gen\_|gen\+|\|gen\|')                                      THEN 'PPC - Generic'
                        WHEN REGEXP_CONTAINS(SPLIT(IF(REGEXP_CONTAINS(campaign_name, 'c:'), REGEXP_REPLACE(campaign_name, '-', '|'), campaign_name), '|')[SAFE_OFFSET(7)], 'bnd' )
                        OR REGEXP_CONTAINS(campaign_name, r'(?i)Brand|brd\_|brd\+')                                                                           THEN 'PPC - Brand'
                        ELSE 'PPC - Other'
                    END
                    ELSE 'Display - Other'
                END AS ChannelGrouping
                , upper(trim(Currency)) as Currency

        FROM final
        WHERE campaign_id IS NOT NULL
        GROUP BY 1,2,3,4,7,8
    ) a
    left join `api-project-786064088220.AttributionMediaReporting_PP.PPC_Other_lkp` b
    on REGEXP_REPLACE(lower(a.campaign_name), '  | ', '') = lower(b.campaign)
