WITH  ptnr2 AS (SELECT distinct SAFE_CAST(campaign_id AS INT64) AS tracker_id, LOWER(publisher) AS partner
                FROM {{ref('gvc_dim_campaigns')}}
                WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND ChannelGrouping = 'Display - Partners' AND NOT REGEXP_CONTAINS(Publisher, '(?i)dcmm') AND SAFE_CAST(campaign_id AS INT64) IS NOT NULL),

     final AS( SELECT * REPLACE(CASE WHEN ChannelGrouping = 'Direct' THEN '(notset)' ELSE LOWER(campaign) END AS campaign,
                                CASE WHEN ChannelGrouping = 'Direct' THEN '(Direct)' ELSE source END AS source)
               FROM(  SELECT * REPLACE(CASE WHEN source = 'dfa' AND (NOT REGEXP_CONTAINS(Campaign, 'notset|not set') OR campaign IS NOT NULL)
                                            THEN '' ELSE source END AS source)
                              ,CASE WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:' )  [SAFE_OFFSET(1)],1,5) AS INT64)
                                    WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)],1,5) AS INT64)
                                    WHEN (REGEXP_CONTAINS(ChannelGrouping,'artners') AND wm_tracking> 4600000) OR REGEXP_CONTAINS(ChannelGrouping,'(?i)Affi') THEN wm_tracking
                                    END AS cid
                              ,CASE WHEN REGEXP_CONTAINS(source, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL)
                                     AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
                                    THEN 1 END AS google_excl
                              ,CASE WHEN ChannelGrouping = 'Display - Partners' THEN
                                    CASE WHEN partner IS NOT NULL THEN partner 
                                         WHEN REGEXP_CONTAINS(source, '(?i)wakeapp') THEN LOWER(source) END
                                    END AS partner_name
                               
                       FROM {{ref('gvc_conversions_unique_ledger')}}
                       LEFT JOIN ptnr2 cc               
                              ON tracker_id = wm_tracking 
                       WHERE Date >= '2021-01-01')
               WHERE google_excl IS NULL AND cid IS NULL),
                
  
       lal AS ( SELECT * REPLACE(CAST(Date AS DATE) AS Date)
                FROM {{ref('gvc_campaigns_costs')}}
                WHERE Date >= '2021-01-01' AND join_type ='name' 
              ), 
              
              
       rar AS ( SELECT  Date                                                                                        AS Date
                       ,Brand                                                                                       AS Brand
                       ,ChannelGrouping                                                                             AS ChannelGrouping
                       ,source                                                                                      AS Publisher
                       ,REGEXP_REPLACE(REGEXP_REPLACE(campaign, 'c-wm', 'c_wm'), '//','/')                          AS Campaign
                       ,SUM(IF(Conversion = 'Registration', 1, 0))                                                  AS Registrations
                       ,SUM(IF(Conversion = 'FTD'         , 1, 0))                                                  AS FTDs
                       ,ROUND(SUM(IF(Conversion = 'FTD'         , event_value_GBP, 0)),0)                           AS FTDs_Value
                       ,SUM(IF(Conversion = 'Deposit'     , 1, 0))                                                  AS Deposits
                       ,ROUND(SUM(IF(Conversion = 'Deposit'     , event_value_GBP, 0)),0)                           AS Deposits_Value
                       ,SUM(IF(Conversion = 'Bet'         , 1, 0))                                                  AS Bets
                       ,ROUND(SUM(IF(Conversion = 'Bet'         , event_value_GBP, 0)),0)                           AS Bets_Value
                       ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Dataset, 'DCM')        , 1, 0))           AS Total_DCM_Conversions
                       ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Dataset, 'pps')        , 1, 0))           AS Total_AppsFlyer_Conversions
                       ,SUM(IF(Conversion IS NOT NULL AND NOT REGEXP_CONTAINS(Conv_medium, 'App'), 1, 0))           AS Total_Web_Conversions
                       ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Conv_medium, 'App')    , 1, 0))           AS Total_App_Conversions
                       ,SUM(Click_Conversion)                                                                       AS Click_Conversions
                       ,SUM(View_Conversion)                                                                        AS View_Conversions
                       ,ROUND(SUM(pNGR_0),2)                                                                        AS Total_pNGR_0
                       ,SUM(IF(pNGR_0 IS NOT NULL,1,0))                                                             AS Count_pNGR_0
                       ,ROUND(SUM(pNGR_0)/SUM(IF(pNGR_0 IS NOT NULL,1,0)),2)                                        AS Average_pNGR_0
                       ,ROUND(SUM(pNGR_21),2)                                                                       AS Total_pNGR_21
                       --,CASE WHEN source IN ('AppNexus', 'DV360')
                       --      THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(Campaign, '|')[SAFE_OFFSET(1)], '_', SPLIT(Campaign, '|')[SAFE_OFFSET(18)]), '  ', ' '), ' ', '')
                                     --      ELSE REGEXP_REPLACE(LOWER(campaign), ' ', '')
                                     --      END AS cpname
                       ,REGEXP_REPLACE(LOWER(REGEXP_REPLACE(campaign, 'c-wm', 'c_wm')), ' ', '')                    AS cpname
                       ,MAX(wm_tracking)                                                                            AS GA_wm_track
                       ,partner_name
                       ,SUM(IF(Conversion = 'Registration'  AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Registrations_72h
                       ,SUM(IF(Conversion = 'FTD'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS FTDs_72h
                       ,ROUND(SUM(IF(Conversion = 'FTD'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS FTDs_Value_72h
                       ,SUM(IF(Conversion = 'Deposit'       AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Deposits_72h
                       ,ROUND(SUM(IF(Conversion = 'Deposit' AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS Deposits_Value_72h
                       ,SUM(IF(Conversion = 'Bet'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Bets_72h
                       ,ROUND(SUM(IF(Conversion = 'Bet'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS Bets_Value_72h
                       ,ROUND(SUM(IF((lag_hours <=72 OR (source ='Apple' AND lag_hours=99)),pNGR_0,0)),2)                                       AS Total_pNGR_0_72h
                       ,CASE WHEN REGEXP_CONTAINS(channelgrouping, '(?i)direct|referral|organic')  
                             THEN LOWER(country) ELSE NULL END                                                      AS country
                FROM final
                WHERE Date >= '2021-01-01'
                GROUP BY 1,2,3,4,5,23,25,34)




,ru AS(
SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(channelgrouping, '(?i)Offline') THEN Campaign ELSE country END AS country,
                 CASE WHEN REGEXP_CONTAINS(channelgrouping, '(?i)Offline') THEN '' ELSE Campaign END AS Campaign)


FROM(
   SELECT  IFNULL(cc.Date,          f.Date)                                      AS Date
          ,IFNULL(cc.Brand,         f.Brand)                                     AS Brand
          ,CASE WHEN IFNULL(cc.Publisher, f.Publisher) IN ('AppNexus', 'DV360')
                THEN 'Display - Programmatic'
                ELSE IFNULL(cc.ChannelGrouping,  f.ChannelGrouping) END          AS ChannelGrouping
          ,IFNULL(cc.Publisher,        f.Publisher)                              AS Publisher
          ,IFNULL(cc.Campaign_name, f.Campaign)                                  AS Campaign
          ,0 AS Visits
          ,cc.Spend
          ,cc.Clicks
          ,cc.Impressions
          ,Registrations
          ,FTDs
          ,ROUND(FTDs_value, 0) AS FTDs_value
          ,Deposits
          ,ROUND(Deposits_value, 0) AS Deposits_value
          ,Bets
          ,ROUND(Bets_value, 0) AS Bets_value
          ,Total_DCM_Conversions
          ,Total_AppsFlyer_Conversions
          ,Total_Web_Conversions
          ,Total_App_Conversions
          ,Click_Conversions
          ,View_Conversions
          ,Total_pNGR_0
          ,Average_pNGR_0
          ,Total_pNGR_21
          ,0 AS crm_count
          ,GA_wm_track
          ,partner_name
          ,Registrations_72h
          ,FTDs_72h
          ,ROUND(FTDs_Value_72h,0) AS FTDs_Value_72h
          ,Deposits_72h
          ,ROUND(Deposits_Value_72h,0) AS Deposits_Value_72h
          ,Bets_72h
          ,ROUND(Bets_Value_72h,0) AS Bets_Value_72h
          ,ROUND(Total_pNGR_0_72h,0) AS Total_pNGR_0_72h
          ,country
        
    FROM rar f

FULL OUTER JOIN lal cc
ON f.Brand = cc.Brand AND f.Date = cc.Date AND f.cpname = cc.Campaign)
)

SELECT ru.* REPLACE(CASE WHEN Delivered IS NOT NULL THEN delivered ELSE impressions END AS impressions)
FROM ru
LEFT JOIN {{ref('optimove_impressions')}} oi
       ON ru.date = oi.date AND LOWER(Campaign) = LOWER(campaign_desc) AND oi.ChannelGrouping = ru.ChannelGrouping
WHERE LOWER(campaign_desc) IN 
            (SELECT distinct LOWER(campaign) 
            FROM {{ref('gvc_conversions_ledger')}}
            WHERE REGEXP_CONTAINS(ChannelGrouping, '(?i)CRM') )









          
