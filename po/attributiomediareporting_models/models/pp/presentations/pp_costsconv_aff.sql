WITH convs AS (  SELECT *, CASE WHEN Conversion IN ('FTD', 'Registration') THEN 'cpa' ELSE 'rev_share' END AS join_type
                 FROM {{ref('pp_conversions_unique_ledger')}} ul
                 LEFT JOIN (SELECT distinct Tracker_id, beneficiary_id FROM {{ref('affiliates_list')}} ) af
                        ON SAFE_CAST(tracker_id AS INT64) = SAFE_CAST(wm_tracking AS INT64)
                 WHERE Date >= '2021-01-01' AND REGEXP_CONTAINS(ChannelGrouping,  '(?i)aff') ),

      aff AS (   SELECT * FROM {{ref('costs_affiliates')}}
                 WHERE Date_aff >= '2021-01-01' AND REGEXP_CONTAINS(Brand_aff, '(?i)party') ),


     costs AS (  SELECT distinct date_aff,
                                 brand_aff,
                                 acquisition_channel,
                                 src_account_id,
                                 beneficiary_id,
                                 tracker_id,
                                 Country_aff,
                                 IA_CPA_COST AS Referral_Cost,
                                 'cpa' AS cost_type
                 FROM aff WHERE IA_CPA_COST >0

              UNION ALL

                 SELECT distinct date_aff,
                                 brand_aff,
                                 acquisition_channel,
                                 src_account_id,
                                 beneficiary_id,
                                 tracker_id,
                                 Country_aff,
                                 Rev_Share AS Referral_Cost,
                                 'rev_share' AS cost_type
                 FROM aff WHERE ABS(Rev_Share) >0),


     final AS (  SELECT  Date                                                                                         AS Date
                         ,Brand                                                                                       AS Brand
                         ,ChannelGrouping                                                                             AS ChannelGrouping
                         ,""                                                                                          AS Publisher
                         ,join_type                                                                                   AS Campaign
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
                         ,IFNULL('notset', REGEXP_REPLACE(LOWER(REGEXP_REPLACE(campaign, 'c-wm', 'c_wm')), ' ', ''))  AS cpname
                         ,MAX(wm_tracking)                                                                            AS GA_wm_track
                         ,customerid
                         ,SUM(IF(Conversion = 'Registration'  AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Registrations_72h
                         ,SUM(IF(Conversion = 'FTD'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS FTDs_72h
                         ,ROUND(SUM(IF(Conversion = 'FTD'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS FTDs_Value_72h
                         ,SUM(IF(Conversion = 'Deposit'       AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Deposits_72h
                         ,ROUND(SUM(IF(Conversion = 'Deposit' AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS Deposits_Value_72h
                         ,SUM(IF(Conversion = 'Bet'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))                  AS Bets_72h
                         ,ROUND(SUM(IF(Conversion = 'Bet'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value_GBP, 0)),0) AS Bets_Value_72h
                         ,ROUND(SUM(IF((lag_hours <=72 OR (source ='Apple' AND lag_hours=99)),pNGR_0,0)),2)                                       AS Total_pNGR_0_72h
                         ,LOWER(country)                                                                                                          AS country
                         ,MAX(beneficiary_id)                                                                                                     AS beneficiary_id
                  FROM convs
                  GROUP BY 1,2,3,4,5,23,25,34),


        su AS( SELECT *, SAFE_CAST(COALESCE(cc.beneficiary_id, f.beneficiary_id) AS STRING) AS beneficiary_id_
               FROM final f
               FULL OUTER JOIN costs cc
               ON Date = date_aff AND Brand = Brand_aff AND campaign = cost_type
               AND CAST(GA_wm_track AS STRING) = tracker_id AND SAFE_CAST(src_account_id AS INT64) = SAFE_CAST(customerid AS INT64)
               )


SELECT  IFNULL(Date, date_aff)              AS Date
       ,IFNULL(Brand, Brand_aff)            AS Brand
       ,'Affiliate'                         AS ChannelGrouping
       ,beneficiary_id_                     AS Publisher
       ,IFNULL(campaign, cost_type)         AS campaign
       ,0                                   AS Visits
       ,SUM(Referral_Cost)                  AS Spend
       ,0                                   AS Clicks
       ,0                                   AS Impressions
       ,SUM(Registrations)                  AS Registrations
       ,SUM(FTDs)                           AS FTDs
       ,ROUND(SUM(FTDs_value), 0)           AS FTDs_value
       ,SUM(Deposits)                       AS Deposits
       ,ROUND(SUM(Deposits_value), 0)       AS Deposits_value
       ,SUM(Bets)                           AS Bets
       ,ROUND(SUM(Bets_value), 0)           AS Bets_value
       ,SUM(Total_DCM_Conversions)          AS Total_DCM_Conversions
       ,SUM(Total_AppsFlyer_Conversions)    AS Total_AppsFlyer_Conversions
       ,SUM(Total_Web_Conversions)          AS Total_Web_Conversions
       ,SUM(Total_App_Conversions)          AS Total_App_Conversions
       ,SUM(Click_Conversions)              AS Click_Conversions
       ,SUM(View_Conversions)               AS View_Conversions
       ,SUM(Total_pNGR_0)                   AS Total_pNGR_0
       ,SUM(Average_pNGR_0)                 AS Average_pNGR_0
       ,SUM(Total_pNGR_21)                  AS Total_pNGR_21
       ,0                                   AS crm_count
       ,IFNULL(GA_wm_track,
               CAST(tracker_id AS INT64))  AS GA_wm_track
       ,''                                 AS partner_name
       ,''                                 AS cid
       ,SUM(Registrations_72h)             AS Registrations_72h
       ,SUM(FTDs_72h)                      AS FTDs_72h
       ,ROUND(SUM(FTDs_Value_72h),0)       AS FTDs_Value_72h
       ,SUM(Deposits_72h)                  AS Deposits_72h
       ,ROUND(SUM(Deposits_Value_72h),0)   AS Deposits_Value_72h
       ,SUM(Bets_72h)                      AS Bets_72h
       ,ROUND(SUM(Bets_Value_72h),0)       AS Bets_Value_72h
       ,ROUND(SUM(Total_pNGR_0_72h),0)     AS Total_pNGR_0_72h
       ,TRIM(IFNULL(country, country_aff)) AS country

FROM su
GROUP BY 1,2,3,4,5,6,27,38
