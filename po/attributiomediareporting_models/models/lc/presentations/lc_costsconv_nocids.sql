WITH ptnr2 AS  (SELECT distinct LOWER(partner) FROM {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }} ),

     final AS( SELECT * REPLACE(LOWER(campaign) AS campaign)
               FROM(  SELECT * REPLACE(CASE WHEN source = 'dfa' AND (NOT REGEXP_CONTAINS(Campaign, 'notset|not set') OR campaign IS NOT NULL)
                                            THEN '' ELSE source END AS source)
                              ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:') [SAFE_OFFSET(1)],1,5) AS INT64)    
                                    WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:' )  [SAFE_OFFSET(1)],1,5) AS INT64)
                                    WHEN REGEXP_CONTAINS(adcontent,'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                    WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)],1,5) AS INT64)
                                    WHEN REGEXP_CONTAINS(ChannelGrouping,'artners') AND NOT REGEXP_CONTAINS(Brand, '(?i)gala') AND wm_tracking> 4995000 THEN wm_tracking
                                    WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)affiliate') AND wm_tracking IS NOT NULL THEN wm_tracking
                                    END AS cid
                              ,CASE WHEN REGEXP_CONTAINS(source, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL)
                                     AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
                                    THEN 1 END AS google_excl
                              ,CASE WHEN ChannelGrouping = 'Display - Partners' THEN
                                    CASE WHEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(0)]) IN (SELECT * FROM ptnr2) THEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(0)])
                                         WHEN LOWER(SPLIT(REGEXP_REPLACE(source, 'network',''),'_')[SAFE_OFFSET(0)]) IN (SELECT * FROM ptnr2) THEN LOWER(SPLIT(REGEXP_REPLACE(source, 'network','')  , '_')[SAFE_OFFSET(0)])
                                         WHEN adcontent IS NOT NULL AND NOT REGEXP_CONTAINS(adcontent, 'cid|c:') AND adcontent <> '' THEN adcontent 
                                         ELSE source END
                               END AS partner_name
                               
                       FROM  {{ref('lc_conversions_unique_ledger')}}
                       WHERE Date> '2019-12-31' )
               WHERE google_excl IS NULL AND (cid IS NULL OR cid IN (52840, 00000))
                ),
  
       lal AS (SELECT * FROM(       
                      SELECT * REPLACE(CAST(Date AS DATE) AS Date), 
                            CASE WHEN REGEXP_CONTAINS(campaign_name, 'cid') THEN SAFE_CAST(SUBSTR(SPLIT(campaign_name, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64) 
                                 WHEN REGEXP_CONTAINS(Campaign, '(?i)c:'  ) THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                                 WHEN (REGEXP_CONTAINS(ChannelGrouping,'artners') AND NOT REGEXP_CONTAINS(Brand, '(?i)gala') AND SAFE_CAST(campaign_id AS INT64)> 4995000) 
                                   OR REGEXP_CONTAINS(ChannelGrouping,'ffili')  THEN SAFE_CAST(campaign_id AS INT64)
                            END AS cid
                      FROM  {{ref('lc_campaigns_costs')}}
                      WHERE Date > '2020-01-20' AND REGEXP_CONTAINS(Brand, '(?i)coral|ladbrokes') AND google_excl IS NULL)
               WHERE cid IS NULL OR cid IN (52840, 00000)
              )

SELECT *
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
        
    FROM(
                  SELECT  Date                                                                                        AS Date
                         ,Brand                                                                                       AS Brand
                         ,ChannelGrouping                                                                             AS ChannelGrouping
                         ,source                                                                                      AS Publisher
                         ,REGEXP_REPLACE(campaign, 'c-wm', 'c_wm')                                                    AS Campaign
                         ,SUM(IF(Conversion = 'Registration', 1, 0))                                                  AS Registrations
                         ,SUM(IF(Conversion = 'FTD'         , 1, 0))                                                  AS FTDs
                         ,ROUND(SUM(IF(Conversion = 'FTD'         , event_value, 0)),0)                               AS FTDs_Value
                         ,SUM(IF(Conversion = 'Deposit'     , 1, 0))                                                  AS Deposits
                         ,ROUND(SUM(IF(Conversion = 'Deposit'     , event_value, 0)),0)                               AS Deposits_Value
                         ,SUM(IF(Conversion = 'Bet'         , 1, 0))                                                  AS Bets
                         ,ROUND(SUM(IF(Conversion = 'Bet'         , event_value, 0)),0)                               AS Bets_Value
                         ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Dataset, 'DCM')        , 1, 0))           AS Total_DCM_Conversions
                         ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Dataset, 'pps')        , 1, 0))           AS Total_AppsFlyer_Conversions
                         ,SUM(IF(Conversion IS NOT NULL AND NOT REGEXP_CONTAINS(Conv_medium, 'App'), 1, 0))           AS Total_Web_Conversions
                         ,SUM(IF(Conversion IS NOT NULL AND REGEXP_CONTAINS(Conv_medium, 'App')    , 1, 0))           AS Total_App_Conversions
                         ,SUM(Click_Conversion)                                                                       AS Click_Conversions
                         ,SUM(View_Conversion)                                                                        AS View_Conversions
                         ,ROUND(SUM(pNGR_0),2)                                                                        AS Total_pNGR_0
                         ,SUM(IF(pNGR_0 IS NOT NULL,1,0))                                                             AS Count_pNGR_0
                         ,ROUND(SUM(pNGR_0)/SUM(IF(pNGR_0 IS NOT NULL,1,0)),2)                                        AS Average_pNGR_0
                         ,REGEXP_REPLACE(LOWER(REGEXP_REPLACE(campaign, 'c-wm', 'c_wm')), ' ', '')                    AS cpname
                         ,MAX(wm_tracking)                                                                            AS GA_wm_track
                         ,partner_name
                         ,SUM(IF(Conversion = 'Registration'  AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))              AS Registrations_72h
                         ,SUM(IF(Conversion = 'FTD'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))              AS FTDs_72h
                         ,ROUND(SUM(IF(Conversion = 'FTD'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value, 0)),0) AS FTDs_Value_72h
                         ,SUM(IF(Conversion = 'Deposit'       AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))              AS Deposits_72h
                         ,ROUND(SUM(IF(Conversion = 'Deposit' AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value, 0)),0) AS Deposits_Value_72h
                         ,SUM(IF(Conversion = 'Bet'           AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), 1, 0))              AS Bets_72h
                         ,ROUND(SUM(IF(Conversion = 'Bet'     AND (lag_hours <=72 OR (source ='Apple' AND lag_hours=99)), event_value, 0)),0) AS Bets_Value_72h
                         ,ROUND(SUM(IF((lag_hours <=72 OR (source ='Apple' AND lag_hours=99)),pNGR_0,0)),2)                                   AS Total_pNGR_0_72h
                         
                  FROM final
                  WHERE Date > '2019-12-31'
                  GROUP BY 1,2,3,4,5,22,24) f

FULL OUTER JOIN lal cc
ON f.Brand = cc.Brand AND f.Date = cc.Date AND f.cpname = cc.Campaign)







          
