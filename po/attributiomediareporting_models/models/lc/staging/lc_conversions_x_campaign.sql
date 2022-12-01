WITH  ptnr2 AS (SELECT distinct LOWER(partner) FROM {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }} ),

       prep AS (SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)Affi') AND wm_tracking IS NOT NULL THEN '' ELSE campaign END AS campaign,
                                 CASE WHEN source = 'dfa' AND (NOT REGEXP_CONTAINS(Campaign, 'notset|not set') OR campaign IS NULL) THEN '' 
                                      WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)Affi') AND wm_tracking IS NOT NULL THEN ''
                                      ELSE source END AS source)
                                      
                      ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:') [SAFE_OFFSET(1)],1,5) AS INT64)    
                            WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  ) [SAFE_OFFSET(1)],1,5) AS INT64)
                            WHEN REGEXP_CONTAINS(adcontent,'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                            WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)],1,5) AS INT64)
                            WHEN REGEXP_CONTAINS(ChannelGrouping,'artners') AND NOT REGEXP_CONTAINS(Brand, '(?i)gala') AND wm_tracking> 4995000 THEN wm_tracking
                            WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)affiliate') AND wm_tracking IS NOT NULL THEN wm_tracking
                            END AS cid
                      ,CASE WHEN ChannelGrouping = 'Display - Partners' THEN
                            CASE WHEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(0)]) IN (SELECT * FROM ptnr2) THEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(0)])
                                 WHEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(1)]) IN (SELECT * FROM ptnr2) THEN LOWER(SPLIT(Campaign, '_')[SAFE_OFFSET(1)])
                                 WHEN SPLIT(LOWER(SPLIT(Campaign, '-')[SAFE_OFFSET(1)]), '_') [SAFE_OFFSET(0)] IN (SELECT * FROM ptnr2) 
                                 THEN SPLIT(LOWER(SPLIT(Campaign, '-')[SAFE_OFFSET(1)]), '_') [SAFE_OFFSET(0)]
                                 WHEN LOWER(SPLIT(REGEXP_REPLACE(source, 'network',''),'_')[SAFE_OFFSET(0)]) IN (SELECT * FROM ptnr2) THEN LOWER(SPLIT(REGEXP_REPLACE(source, 'network','')  , '_')[SAFE_OFFSET(0)])
                                 WHEN adcontent IS NOT NULL AND NOT REGEXP_CONTAINS(adcontent, 'cid|c:') AND adcontent <> '' THEN adcontent END
                            END AS partner_name
               FROM {{ref('lc_conversions_unique_ledger')}} 
               ),
       
     prtner AS( SELECT * EXCEPT(rank_1,length), ROW_NUMBER() OVER( PARTITION BY cid ORDER BY rank_1 DESC, length DESC, campaign) AS rank_2
                FROM(
                    SELECT distinct cid, campaign, LENGTH(campaign) AS length
                          ,CASE WHEN REGEXP_CONTAINS(LOWER(campaign), LOWER(source)) THEN CASE WHEN REGEXP_CONTAINS(campaign, '(|{')  THEN 1 ELSE 2 END END AS rank_1
                    FROM prep
                    WHERE REGEXP_CONTAINS(ChannelGrouping,'artners') AND cid> 4995000)
               ),
      
      
      final AS( SELECT p.* REPLACE(CASE WHEN pa.campaign IS NOT NULL THEN pa.campaign ELSE p.campaign END AS campaign)
                FROM prep p
                LEFT JOIN (SELECT * FROM prtner WHERE rank_2 = 1) pa 
                ON pa.cid = p.cid
               ),
       


         aa AS(             SELECT Date                                                                                         AS Date
                                   ,Brand                                                                                       AS Brand
                                   ,CASE WHEN index = 1 THEN 'Mixed'
                                         WHEN source = 'dbm' THEN 'Display - Programmatic' ELSE ChannelGrouping END             AS ChannelGrouping
                                   ,CASE WHEN index = 1 THEN 'Mixed' 
                                         WHEN source = 'dbm' THEN 'DV360' ELSE source END                                       AS Publisher 
                                   ,MAX(campaign)                                                                               AS Campaign
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
                                   ,cid
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
                                LEFT JOIN {{ref('lc_dim_duplicate_cids')}} d USING (Brand, cid)
                                WHERE Date > '2019-12-31' AND ((cid IS NOT NULL AND cid != 52840) OR campaignid IS NOT NULL)                                
                                GROUP BY 1,2,3,4,22,23
                                )

SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)partner') AND partner_name IS NOT NULL THEN INITCAP(partner_name) 
                      WHEN REGEXP_CONTAINS(Campaign, '(?i)Thingortwoeu') THEN 'Thingortwo'
                      ELSE publisher END AS publisher)
        , CASE WHEN REGEXP_CONTAINS(publisher, 'oogle|earc') 
                AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL)
                AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
               THEN 1 END AS google_excl
FROM aa 


