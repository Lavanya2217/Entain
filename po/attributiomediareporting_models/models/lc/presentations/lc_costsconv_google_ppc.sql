WITH  ffr  AS ( SELECT *
                FROM {{ref('lc_dim_campaigns')}} WHERE CAST(Date AS Date) IS NOT NULL AND newest_rank =1),

     FINAL AS ( SELECT c.* REPLACE (CASE WHEN SAFE_CAST(campaign AS INT64) IS NOT NULL AND d.campaign_name IS NOT NULL
                                         THEN d.campaign_name ELSE campaign END AS campaign,
                                    CASE WHEN SAFE_CAST(campaign AS INT64) IS NOT NULL AND d.campaign_name IS NOT NULL
                                         THEN d.ChannelGrouping ELSE c.ChannelGrouping END AS ChannelGrouping)

                FROM (SELECT * REPLACE(CASE WHEN campaign_id IS NOT NULL THEN campaign_id  WHEN campaignid IS NOT NULL THEN CAST(campaignid AS STRING)
                                          WHEN SAFE_CAST(campaign AS INT64) IS NOT NULL THEN campaign

                                     END AS campaign_id )
                           ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign,  'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                 WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign,  'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                                 WHEN REGEXP_CONTAINS(adcontent, 'cid') THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                 WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                                 END AS cid
                      FROM {{ref('lc_conversions_unique_ledger')}}
                      WHERE 1=1
                        AND REGEXP_CONTAINS(source, 'oogle|earc')
                        AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL)
                        AND NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC')) c
                 LEFT JOIN ffr d
                 ON d.campaign_id = c.campaign
                ),

        lal AS ( SELECT distinct Date, Brand, ChannelGrouping, Publisher, campaign_id
                        ,MAX(campaign_name)          AS Campaign_n
                        ,ROUND(SUM(spend),2)         AS Spend
                        ,ROUND(SUM(clicks))          AS Clicks
                        ,ROUND(SUM(impressions))     AS Impressions

                 FROM(   SELECT * REPLACE(CAST(Date AS DATE) AS Date),
                                  CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)cid') THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                       WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                                  END AS cid
                         FROM {{ref('lc_campaigns_costs')}}
                         WHERE Date > '2020-01-19' AND REGEXP_CONTAINS(Brand, '(?i)coral|ladbrokes') AND google_excl = 1)
                 GROUP BY 1,2,3,4,5
                 ),

         bb AS ( SELECT distinct aa.* REPLACE( CASE WHEN campaignid IS NULL AND aa.cid IS NOT NULL AND campaign_name IS NOT NULL
                                                    THEN tt.campaign_id ELSE CAST(campaignid AS STRING)
                                                    END AS campaignid)
                 FROM final aa
                 LEFT JOIN (SELECT distinct *
                            FROM {{ref('lc_dim_campaigns')}}) tt
                 ON CAST(aa.campaignid AS STRING) = tt.campaign_id
                 ),


 rr         AS ( SELECT a.* REPLACE(CASE WHEN a.campaignid IS NULL AND cpid IS NOT NULL THEN cpid ELSE a.campaignId END AS campaignId)
                 FROM bb a
                 LEFT JOIN (SELECT cid, MAX(campaignid) AS cpid
                            FROM bb
                            WHERE campaignid IS NOT NULL
                            group by 1) b
                 ON a.cid=b.cid
                ),

    aa      AS ( SELECT  Date                                                                                       AS Date
                       ,Brand                                                                                       AS Brand
                       ,CASE WHEN REGEXP_CONTAINS(campaign, 'competitor') THEN 'PPC - Competitor'
                             ELSE ChannelGrouping END                                                               AS ChannelGrouping
                       ,source                                                                                      AS Publisher
                       ,MAX(campaign)                                                                               AS Campaign
                       ,campaignid                                                                                  AS campaignid
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
                       ,MAX(cid)                                                                                    AS cid
                       ,''                                                                                          AS partner_name
                       ,SUM(IF(Conversion = 'Registration'  AND lag_hours <= 72, 1, 0))                             AS Registrations_72h
                       ,SUM(IF(Conversion = 'FTD'           AND lag_hours <= 72, 1, 0))                             AS FTDs_72h
                       ,ROUND(SUM(IF(Conversion = 'FTD'     AND lag_hours <= 72     , event_value, 0)),0)           AS FTDs_Value_72h
                       ,SUM(IF(Conversion = 'Deposit'       AND lag_hours <= 72, 1, 0))                             AS Deposits_72h
                       ,ROUND(SUM(IF(Conversion = 'Deposit' AND lag_hours <= 72     , event_value, 0)),0)           AS Deposits_Value_72h
                       ,SUM(IF(Conversion = 'Bet'           AND lag_hours <= 72, 1, 0))                             AS Bets_72h
                       ,ROUND(SUM(IF(Conversion = 'Bet'     AND lag_hours <= 72     , event_value, 0)),0)           AS Bets_Value_72h
                       ,ROUND(SUM(IF(lag_hours <= 72,pNGR_0,0)),2)                                                  AS Total_pNGR_0_72h
                 FROM rr
                 WHERE Date > '2020-01-19'
                 GROUP BY 1,2,3,4,6
                 )


SELECT  IFNULL(cc.Date,          f.Date)                                      AS Date
       ,IFNULL(cc.Brand,         f.Brand)                                     AS Brand
       ,IFNULL(cc.ChannelGrouping,  f.ChannelGrouping )                       AS ChannelGrouping
       ,IFNULL(cc.Publisher,        f.Publisher)                              AS Publisher
       ,IFNULL(cc.Campaign_id, f.Campaignid)                                  AS Campaignid
       ,IFNULL(cc.Campaign_n, f.Campaign)                                     AS Campaign
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
       ,partner_name
       ,0 AS crm_count
       ,Registrations_72h
       ,FTDs_72h
       ,ROUND(FTDs_Value_72h,0) AS FTDs_Value_72h
       ,Deposits_72h
       ,ROUND(Deposits_Value_72h,0) AS Deposits_Value_72h
       ,Bets_72h
       ,ROUND(Bets_Value_72h,0) AS Bets_Value_72h
       ,ROUND(Total_pNGR_0_72h,0) AS Total_pNGR_0_72h

FROM aa f
FULL OUTER JOIN (SELECT * FROM lal) cc
ON f.brand = cc.brand AND f.date = cc.date AND f.campaignid = cc.campaign_id
