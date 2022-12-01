WITH final AS (SELECT *
               FROM {{ref('gvc_conversions_x_campaign')}} a
               WHERE Date> '2020-12-31' AND a.cid IS NOT NULL
                 AND google_excl IS NULL
               ),

      lal AS ( SELECT Date, Brand
                      ,ChannelGrouping
                      ,Publisher
                      ,cid
                      ,MAX(campaign_name) AS Campaign_n
                      ,ROUND(SUM(spend),2)         AS Spend
                      ,ROUND(SUM(clicks))          AS Clicks
                      ,ROUND(SUM(impressions))     AS Impressions


               FROM(   SELECT * REPLACE(CAST(Date AS DATE) AS Date)
                       FROM {{ref('gvc_campaigns_costs')}}
                       WHERE Date > '2020-01-20' AND join_type = 'cid') a

               GROUP BY 1,2,3,4,5
               )


SELECT *
FROM(
      SELECT  IFNULL(cc.Date,          f.Date)                                      AS Date
             ,IFNULL(cc.Brand,         f.Brand)                                     AS Brand
             ,CASE WHEN IFNULL(cc.Publisher, f.Publisher) IN ('AppNexus', 'DV360')
                    AND (NOT REGEXP_CONTAINS(IFNULL(cc.Campaign_n, f.Campaign), '(?i)vod') OR f.campaign IS NULL) THEN 'Display - Programmatic'
                   ELSE IFNULL(cc.ChannelGrouping,  f.ChannelGrouping) END          AS ChannelGrouping
             ,IFNULL(cc.Publisher, f.Publisher)                                     AS Publisher
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
             ,Total_pNGR_21
             ,0 AS crm_count
             ,partner_name
             ,IFNULL(cc.cid, f.cid) AS cid
             ,Registrations_72h
             ,FTDs_72h
             ,ROUND(FTDs_Value_72h,0) AS FTDs_Value_72h
             ,Deposits_72h
             ,ROUND(Deposits_Value_72h,0) AS Deposits_Value_72h
             ,Bets_72h
             ,ROUND(Bets_Value_72h,0) AS Bets_Value_72h
             ,ROUND(Total_pNGR_0_72h,0) AS Total_pNGR_0_72h

      FROM  final f
      FULL OUTER JOIN lal cc
      ON f.Brand = cc.Brand AND f.Date = cc.Date AND f.cid = cc.cid --AND f.Publisher = cc.Publisher
      )





----Destination: CostsConv_CIDs
