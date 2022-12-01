WITH final AS (SELECT *
               FROM {{ref('lc_conversions_x_campaign')}} a
               WHERE Date> '2019-12-31' AND a.cid IS NOT NULL AND SAFE_CAST(a.cid AS INT64) NOT IN (52840,00000)
                 AND google_excl IS NULL
                 AND NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)aff')
               ),

      lal AS ( SELECT Date, Brand
                      ,CASE WHEN index = 1 THEN 'Mixed' ELSE ChannelGrouping END AS ChannelGrouping
                      ,CASE WHEN index = 1 THEN 'Mixed' ELSE Publisher       END AS Publisher
                      ,cid
                      ,MAX(campaign_name) AS Campaign_n
                      ,ROUND(SUM(spend),2)         AS Spend
                      ,ROUND(SUM(clicks))          AS Clicks
                      ,ROUND(SUM(impressions))     AS Impressions
                      , index

               FROM(   SELECT * REPLACE(CAST(Date AS DATE) AS Date),
                              CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)cid') THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                   WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                                   WHEN (REGEXP_CONTAINS(ChannelGrouping,'artners') AND NOT REGEXP_CONTAINS(Brand, '(?i)gala')
                                    AND SAFE_CAST(campaign_id AS INT64)> 4995000)     THEN SAFE_CAST(campaign_id AS INT64)
                              END AS cid
                       FROM {{ref('lc_campaigns_costs')}}
                       WHERE Date > '2020-01-20' AND REGEXP_CONTAINS(Brand, '(?i)coral|ladbrokes')
                         AND google_excl IS NULL AND NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)aff')
                    ) a

               LEFT JOIN {{ref('lc_dim_duplicate_cids')}} d USING (Brand, cid)
               WHERE cid IS NOT NULL AND SAFE_CAST(cid AS INT64) NOT IN (52840,00000)
               GROUP BY 1,2,3,4,5,index
               )


SELECT *
FROM(
      SELECT  IFNULL(cc.Date,          f.Date)                                      AS Date
             ,IFNULL(cc.Brand,         f.Brand)                                     AS Brand
             ,CASE WHEN IFNULL(cc.Publisher, f.Publisher) IN ('AppNexus', 'DV360')
                    AND NOT REGEXP_CONTAINS(IFNULL(cc.Campaign_n, f.Campaign), '(?i)vod') THEN 'Display - Programmatic'
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
             ,0 AS crm_count
             ,CASE WHEN cc.ChannelGrouping = 'Display - Partners' AND NOT REGEXP_CONTAINS(campaign_n, 'cid|c:') THEN CONCAT(IFNULL(partner_name, LOWER(cc.publisher)), '|', campaign_n)
                   ELSE partner_name END AS partner_name
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
