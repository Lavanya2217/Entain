WITH final AS (SELECT * --
               FROM {{ref('pp_conversions_x_campaign')}} a
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
                       FROM {{ref('pp_campaigns_costs')}}
                       WHERE Date > '2020-01-20' AND join_type = 'cid') a

               GROUP BY 1,2,3,4,5
               ),

      update_market_info AS (
        -- As a prerequisite to joining the conversions to the costs, join to Campaigns_Costs solely to acquire and update the marketing info.
        -- This will avoid duplication in cases where there's multiple stes of marketing infor per CID
                SELECT
                		  f.Date                                           AS Date
                		, f.Brand                                          AS Brand
                		, IFNULL(cc.ChannelGrouping,  f.ChannelGrouping)   AS ChannelGrouping
                		, IFNULL(cc.Publisher, f.Publisher)                AS Publisher
                		, IFNULL(cc.Campaign_n, f.Campaign)                AS Campaign
                		, sum(Registrations					        ) as Registrations
                		, sum(FTDs              			      ) as FTDs
                		, sum(FTDs_value        			      ) as FTDs_value
                		, sum(Deposits          			      ) as Deposits
                		, sum(Deposits_value    			      ) as Deposits_value
                		, sum(Bets              			      ) as Bets
                		, sum(Bets_value        			      ) as Bets_value
                		, sum(Total_DCM_Conversions         ) as Total_DCM_Conversions
                		, sum(Total_AppsFlyer_Conversions   ) as Total_AppsFlyer_Conversions
                		, sum(Total_Web_Conversions         ) as Total_Web_Conversions
                		, sum(Total_App_Conversions         ) as Total_App_Conversions
                		, sum(Click_Conversions             ) as Click_Conversions
                		, sum(View_Conversions              ) as View_Conversions
                		, sum(Total_pNGR_0                  ) as Total_pNGR_0
                		, sum(Average_pNGR_0                ) as Average_pNGR_0
                		, sum(Total_pNGR_21					        ) as Total_pNGR_21
                		, partner_name
                		, f.cid                               AS cid
                		, sum(Registrations_72h				      ) as Registrations_72h
                		, sum(FTDs_72h              		    ) as FTDs_72h
                		, sum(FTDs_Value_72h        		    ) as FTDs_Value_72h
                		, sum(Deposits_72h          		    ) as Deposits_72h
                		, sum(Deposits_Value_72h    		    ) as Deposits_Value_72h
                		, sum(Bets_72h              		    ) as Bets_72h
                		, sum(Bets_Value_72h        		    ) as Bets_Value_72h
                		, sum(Total_pNGR_0_72h      		    ) as Total_pNGR_0_72h
                 FROM  final f
                 left JOIN lal cc
                 ON f.Brand = cc.Brand AND f.Date = cc.Date AND f.cid = cc.cid --AND f.Publisher = cc.Publisher
                 group by 1,2,3,4,5,partner_name,f.cid
                 )

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

      FROM  update_market_info f
      FULL OUTER JOIN lal cc
      ON f.Brand = cc.Brand AND f.Date = cc.Date AND f.cid = cc.cid