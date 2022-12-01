WITH costs AS(      SELECT Brand 
                           ,CAST(Date AS DATE) AS Date
                           ,NULL AS Hour, '' AS Event_time     
                           , ChannelGrouping, '' AS medium, Publisher, NULL AS visitid
                           , campaign_id AS Campaign_name, '' AS CustomerID
                           , 'Cost' AS Conversion, NULL AS Lag_days ,NULL AS Lag_hours,NULL AS View_conversion ,NULL AS Click_conversion , '' AS Conv_medium , '' AS Dataset
                           ,campaign AS adContent ,NULL AS event_value ,'' AS transaction_id ,NULL AS FTD_date, NULL AS pNGR_0, '' AS keyword
                           ,campaign_id
                           ,Spend
                           ,CASE WHEN ChannelGrouping = 'Affiliate' THEN CAST(campaign_id AS INT64) ELSE NULL END AS GA_wm_track
                    FROM  {{ref('gvc_campaigns_costs')}}
                    WHERE CAST(Date AS DATE) BETWEEN '2021-01-01' AND Current_date() AND REGEXP_CONTAINS(Brand, '(?i)bwin|gala|cheeky|foxy') AND Spend >0),


     final AS(      SELECT * EXCEPT(medium) 
                           ,CASE WHEN ChannelGrouping = 'Display - Partners' AND adcontent IS NOT NULL AND NOT REGEXP_CONTAINS(adcontent, 'cid')
                                 THEN adcontent END AS partner_name
                           ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN 1 END AS namconv
                           ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN
                                 CASE WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                                      WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                           END AS Split
                    FROM( SELECT * EXCEPT(campaignid, wm_tracking, pngr_21, pngr_0_type, ftd_attributed_channel, event_value_GBP, currency, country, website )
                                   REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign, 'c:') THEN REGEXP_REPLACE(Campaign, '-', '|') ELSE campaign END AS campaign,
                                           CASE WHEN Brand = 'Bwin' THEN event_value_GBP ELSE event_value END AS event_value,
                                           CAST(event_time AS STRING) AS event_time) 
                                   ,0 AS Spend, wm_tracking AS GA_wm_track, campaign AS original_campaign
                          FROM {{ref('gvc_conversions_unique_ledger')}}
                          UNION ALL
                          SELECT * EXCEPT(Campaign_id)
                                   REPLACE(CASE WHEN REGEXP_CONTAINS(campaign_name, 'c:') THEN REGEXP_REPLACE(campaign_name, '-', '|') ELSE campaign_name END AS campaign_name)
                                   ,campaign_name AS original_campaign
                          FROM costs)
              ),
              
      excl AS (     SELECT distinct tracker_id AS tracker
                    FROM {{ref('affiliates_list')}}
                    WHERE tracker_id IS NOT NULL)


SELECT * EXCEPT(namconv, split) 
          REPLACE (CASE WHEN ChannelGrouping = 'Direct' AND campaign NOT LIKE '%(not%'           THEN 'Other'     ELSE  
                            REGEXP_REPLACE(ChannelGrouping, 'Dispaly', 'Display')  END AS ChannelGrouping,
                   CASE WHEN REGEXP_CONTAINS(ChannelGrouping, 'rral') 
                         AND (campaign = '(not set)' OR campaign = '(notset)')                   THEN source      ELSE Campaign        END AS Campaign,
                   CASE WHEN REGEXP_CONTAINS(Channelgrouping,'(?i)direct|eferral')               THEN NULL        ELSE source          END AS source)
                                
                  ,CASE WHEN Channelgrouping = 'Affiliate' THEN
                        CASE WHEN REGEXP_CONTAINS(source, '(?i)coral|ladb|direct') THEN 'Affiliate - No referral' ELSE Campaign END
                        WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Refer|Affi|Social - Org')                           THEN  source
                        WHEN (REGEXP_CONTAINS(ChannelGrouping, '(?i)crm|aff') AND NOT REGEXP_CONTAINS(Campaign, 'cid:|c:'))
                          OR (REGEXP_CONTAINS(ChannelGrouping, 'artner') AND NOT REGEXP_CONTAINS(Campaign, 'cid|c:')) 
                          OR Channelgrouping = 'Dispaly - Other'                                                       THEN Campaign
                        WHEN NOT REGEXP_CONTAINS(Campaign, 'cid:|c:') AND NOT REGEXP_CONTAINS(campaign, '(?i)crm') 
                         AND REGEXP_CONTAINS(Channelgrouping, '(?i)ppc|display|vod|social') AND NOT REGEXP_CONTAINS(Channelgrouping, '(?i)part') THEN 'naming_issue'
                        WHEN REGEXP_CONTAINS(Campaign, 'cid:|c:') THEN
                        CASE WHEN LENGTH(SPLIT(Campaign, '|')[SAFE_OFFSET(4)])=2 THEN SPLIT(Campaign, '|')[SAFE_OFFSET(1)] 
                        ELSE 'naming_issue' END                                                                                                     END AS Campaign_name
                        
                  ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, 'artner') AND NOT REGEXP_CONTAINS(Campaign, 'cid|c:') THEN
                        CASE WHEN SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) IS NOT NULL THEN SPLIT(campaign, '_')[SAFE_OFFSET(2)]
                             ELSE LOWER(LEFT(brand,3)) END
                        WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(2)]                                                END AS LCBrand
                  
                  ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, 'artner') AND NOT REGEXP_CONTAINS(Campaign, 'cid|c:') THEN 
                        IFNULL(CASE WHEN SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) IS NOT NULL THEN SPLIT(campaign, '_')[SAFE_OFFSET(3)] END,
                               CASE WHEN LOWER(campaign) LIKE '%cs_%' OR LOWER(campaign) LIKE '%ls_%' OR REGEXP_CONTAINS(campaign, '(?i)sport|horseracing' ) THEN 'sprts'
                                   WHEN LOWER(campaign) LIKE '%cc_%' OR LOWER(campaign) LIKE '%lc_%' OR REGEXP_CONTAINS(campaign, '(?i)casino')              THEN 'casi'
                                   WHEN LOWER(campaign) LIKE '%cb_%' OR LOWER(campaign) LIKE '%lb_%' OR REGEXP_CONTAINS(campaign, '(?i)bingo' )              THEN 'bngo'
                                   END)                        
                        WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(3)]                                                END AS SubBrand
                  ,CASE WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(4)]                                                END AS Geoinfo
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(5)]                                                END AS Day
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(6)]                                                END AS Month
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(7)]                                                END AS Year
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(8)]
                        WHEN namconv = 1 AND Split = 'Nsr' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(14)]                                               END AS Language
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(9)]                                                END AS Objective
                  ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(10)] ELSE 'search' END                  END AS Channel
                  ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(11)]
                                                                      ELSE SPLIT(Campaign, '|')[SAFE_OFFSET (9)]  END                               END AS Offer
                  ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(12)]   
                        WHEN Channelgrouping = 'Affiliate' AND Conversion = 'Cost' THEN adcontent                                                   END AS Type
                  ,CASE WHEN Channelgrouping = 'Dispaly - Other'                                 THEN Source
                        WHEN Channelgrouping = 'Display - Partners' AND partner_name IS NOT NULL THEN SPLIT(LOWER(partner_name), '|')[SAFE_OFFSET(0)]
                        WHEN REGEXP_CONTAINS(Campaign, '(?i)cid|c:')                             THEN
                        CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(13)]
                             WHEN Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET (5)] END                                            END AS Platform
                  ,CASE WHEN namconv = 1 AND Split  = 'Src' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(14)]
                        WHEN namconv = 1 AND Split  = 'Nsr' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(13)]                                              END AS Agency
                  ,CASE WHEN namconv = 1 AND Split  = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(15)]                                              END AS Targeting
                  ,CASE WHEN namconv = 1 AND Split  = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(16)]                                              END AS Strategy
                  ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(17)]
                                                   WHEN Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(10)] END                      END AS Medium
                  ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)cid') THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                        WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)      END AS Campaign_Id
                  ,CASE WHEN namconv = 1 AND REGEXP_CONTAINS(Campaign,'cid:')
                        THEN SPLIT(SPLIT(Campaign, '|')[SAFE_OFFSET(0)],'wm:')[SAFE_OFFSET(0)]                                                      END AS IA_tracking
                  ,CASE WHEN SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) IS NOT NULL THEN CAST(SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) AS STRING)
                        WHEN namconv = 1 THEN SPLIT(SPLIT(Campaign, '|')[SAFE_OFFSET(0)],'wm:')[SAFE_OFFSET(1)]                
                        WHEN Channelgrouping = 'Affiliate' AND SAFE_CAST(GA_wm_track AS INT64) >0  THEN CAST(GA_wm_track AS STRING)                 END AS WM_tracking
                  ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(6)]                                      END AS Buy_Type
                  ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(7)]                                      END AS KW_Type
                  ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(8)]                                      END AS KW_Theme
                  ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(11)]                                     END AS Event
                  ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(12)]                                     END AS Game
         
FROM final

