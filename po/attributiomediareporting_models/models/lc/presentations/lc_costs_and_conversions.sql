
WITH ru AS (SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign, 'c:') THEN REGEXP_REPLACE(Campaign, '-', '|') ELSE Campaign END AS Campaign,
                             CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)clickwor') OR REGEXP_CONTAINS(publisher, '(?i)clickwor') OR REGEXP_CONTAINS(partner_name, '(?i)clickwor') THEN 'Clickwork' ELSE Publisher END AS Publisher,
                             CASE WHEN ChannelGrouping = 'Display - Partners' THEN
                                  CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)wakeapp')  THEN 'wakeapp'
                                       WHEN REGEXP_CONTAINS(Campaign, '(?i)clickwor') OR REGEXP_CONTAINS(publisher, '(?i)clickwor') OR REGEXP_CONTAINS(partner_name, '(?i)clickwor') THEN 'clickwork'
                                       WHEN partner_name = '(not set)' THEN ''
                                       WHEN partner_name IS NOT NULL                 THEN partner_name
                                       ELSE publisher END
                                  END AS partner_name)
                     ,Campaign AS original_campaign
            FROM {{ref('lc_costsconv_total')}} WHERE DATE < CURRENT_DATE())


, fu AS(
SELECT * EXCEPT(Split, partner_name, cid)
         REPLACE (CASE WHEN ChannelGrouping = 'Direct' AND campaign NOT LIKE '%(not%'           THEN 'Other'     ELSE
                            REGEXP_REPLACE(ChannelGrouping, 'Dispaly', 'Display')
                  END AS ChannelGrouping,
                  CASE WHEN Channelgrouping = 'Affiliate' THEN ''
                       WHEN REGEXP_CONTAINS(ChannelGrouping, 'rral')
                        AND (campaign = '(not set)' OR campaign = '(notset)')                   THEN Publisher   ELSE Campaign        END AS Campaign,
                  CASE WHEN REGEXP_CONTAINS(Channelgrouping,'(?i)direct|eferral')               THEN NULL        ELSE Publisher       END AS Publisher)

         ,CASE WHEN Channelgrouping = 'Affiliate' AND REGEXP_CONTAINS(Publisher, '(?i)coral|ladb|direct')     THEN 'Affiliate - No referral'
               WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Refer|Affi|Social - Org')                           THEN  Publisher
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
               CASE WHEN LOWER(campaign) LIKE '%cs_%' OR LOWER(campaign) LIKE '%ls_%' OR REGEXP_CONTAINS(campaign, '(?i)sport|horseracing|sprts' ) THEN 'sprts'
                    WHEN LOWER(campaign) LIKE '%cc_%' OR LOWER(campaign) LIKE '%lc_%' OR REGEXP_CONTAINS(campaign, '(?i)casino|casi')              THEN 'casi'
                    WHEN LOWER(campaign) LIKE '%cb_%' OR LOWER(campaign) LIKE '%lb_%' OR REGEXP_CONTAINS(campaign, '(?i)bingo|bngo' )              THEN 'bngo'
                    END
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
         ,CASE WHEN Channelgrouping = 'Affiliate' THEN campaign
               WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(12)]                                               END AS Type
         ,CASE WHEN Channelgrouping = 'Dispaly - Other'                                 THEN Publisher
               WHEN REGEXP_CONTAINS(Campaign, '(?i)cid|c:')                             THEN
               CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(13)]
                    WHEN Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET (5)] END
               WHEN Channelgrouping = 'Display - Partners' AND partner_name IS NOT NULL THEN SPLIT(LOWER(partner_name), '|')[SAFE_OFFSET(0)] END AS Platform
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Reg') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(14)]
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

FROM(
      SELECT * --REPLACE(CASE WHEN (NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)crm|ref|orga|affi|ispaly|partner|ppc|disp|soc') OR ChannelGrouping = 'Other') AND Spend IS NULL
               --             THEN 'Unassigned' ELSE campaign END AS campaign)
                            ,campaign AS cpxd

      FROM(
              SELECT distinct ru.* REPLACE(CASE WHEN sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN 1 END AS namconv
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN
                      CASE WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                           WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                           END AS Split
              FROM ru
              LEFT JOIN {{ref('lc_sessions_count')}} s
              ON s.Brand = ru.Brand AND s.cp = ru.campaign AND s.Date = ru.Date
              WHERE (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL) AND NOT REGEXP_CONTAINS(ChannelGrouping, 'Referral')

        UNION ALL
              SELECT distinct ru.* REPLACE(CASE WHEN sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN 1 END AS namconv
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN
                      CASE WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                           WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                           END AS Split
              FROM ru
              LEFT JOIN {{ref('lc_sessions_count')}} s
              ON s.Brand = ru.Brand AND s.cp = ru.ChannelGrouping AND s.Date = ru.Date AND s.source = ru.Publisher
              WHERE REGEXP_CONTAINS(campaign, 'not set|notset') AND NOT REGEXP_CONTAINS(ChannelGrouping, 'Referral')

        UNION ALL
              SELECT distinct ru.* REPLACE(CASE WHEN sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN 1 END AS namconv
                     ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid:|c:') THEN
                      CASE WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                           WHEN REGEXP_CONTAINS(SPLIT(Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                           END AS Split
              FROM ru
              LEFT JOIN {{ref('lc_sessions_count')}} s
              ON s.Brand = ru.Brand AND s.cp = ru.ChannelGrouping AND s.Date = ru.Date AND s.source = ru.Publisher
              WHERE REGEXP_CONTAINS(ChannelGrouping, 'Referral')
        ))

ORDER BY 1,2,3,4,5)


SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(publisher, '(?i)partner|3dot14|fluent|Unknown|Unityads') AND platform <> '' THEN INITCAP(platform) ELSE publisher END AS Publisher)
FROM Fu
UNION ALL
SELECT CAST(CONCAT(MONTH, "-01") AS DATE), Brand, 'Finance Actuals' AS Channelgrouping, sub_channel AS Publisher, CONCAT('Actual ', kpi_type) AS campaign
       ,NULL, spend, NULL,NULL
       ,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL as GA_wm_track
       ,'','',NULL,''
       ,Channel AS campaign_name
       ,Brand AS LCGBrand
       ,product AS SubBrand
       ,'','','','','','','',''
       ,Measure_Type AS Type
       ,sub_channel AS platform
       ,team AS Agency
       ,'','','',NULL
       ,'','','','','','',''

FROM `lcg-fivetran-dev.LCFinance_Table.LCFinance_table`
WHERE spend>0
