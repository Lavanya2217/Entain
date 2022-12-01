WITH ru AS (SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign, 'c:') THEN REGEXP_REPLACE(Campaign, '-', '|') ELSE Campaign END AS Campaign,
                             CASE WHEN ChannelGrouping = 'Direct' THEN '(Direct)' ELSE Publisher END AS Publisher,
                             CASE WHEN ChannelGrouping = 'Display - Partners' THEN
                                  CASE WHEN partner_name IS NOT NULL                 THEN partner_name
                                       ELSE publisher END
                                  END AS partner_name)
                     ,Campaign AS original_campaign
            FROM {{ref('pp_costsconv_total')}} WHERE DATE < CURRENT_DATE()),


   vu AS   (SELECT * ,campaign AS cpxd
            FROM(
                    SELECT distinct ru.* REPLACE(CASE WHEN (visits IS NULL OR visits = 0) AND sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN 1 END AS namconv
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN
                            CASE WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                                 WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                                  END AS Split
                    FROM ru
                    LEFT JOIN (SELECT distinct * FROM {{ref('pp_sessions_count')}}) s
                    ON s.Brand = ru.Brand AND LOWER(s.cp) = LOWER(ru.campaign) AND s.Date = ru.Date
                    WHERE (NOT REGEXP_CONTAINS(ru.campaign, 'not set|notset') OR ru.campaign IS NULL) AND NOT REGEXP_CONTAINS(ChannelGrouping, 'Organic|Referral|Direct|CRM')

              UNION ALL

                    SELECT distinct ru.* REPLACE(CASE WHEN (visits IS NULL OR visits = 0) AND sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN 1 END AS namconv
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN
                            CASE WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                                 WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                                  END AS Split
                    FROM ru
                    LEFT JOIN (SELECT distinct * FROM {{ref('pp_sessions_count')}}) s
                    ON s.Brand = ru.Brand AND s.Date = ru.Date AND LOWER(s.campaign) = LOWER(ru.campaign) AND Channelgrouping = s.cp
                    WHERE REGEXP_CONTAINS(ChannelGrouping, '(?i)CRM')

              UNION ALL
                    SELECT distinct ru.* REPLACE(CASE WHEN (visits IS NULL OR visits = 0) AND sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN 1 END AS namconv
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN
                            CASE WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                                 WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                                  END AS Split
                    FROM ru
                    LEFT JOIN (SELECT distinct * FROM {{ref('pp_sessions_count')}}) s
                    ON s.Brand = ru.Brand AND s.cp = ru.ChannelGrouping AND s.Date = ru.Date
                    and regexp_replace(INITCAP(lower(s.source)),r'_int| |_|-|\.','') = regexp_replace(INITCAP(lower(ru.Publisher)),r'_int| |_|-|\.','')
                    WHERE REGEXP_CONTAINS(ru.Campaign, 'not set|notset')
                      AND NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)Organic|Referral|Direct|CRM')

              UNION ALL
                    SELECT distinct ru.* REPLACE(CASE WHEN (visits IS NULL OR visits = 0) AND sessions_count IS NOT NULL THEN sessions_count ELSE visits END AS Visits)
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN 1 END AS namconv
                           ,CASE WHEN REGEXP_CONTAINS(ru.Campaign, 'cid:|c:') THEN
                            CASE WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(13)], 'cid') THEN 'Src'
                                 WHEN REGEXP_CONTAINS(SPLIT(ru.Campaign, '|')[SAFE_OFFSET(15)], 'c:')  THEN 'Nsr' ELSE 'Reg' END
                                  END AS Split
                    FROM ru
                    LEFT JOIN (SELECT distinct * FROM {{ref('pp_sessions_count')}}) s
                    ON s.Brand = ru.Brand AND s.cp = ru.ChannelGrouping AND s.Date = ru.Date AND s.country = ru.country
                    and regexp_replace(INITCAP(lower(s.source)),r'_int| |_|-|\.','') = regexp_replace(INITCAP(lower(ru.Publisher)),r'_int| |_|-|\.','')
                    WHERE REGEXP_CONTAINS(ChannelGrouping, '(?i)Organic|Referral|Direct')
              )),


   tu AS   (SELECT a.* REPLACE(b.string_field_2 AS crm_country
                              ,c.string_field_2 AS crm_language
                              ,CASE WHEN crm_product = 'c' THEN 'casi'
                                    WHEN crm_product = 's' THEN 'sprts'
                                    WHEN crm_product = 'b' THEN 'bngo' END AS crm_product)
            FROM( SELECT *, CASE WHEN crm_conv = 1 THEN SUBSTR(SPLIT(REGEXP_REPLACE(campaign, '/', ''), '-')[SAFE_OFFSET(0)],-5,2) END AS crm_country
                          , CASE WHEN crm_conv = 1 THEN SUBSTR(SPLIT(REGEXP_REPLACE(campaign, '/', ''), '-')[SAFE_OFFSET(0)],-3,1) END AS crm_language
                          , CASE WHEN crm_conv = 1 THEN  RIGHT(SPLIT(REGEXP_REPLACE(campaign, '/', ''), '-')[SAFE_OFFSET(0)], 1)   END AS crm_product

                  FROM( SELECT *
                                -- is this neede for Party?
                                , CASE WHEN /* REGEXP_CONTAINS(Brand, '(?i)Bwin') AND */ REGEXP_CONTAINS(ChannelGrouping, '(?i)crm') AND NOT REGEXP_CONTAINS(Campaign, 'c:')
                                        AND LENGTH(REGEXP_REPLACE(campaign, '/', '')) - LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(campaign, '/', ''), '-', '')) = 2
                                       THEN 1 END AS crm_conv
                        FROM vu)
                 ) a
            LEFT JOIN (SELECT * FROM {{ source('files', 'crm_decoder') }} WHERE string_field_0 = 'country') b  ON a.crm_country  = b.string_field_1
            LEFT JOIN (SELECT * FROM {{ source('files', 'crm_decoder') }} WHERE string_field_0 = 'lang'   ) c  ON a.crm_language = c.string_field_1
            )


SELECT * EXCEPT(Split, partner_name, cid, country, crm_country, crm_language, crm_product)
         REPLACE (CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)crm') AND NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)social') AND impressions IS NOT NULL THEN Visits  ELSE clicks END AS Clicks,
                  CASE WHEN ChannelGrouping = 'Direct' AND campaign NOT LIKE '%(not%'           THEN 'Other'     ELSE
                            REGEXP_REPLACE(ChannelGrouping, 'Dispaly', 'Display')                                                     END AS ChannelGrouping,
                  CASE WHEN Channelgrouping = 'Affiliate'                                       THEN ''
                       WHEN REGEXP_CONTAINS(ChannelGrouping, 'rral')
                        AND (campaign = '(not set)' OR campaign = '(notset)')                   THEN Publisher   ELSE Campaign        END AS Campaign,
                  CASE
                    WHEN REGEXP_CONTAINS(Channelgrouping,'(?i)direct|eferral')
                        and not REGEXP_CONTAINS(Channelgrouping,'(?i)Display - Direct')  THEN NULL
                    when REGEXP_CONTAINS(Channelgrouping,'(?i)ppc') and REGEXP_CONTAINS(Publisher,'(?i)google') then 'Google_Ads'
                    ELSE initcap(lower(Publisher))    END AS Publisher)

         ,CASE WHEN Channelgrouping = 'Affiliate'                                                         THEN ''
               WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Refer|Affi|Social - Org')                       THEN Publisher
               WHEN (REGEXP_CONTAINS(ChannelGrouping, '(?i)crm|aff|ppc|display|vod|social') AND NOT REGEXP_CONTAINS(Campaign, 'cid:|c:'))
                 OR Channelgrouping = 'Display - Other'                                                   THEN Campaign
             -- WHEN NOT REGEXP_CONTAINS(Campaign, 'c:') AND NOT REGEXP_CONTAINS(campaign, '(?i)crm')
             --  AND REGEXP_CONTAINS(Channelgrouping, '(?i)ppc|display|vod|social')                        THEN 'naming_issue'
               WHEN REGEXP_CONTAINS(Campaign, 'c:') THEN
               CASE WHEN LENGTH(SPLIT(Campaign, '|')[SAFE_OFFSET(4)])=2 THEN SPLIT(Campaign, '|')[SAFE_OFFSET(1)]
               ELSE 'naming_issue' END                                                                                                     END AS Campaign_name
         ,CASE WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(2)]                                                END AS LCBrand
         ,CASE WHEN crm_product IS NOT NULL       THEN crm_product
               WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(3)]                                                END AS SubBrand
         ,CASE WHEN namconv = 1                   THEN SPLIT(Campaign, '|')[SAFE_OFFSET(4)]
               WHEN country     IS NOT NULL THEN country
               WHEN crm_country IS NOT NULL THEN crm_country                                                                               END AS Geoinfo
         ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(5)]                                                END AS Day
         ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(6)]                                                END AS Month
         ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(7)]                                                END AS Year
         ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(8)]
               WHEN namconv = 1 AND Split = 'Nsr' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(14)]
               WHEN crm_language IS NOT NULL THEN crm_language                                                                             END AS Language
         ,CASE WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(9)]                                                END AS Objective
         ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(10)] ELSE 'search' END                  END AS Channel
         ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(11)]
                                                             ELSE SPLIT(Campaign, '|')[SAFE_OFFSET (9)]  END                               END AS Offer
         ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Affi')  THEN Campaign
          -- removed as bwin condition - do we need this?
               --WHEN Brand = 'Bwin' AND Channelgrouping = 'Display - Partners'           THEN
                  --CASE WHEN (REGEXP_CONTAINS(publisher, '(?i)odds') OR REGEXP_CONTAINS(campaign, '(?i)odds')) THEN 'partnerodds' ELSE 'partner' END
               WHEN namconv = 1 AND Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(12)]                                               END AS Type
         ,CASE WHEN Channelgrouping = 'Dispaly - Other'                                 THEN Publisher
               WHEN Channelgrouping = 'Display - Partners' AND partner_name IS NOT NULL THEN partner_name
               WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')                                 THEN
               CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(13)]
                    WHEN Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET (5)] END                                            END AS Platform
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Reg') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(14)]
               WHEN namconv = 1 AND Split  = 'Nsr' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(13)]                                              END AS Agency
         ,CASE WHEN namconv = 1 AND Split  = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(15)]                                              END AS Targeting
         ,CASE WHEN namconv = 1 AND Split  = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(16)]                                              END AS Strategy
         ,CASE WHEN namconv = 1 THEN CASE WHEN Split = 'Reg' THEN SPLIT(Campaign, '|')[SAFE_OFFSET(17)]
                                          WHEN Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(10)] END                      END AS Medium
         ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
               WHEN cid IS NOT NULL THEN cid                                                                                               END AS Campaign_Id
         ,CASE WHEN namconv = 1 AND REGEXP_CONTAINS(Campaign,'cid:')
               THEN SPLIT(SPLIT(Campaign, '|')[SAFE_OFFSET(0)],'wm:')[SAFE_OFFSET(0)]                                                      END AS IA_tracking
         ,CASE WHEN namconv = 1 THEN
               CASE WHEN SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) IS NOT NULL THEN CAST(SAFE_CAST(SUBSTR(Campaign,1,7) AS INT64) AS STRING)
                    ELSE SPLIT(SPLIT(Campaign, '|')[SAFE_OFFSET(0)],'wm:')[SAFE_OFFSET(1)] END
               WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Partner|aff')
                AND SAFE_CAST(GA_wm_track AS INT64) >0  THEN CAST(GA_wm_track AS STRING)                                                   END AS WM_tracking
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(6)]                                      END AS Buy_Type
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(7)]                                      END AS KW_Type
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(8)]                                      END AS KW_Theme
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(11)]                                     END AS Event
         ,CASE WHEN namconv = 1 AND Split IN ('Src', 'Nsr') THEN SPLIT(Campaign, '|')[SAFE_OFFSET(12)]                                     END AS Game

FROM tu
ORDER BY 1,2,3,4,5
