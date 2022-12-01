WITH ga        AS (SELECT *, CASE WHEN Conversion = 'Bet' THEN 1 ELSE ROW_NUMBER() OVER ( PARTITION BY Customer, Conversion, Date, Hour, Minute ORDER BY event_value DESC) END AS rank
                   FROM(
                          SELECT * EXCEPT(fullVisitorID, device, landing, CD108, keyword, event_value, adContent, campaignId,wm_tracking, transaction_id)
                                   REPLACE(CAST(Event_Time AS STRING) AS Event_Time
                                          ,CASE WHEN REGEXP_CONTAINS(source, 'twitter|t.co$' )  THEN 'Twitter'
                                                WHEN NOT REGEXP_CONTAINS(source, '(?i)crm|referral')
                                                 AND REGEXP_CONTAINS(source, '(?i)facebook')    THEN 'Facebook'
                                                WHEN REGEXP_CONTAINS(source, 'youtube' )        THEN 'Youtube'
                                                WHEN REGEXP_CONTAINS(source, 'insta'   )        THEN 'Instagram'
                                                WHEN REGEXP_CONTAINS(source, 'snap'    )        THEN 'Snapchat'
                                                WHEN REGEXP_CONTAINS(ChannelGrouping, 'PPC') THEN
                                                     CASE WHEN REGEXP_CONTAINS(source, 'google|search$|_search') AND NOT REGEXP_CONTAINS(source, '(?i)microsoft')
                                                     THEN 'Google_Ads' ELSE source END
                                                ELSE source END AS source,

                                           CASE WHEN REGEXP_CONTAINS(campaign, 'not set|notset') AND REGEXP_CONTAINS(source, 'utm_campaign')
                                           THEN SPLIT(source, 'utm_campaign ')[SAFE_OFFSET(1)] ELSE campaign END AS campaign
                                           )
                                   ,CASE WHEN SUBSTR(adcontent,6,1)= '.' AND SAFE_CAST(SUBSTR(adcontent,1,5) AS INT64) IS NOT NULL THEN CONCAT('c:', adcontent) ELSE adcontent END AS adContent
                                   ,keyword AS keyword
                                   ,event_value
                                   ,CASE WHEN NOT REGEXP_CONTAINS(transaction_id, 'unde') THEN REGEXP_REPLACE(REGEXP_REPLACE(transaction_id, '%2F', '/'), '%2C', ',') END AS transaction_id
                                   ,CASE WHEN REGEXP_CONTAINS(medium, 'wm') THEN SAFE_CAST(SPLIT(medium, 'wm:')[SAFE_OFFSET(1)] AS INT64)
                                         WHEN REGEXP_CONTAINS(landing, '=12345&') THEN 12345 ELSE wm_tracking END AS wm_tracking
                                   ,CASE WHEN SAFE_CAST(campaign AS INT64) IS NOT NULL AND campaignid IS NULL AND REGEXP_CONTAINS(source, '(?i)microsoft|oogle|bing|search')
                                         THEN SAFE_CAST(campaign AS INT64)
                                         ELSE campaignid END AS campaignid
                                   ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute

                          FROM (SELECT distinct * FROM {{ref('lc_conversions_analytics_final')}}
                                WHERE Date >= '2020-01-20' AND REGEXP_CONTAINS(Brand, '(?i)Coral|Ladbrokes')) )
                   ),

      excl     AS (SELECT STRING_AGG(excl, '|') FROM {{ source('exclusions_lists_lc', 'exclusion_list') }} ),
      trac     AS (SELECT distinct trackerid AS tracker
                   FROM {{ source('exclusions_lists_lc', 'exclusion_list_Affiliates_final') }} 
                   WHERE trackerid IS NOT NULL),
      ptnr     AS (SELECT distinct Tracker_id AS tracker
                   FROM {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }} ),
      ptnr2    AS (SELECT STRING_AGG(LOWER(partner), '|') FROM (SELECT distinct partner FROM {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }} )),


      uni      AS (SELECT d.* REPLACE(CASE WHEN IMS_customer_id IS NOT NULL THEN CAST(tt.Customer_ID AS STRING) ELSE CustomerID END AS CustomerID,
                                      CAST(FLOOR(Lag_hours/24) AS INT64) AS Lag_days)
                   FROM(
                        SELECT distinct * EXCEPT(ranknew) FROM {{ref('lc_conversions_dcm')}}
                        WHERE REGEXP_CONTAINS(Brand, '(?i)Coral|Ladbrokes') AND ranknew = 1
                        UNION ALL
                        SELECT distinct * REPLACE(CAST(Event_Time AS STRING) AS Event_Time) FROM {{ref('lc_conversions_appsflyer')}}
                        UNION ALL
                        SELECT * EXCEPT(rank) FROM ga WHERE rank = 1
                        UNION ALL
                          SELECT distinct * EXCEPT(visitStartTime,visitEndTime,country,currency,website,match_ind,gclid)
                                            REPLACE(CAST(Event_Time AS STRING) AS Event_Time)
                          FROM  {{ref('missing_deposits_final')}}
                          WHERE REGEXP_CONTAINS(brand, '(?i)ladbrokes|coral')
                      ) d

                   LEFT JOIN (SELECT distinct * FROM {{ source('other_lists_lc', 'IMS_CustomerID') }} ) tt
                   ON d.CustomerID = IMS_customer_id AND tt.Brand = d.Brand
                   WHERE CUstomerID IS NOT NULL
                   ),



     pngrjoin  AS (SELECT s.* REPLACE(CASE WHEN REGEXP_CONTAINS(source, '(?i)Apple') THEN REGEXP_REPLACE(campaign, 'c_wm', 'c-wm') ELSE campaign END AS campaign),
                          IFNULL(p.FTD_DATE_ID, t.FTD_DATE_ID) AS FTD_Date,
                          CASE WHEN value_0     IS NOT NULL AND ABS(value_0)>0 THEN value_0 ELSE Avg_value_0 END AS value_0,
                          CASE WHEN value_0     IS NOT NULL AND ABS(value_0)>0 THEN 'actual'
                               WHEN Avg_value_0 IS NOT NULL THEN 'avg'   END AS pngr0_type,
                          Value_20
                   FROM uni s
                   LEFT JOIN (SELECT distinct * FROM {{ref('lc_td_pngr_final')}}) p
                   ON s.CustomerID = CAST(p.Customer_ID AS STRING) AND s.Date = FTD_DATE_ID AND p.Brand = s.Brand
                   LEFT JOIN (SELECT distinct * FROM {{ref('lc_td_ftds')}}) t
                   ON SAFE_CAST(s.CustomerID AS INT64)= SAFE_CAST(t.src_account_ID AS INT64) AND s.Date = t.FTD_DATE_ID AND t.Brand = s.Brand

                    ),

       rayo    AS (SELECT *, NULL AS campaign_name
                   FROM pngrjoin
                   WHERE SAFE_CAST(campaign AS INT64) IS NULL
                      OR (SAFE_CAST(campaign AS INT64) IS NOT NULL AND (source IS NULL OR NOT REGEXP_CONTAINS(source, '(?i)microsoft|oogle|bing|search')))
                   UNION ALL
                   SELECT * EXCEPT (Brand2, Date2, Publisher, campaign_id, End_Date, cid, newest_rank, gap, rank2)
                            REPLACE(CASE WHEN rank2 = 1 AND campaign_name IS NOT NULL THEN campaign_name ELSE campaign END AS campaign)
                   FROM( SELECT *, ROW_NUMBER() OVER( PARTITION BY Brand, date, event_time, conversion, customerid, channelgrouping, campaign, lag_hours, transaction_id, CAST(event_value AS STRING)
                                                      ORDER BY gap ASC, end_date ASC) rank2
                         FROM(
                              SELECT *, CASE WHEN date BETWEEN date2 AND end_date THEN 0
                                             WHEN date > date2 THEN DATE_DIFF(date, end_date, DAY)
                                             WHEN date < date2 THEN 1000
                                         END AS gap
                              FROM (SELECT * FROM pngrjoin where SAFE_CAST(campaign AS INT64) IS NOT NULL AND REGEXP_CONTAINS(source, '(?i)microsoft|oogle|bing|search'))
                              LEFT JOIN ( SELECT distinct * EXCEPT(ChannelGrouping, Brand, date), Brand as Brand2, date as date2
                                          FROM {{ref('lc_dim_campaigns')}}
                                          WHERE CAST(Date AS Date) IS NOT NULL AND REGEXP_CONTAINS(publisher, '(?i)microsoft|oogle|bing|search'))
                              ON campaign = campaign_id
                              )
                        ) WHERE rank2 = 1
                   ),

       final AS (SELECT d.* EXCEPT (FTD_Date, pngr0_type, Value_0, Value_20, keyword)
                            REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign,'(?i)xus')                                     THEN 'AppNexus'
                                         WHEN REGEXP_CONTAINS(Campaign,'(?i)Dv360')                                   THEN 'DV360'
                                         WHEN REGEXP_CONTAINS(Campaign,'(?i)uac')                                     THEN 'Google_UAC'
                                         ELSE source END AS source,
                            CASE WHEN REGEXP_CONTAINS(source, '(?i)Apple') AND REGEXP_CONTAINS(campaign, 'cid|c:') THEN
                            CASE WHEN REGEXP_CONTAINS(campaign, 'c-wm:')
                                 THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign,'@@@')[SAFE_OFFSET(0)],"@@@",SPLIT(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],"c-wm:")[SAFE_OFFSET(0)],
                                      SUBSTR(campaign, STRPOS(campaign, "c-wm:"),12),REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],
                                      STRPOS(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],"c-wm:")+12,100)),"_","|")), "  ", ""), " ", "")
                                 WHEN REGEXP_CONTAINS(campaign, '_c__')
                                 THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign,'@@@')[SAFE_OFFSET(0)],"@@@",SPLIT(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],'c__')[SAFE_OFFSET(0)],"c_|",
                                      REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],STRPOS(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],"c__")+3,100)),"_","|")), "  ", ""), " ", "")
                                 WHEN regexp_contains(campaign, '@@@z@@@') AND NOT REGEXP_CONTAINS(campaign, 'wm:')
                                 THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign,'@@@')[SAFE_OFFSET(0)],"@@@",SPLIT(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],'c_')[SAFE_OFFSET(0)],
                                      REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign,'@@@')[SAFE_OFFSET(2)],STRPOS(SPLIT(campaign,'@@@')[SAFE_OFFSET(2)],"c_")+1,100)),"_","|")), "  ", ""), " ", "")
                                 WHEN REGEXP_CONTAINS(campaign, 'wm:') THEN
                                 CASE WHEN SPLIT(campaign,'@@@')[SAFE_OFFSET(2)] IS NOT NULL
                                      THEN REGEXP_REPLACE(REGEXP_REPLACE(CONCAT( SPLIT(campaign,'@@@')[SAFE_OFFSET(0)],"@@@",SPLIT(SPLIT(campaign,'@@@')[SAFE_OFFSET(1)],'c_|-')[SAFE_OFFSET(0)],
                                           REGEXP_REPLACE(TRIM(SUBSTR(SPLIT(campaign,'@@@')[SAFE_OFFSET(2)],STRPOS(SPLIT(campaign,'@@@')[SAFE_OFFSET(2)],"wm:"),100)),"_","|")
                                            ), "  ", ""), " ", "")
                                      ELSE REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(campaign,'wm:')[SAFE_OFFSET(0)],'wm:',
                                           REGEXP_REPLACE(TRIM(SPLIT(campaign,'wm:')[SAFE_OFFSET(1)]),"_","|")), "  ", ""), " ", "")
                                 END
                                 ELSE REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(SPLIT(campaign,'b_c_')[SAFE_OFFSET(0)],'b_c|',
                                      REGEXP_REPLACE(TRIM(SPLIT(campaign,'b_c_')[SAFE_OFFSET(1)]),"_","|")), "  ", ""), " ", "")
                            END
                        ELSE campaign
                        END AS campaign)


                          ,CASE WHEN FTD_DATE IS NOT NULL THEN 1 ELSE 0                                                         END AS FTD_date
                          ,CASE WHEN Value_0  IS NOT NULL THEN pngr0_type                                                       END AS pNGR_0_type
                          ,CASE WHEN Value_0  IS NOT NULL THEN ROUND(Value_0 ,2)                                                END AS pNGR_0
                          ,CASE WHEN Value_20 IS NOT NULL THEN ROUND(Value_20,2)                                                END AS pNGR_21
                          ,CASE WHEN keyword = '' OR REGEXP_CONTAINS(keyword,'not set|provided') THEN NULL ELSE keyword         END AS keyword
                          ,CASE WHEN REGEXP_CONTAINS(campaign, '(?i)dfa') THEN SPLIT(campaign, ':')[SAFE_OFFSET(2)]             END AS cpid
                          ,CASE WHEN wm_tracking IS NULL OR wm_tracking NOT IN(SELECT distinct tracker FROM trac) THEN 0 ELSE 1 END AS tracker_excl

                 FROM rayo d
                 ),


          rau AS( SELECT * REPLACE(CASE WHEN (NOT REGEXP_CONTAINS(campaign, 'cid|c:') OR Campaign IS NULL) AND REGEXP_CONTAINS(keyword, 'cid|c:') THEN keyword
                                        WHEN REGEXP_CONTAINS(source, '(?i)bing|oogl') AND REGEXP_CONTAINS(campaign, ' - ') AND REGEXP_CONTAINS(campaign, 'cid')
                                   THEN REGEXP_REPLACE(Campaign, ' - ', ' _ ') ELSE Campaign
                                   END AS Campaign)
                  FROM( SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)dv360|appnex')
                                               AND REGEXP_CONTAINS(Campaign, '(?i)ID_')                           THEN SPLIT(Campaign, 'ID_')[SAFE_OFFSET(0)]
                                              WHEN camp          IS NOT NULL THEN camp
                                              ELSE campaign
                                         END AS campaign)
                  FROM final
                  LEFT JOIN (SELECT distinct Campaign_ID , Campaign AS camp FROM {{ source('DCM_UK', 'p_match_table_campaigns_785192') }} ) s
                  ON s.Campaign_ID = cpid)
                  ),


        camp2 AS (SELECT *, ROW_NUMBER() OVER( PARTITION BY Brand, cid, cp2 ORDER BY end_date DESC) AS Rank
                  FROM( SELECT Brand, cid, end_date, campaign_name, publisher, CASE WHEN REGEXP_CONTAINS(campaign_name,'cid|c:')
                                       THEN SPLIT(TRIM(LOWER(REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(campaign_name, '|')[SAFE_OFFSET(1)], '  ', ''), ' ', '')
                                                             )), '&')[SAFE_OFFSET(0)] END AS cp2
                        FROM( SELECT distinct * EXCEPT(Brand)
                                     REPLACE(CASE WHEN REGEXP_CONTAINS(campaign_name,'c:') THEN REGEXP_REPLACE(campaign_name, '-', '|') ELSE campaign_name END AS campaign_name,
                                             CASE WHEN cid IS NULL THEN 888 ELSE cid END AS cid)
                                     ,CASE WHEN REGEXP_CONTAINS(Brand, '(?i)gala bingo' ) THEN 'Gala Bingo'
                                           WHEN REGEXP_CONTAINS(Brand, '(?i)gala spins' ) THEN 'Gala Spins'
                                           WHEN REGEXP_CONTAINS(Brand, '(?i)gala casino') THEN 'Gala Casino'
                                           WHEN REGEXP_CONTAINS(Brand, '(?i)coral'   )    THEN 'Coral'
                                           WHEN REGEXP_CONTAINS(Brand, '(?i)uk|adbro')    THEN 'Ladbrokes'
                                      END AS Brand
                              FROM {{ref('lc_dim_campaigns')}}
                              WHERE CAST(Date AS Date) IS NOT NULL AND cid IS NOT NULL AND cid <> 52840
                                AND NOT REGEXP_CONTAINS(brand, '(?i)party|bwin|cheeky|foxy|kes.be|belgium'))
                       )),

        makrt AS ( SELECT vu.* EXCEPT(cp1, cp2, cid2, campaign_n, end_date)
                            REPLACE(CASE WHEN source = 'dfa' AND REGEXP_CONTAINS(Campaign, 'notset|not set') AND rr.campaign_name IS NOT NULL THEN rr.campaign_name
                                         WHEN campaign_n IS NOT NULL THEN campaign_n ELSE campaign END AS campaign,
                                    CASE WHEN source = 'dfa' AND REGEXP_CONTAINS(Campaign, 'notset|not set') AND rr.publisher IS NOT NULL THEN rr.publisher ELSE source END AS source)

                   FROM(  SELECT  ru.*, cp2
                                 ,CASE WHEN REGEXP_CONTAINS(adcontent, 'cid|c:') AND NOT REGEXP_CONTAINS(adcontent, '(?i)52840') AND (NOT REGEXP_CONTAINS(campaign, '(?i)cid|c:') OR campaign IS NULL)
                                       THEN c.campaign_name END AS campaign_n
                                 ,CASE WHEN REGEXP_CONTAINS(adcontent, 'cid|c:') AND NOT REGEXP_CONTAINS(adcontent, '(?i)52840') AND (NOT REGEXP_CONTAINS(campaign, '(?i)cid|c:') OR campaign IS NULL)
                                       THEN end_date END AS end_date
                          FROM (SELECT *, CASE WHEN REGEXP_CONTAINS(adcontent,'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                               WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)],1,5) AS INT64) END AS cid2
                                        ,TRIM(LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CASE WHEN REGEXP_CONTAINS(campaign,'c:')
                                                      THEN REGEXP_REPLACE(Campaign, '-', '|') ELSE Campaign END, '  ', ''),' ', ''),'fresh 8|fresh8', 'f8'),'g50fs', 'g50+fs'))) cp1
                                FROM rau) ru
                          LEFT JOIN (SELECT * FROM camp2 WHERE rank = 1)c
                          ON (cp1 = cp2 AND cid2 = cid AND c.Brand = ru.Brand)
                        ) vu

                  LEFT JOIN (SELECT * FROM camp2 WHERE rank = 1) rr
                         ON vu.cid2 = rr.cid AND rr.Brand = vu.Brand
                   )



SELECT distinct * EXCEPT (campaign_name, cpid, camp, Campaign_ID, tracker_excl)
         REPLACE( CASE WHEN REGEXP_CONTAINS(campaign,'(?i)verizon|taboola')
                         OR (REGEXP_CONTAINS(source,  '(?i)display') AND REGEXP_CONTAINS(source,  '(?i)partner'))
                         OR (REGEXP_CONTAINS(campaign,'(?i)partner') AND REGEXP_CONTAINS(campaign,'(?i)cid|c:') AND NOT REGEXP_CONTAINS(campaign,'(?i)7star'))
                         OR REGEXP_CONTAINS(LOWER(REGEXP_REPLACE(source, 'searchcactus', '')), (SELECT * FROM ptnr2)) THEN 'Display - Partners'

                       WHEN (REGEXP_CONTAINS(campaign,  '(?i)display') AND REGEXP_CONTAINS(campaign, '(?i)prog'))
                         OR (medium = 'cpm' AND REGEXP_CONTAINS(campaign, '(?i)prog'))
                         OR source = 'dfa'  AND REGEXP_CONTAINS(adcontent, 'cid|c:')
                         OR REGEXP_CONTAINS(source, '(?i)dv360|appnexus')
                         OR REGEXP_CONTAINS(campaign, '(?i)dv360|appnexus|7stars')              THEN 'Display - Programmatic'

                       WHEN (REGEXP_CONTAINS(source, '(?i)display') AND REGEXP_CONTAINS(source, '(?i)other'))
                         OR ((NOT REGEXP_CONTAINS(campaign, '(?i)social') OR campaign IS NULL)
                         AND medium = 'cpm')                                                    THEN 'Display - Other'
                       WHEN REGEXP_CONTAINS(campaign, '(?i)vod')
                         OR REGEXP_CONTAINS(source, '(?i)vod')                                  THEN 'Display - VOD'

                       WHEN REGEXP_CONTAINS(campaign, '(?i)uac|apple') OR REGEXP_CONTAINS(source, '(?i)uac|apple_ads') THEN
                       CASE WHEN REGEXP_CONTAINS(campaign, '(?i)Competitor')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'comp' THEN 'UAC - Competitor'
                            WHEN REGEXP_CONTAINS(campaign,'(?i)Generic|search_non_brand')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'gen'  THEN 'UAC - Generic'
                            WHEN REGEXP_CONTAINS(campaign, '(?i)Brand')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'bnd'  THEN 'UAC - Brand'
                            ELSE 'UAC - Other'                                                  END

                       WHEN source = 'referral|facebook'                                                 THEN 'Affiliate'
                       WHEN REGEXP_CONTAINS(source, '(?i)grandstand')                                    THEN 'CRM - Grandstand'
                       WHEN REGEXP_CONTAINS(source, '(?i)crm') OR REGEXP_CONTAINS(campaign, '(?i)crm') OR REGEXP_CONTAINS(medium, '(?i)crm') THEN
                       CASE WHEN REGEXP_CONTAINS(medium, '(?i)push')                                     THEN 'CRM - Push'
                            WHEN REGEXP_CONTAINS(medium, '(?i)mail|emai')                                THEN 'CRM - Email'
                            WHEN REGEXP_CONTAINS(campaign, '(?i)aceboo|snap')
                              OR REGEXP_CONTAINS(source, '(?i)aceboo|snap')
                              OR REGEXP_CONTAINS(medium, '(?i)social')                                   THEN 'CRM - Social'
                            ELSE 'CRM - Other'                                                           END

                       WHEN medium IN ('cpc', 'b', 'e', 'p')
                         OR (REGEXP_CONTAINS(source, 'oogle') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL))
                         OR (REGEXP_CONTAINS(source,'(?i)search_') AND NOT REGEXP_CONTAINS(source, '(?i)coral|adbro|gala|pineapple|support'))
                         OR (REGEXP_CONTAINS(ChannelGrouping, 'blue|Direct') AND SUBSTR(Campaign,1,1) = '3')   THEN
                       CASE WHEN REGEXP_CONTAINS(campaign, '(?i)Competitor')
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'comp')  THEN 'PPC - Competitor'
                            WHEN REGEXP_CONTAINS(campaign,'(?i)Generic|search_non_brand') OR campaign LIKE '%|gen|%'
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'gen' )  THEN 'PPC - Generic'
                            WHEN REGEXP_CONTAINS(campaign, '(?i)Brand')
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'bnd' )  THEN 'PPC - Brand'
                            ELSE 'PPC - Other'                                                  END

                       WHEN REGEXP_CONTAINS(source, 'amp.org|ampproject.org')
                        AND REGEXP_CONTAINS(source, r'(?i) Gala|oral|adbrokes')                 THEN 'PPC - Other'
                       WHEN REGEXP_CONTAINS(campaign,'(?i)twitter|snapc|faceboo|youtub|instag|social')
                         OR REGEXP_CONTAINS(SPLIT(source,'|')[SAFE_OFFSET(0)],
                                        '(?i)twitter|t.co$|snap|faceboo|youtub|instag|social')  THEN
                       CASE WHEN campaign <> '(not set)'                                        THEN 'Social - Paid'
                            ELSE 'Social - Organic'                                             END

                       WHEN medium = 'organic'                                             THEN
                       CASE WHEN source = 'google'                                              THEN 'Organic - Google'
                            WHEN source = 'bing'                                                THEN 'Organic - Bing'
                            WHEN source = 'yahoo'                                               THEN 'Organic - Yahoo'
                            ELSE 'Organic - Other'                                              END

                       WHEN medium = 'Affiliate'
                         OR SAFE_CAST(SPLIT (source,'_') [SAFE_OFFSET(0)] AS INT64) IS NOT NULL
                         OR REGEXP_CONTAINS(source,'tradedoubler')                              THEN 'Affiliate'
                       WHEN REGEXP_CONTAINS(campaign, '(?i)display')                            THEN
                       CASE WHEN REGEXP_CONTAINS(campaign, '(?i)prog')                          THEN 'Display - Programmatic'
                            ELSE 'Display - Other' END

                       WHEN ChannelGrouping IN ('Direct', 'Referral', '(Other)', 'blue') OR medium = 'referral' THEN
                       CASE WHEN REGEXP_CONTAINS(source, 'yahoo' )                              THEN 'Organic - Yahoo'
                            WHEN REGEXP_CONTAINS(source,
                                 'googlesyndication|ads.google|doubleclick')                    THEN 'Display - Other'
                            WHEN REGEXP_CONTAINS(source, 'google')                              THEN 'Organic - Google'
                            WHEN REGEXP_CONTAINS(source, 'bing$|bing.com')                      THEN 'Organic - Bing'
                            WHEN REGEXP_CONTAINS(source, 'search|yandex|dogpile|duckgo')        THEN 'Organic - Other'
                            WHEN wm_tracking IN (SELECT distinct tracker FROM ptnr)             THEN 'Display - Partners'
                            WHEN tracker_excl = 1                                               THEN 'Affiliate'
                            WHEN REGEXP_CONTAINS(source, (SELECT * FROM excl))
                              OR source = 'unknown' OR source IS NULL                           THEN 'Direct'
                            WHEN REGEXP_CONTAINS(source,'googleweblight|mail.')                 THEN 'Referral - Other'
                            WHEN medium = '(none)' AND source = '(direct)'                      THEN 'Direct'
                            ELSE 'Referral - Other'                                         END

                      ELSE 'Direct'
                  END AS ChannelGrouping,

                  CASE WHEN click_conversion =0 AND view_conversion =0 THEN 1 ELSE view_conversion END AS view_conversion
                  )


FROM makrt
