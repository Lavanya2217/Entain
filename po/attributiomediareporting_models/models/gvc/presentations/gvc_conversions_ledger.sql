WITH ga        AS (SELECT *,CASE WHEN Conversion = 'Bet' THEN 1 ELSE ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Date, Hour, Minute ORDER BY event_value DESC) END AS rank
                   FROM(
                          SELECT * EXCEPT(fullVisitorID, device, landing)
                                   REPLACE(CASE WHEN (NOT REGEXP_CONTAINS(Source, '(?i)crm|referral') AND REGEXP_CONTAINS(Source, '(?i)facebook|instagram'))
                                                  OR REGEXP_CONTAINS(Campaign, '(?i)facebook|instagram') OR REGEXP_CONTAINS(medium, '(?i)facebook|instagram')
                                                  OR (REGEXP_CONTAINS(Source, 'social-other') AND REGEXP_CONTAINS(medium, '(?i)vid') )              THEN 'Facebook'
                                                WHEN REGEXP_CONTAINS(Source, 'twitter|t.co$' ) OR REGEXP_CONTAINS(Source, 'twitter' )               THEN 'Twitter'
                                                WHEN REGEXP_CONTAINS(Source, 'ttd|tradedesk' ) OR REGEXP_CONTAINS(Campaign, 'ttd' )                 THEN 'TradeDesk'
                                                WHEN REGEXP_CONTAINS(Source, 'youtube')                                                             THEN 'Youtube'
                                                WHEN REGEXP_CONTAINS(Campaign, 'snap') OR REGEXP_CONTAINS(Source, 'snap')                           THEN 'Snapchat'
                                                WHEN (REGEXP_CONTAINS(Campaign, 'gads') OR REGEXP_CONTAINS(Source, 'google'))
                                                 AND NOT REGEXP_CONTAINS(Source, 'mail|cse|web|account')                                            THEN 'Google_Ads'
                                                WHEN (REGEXP_CONTAINS(Source, '(?i)bing|microsoft') AND NOT REGEXP_CONTAINS(Source, '(?i)bingo'))
                                                  OR (REGEXP_CONTAINS(Campaign, 'bing') AND NOT REGEXP_CONTAINS(Campaign, '(?i)bingo'))             THEN 'Bing_Ads'
                                                WHEN REGEXP_CONTAINS(Campaign,'(?i)dcm') OR REGEXP_CONTAINS(Source,'(?i)dcm')                       THEN 'DCM'
                                                WHEN REGEXP_CONTAINS(ChannelGrouping, 'PPC') THEN
                                                     CASE WHEN REGEXP_CONTAINS(Source, 'google|search$|_search') AND NOT REGEXP_CONTAINS(Source, '(?i)microsoft')
                                                     THEN 'Google_Ads' ELSE Source END
                                                ELSE Source END AS Source
                                          ,CASE WHEN SUBSTR(adcontent,6,1)= '.' AND SAFE_CAST(SUBSTR(adcontent,1,5) AS INT64) IS NOT NULL THEN CONCAT('c:', adcontent) ELSE adcontent END AS adContent
                                          ,CASE WHEN NOT REGEXP_CONTAINS(transaction_id, 'unde') THEN REGEXP_REPLACE(REGEXP_REPLACE(transaction_id, '%2F', '/'), '%2C', ',') END AS transaction_id
                                          ,CASE WHEN REGEXP_CONTAINS(medium, 'wm') THEN SAFE_CAST(SPLIT(medium, 'wm:')[SAFE_OFFSET(1)] AS INT64)
                                                WHEN REGEXP_CONTAINS(landing, '=12345&') THEN 12345 ELSE wm_tracking END AS wm_tracking
                                          ,CASE WHEN SAFE_CAST(Campaign AS INT64) IS NOT NULL AND Campaignid IS NULL AND REGEXP_CONTAINS(Source, '(?i)microsoft|oogle|bing|search')
                                                THEN SAFE_CAST(Campaign AS INT64)
                                                WHEN REGEXP_CONTAINS(Medium, 'facebook|vid') THEN SAFE_CAST(SPLIT(REGEXP_REPLACE(medium,'_','|'), '|')[SAFE_OFFSET(1)] AS INT64)
                                           ELSE Campaignid END AS Campaignid)
                                   ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute

                          FROM (SELECT distinct * FROM {{ref('gvc_conversions_analytics_final')}} )
                          )
                   ),

      excl     AS (SELECT STRING_AGG(excl, '|') FROM {{ source('exclusions_lists_gvc', 'exclusion_list_Referrals') }} ),
      trac     AS (SELECT distinct tracker_id AS tracker
                   FROM {{ref('affiliates_list')}}
                   WHERE tracker_id IS NOT NULL),

      galaprtn AS(SELECT distinct channelgrouping, Publisher, Brand, SAFE_CAST(campaign_id AS INT64) AS tracker_id
                  FROM {{ref('gvc_dim_campaigns')}}
                  WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND ChannelGrouping = 'Display - Partners' AND NOT REGEXP_CONTAINS(Publisher, '(?i)dcmm') AND SAFE_CAST(campaign_id AS INT64) IS NOT NULL),

      ptnr     AS (SELECT distinct tracker_id AS tracker FROM galaprtn),

      ptnr2    AS (SELECT distinct REGEXP_REPLACE(CASE WHEN LENGTH(Publisher) >10 THEN SPLIT(publisher, '.')[SAFE_OFFSET(0)] ELSE Publisher END, 'DCMM_', '') AS partner,
                                   publisher, campaign_name, cid, brand,
                                   ROW_NUMBER() OVER (PARTITION BY cid ORDER BY LENGTH(campaign_name) DESC) AS rank
                   FROM {{ref('gvc_campaigns_costs')}}
                   WHERE Brand = 'Bwin' AND REGEXP_CONTAINS(Channelgrouping,'(?i)partner') AND NOT REGEXP_CONTAINS(publisher,'(?i)twitter|tradedesk|yahoo|facebook|inter') AND cid IS NOT NULL),

      ptnr3    AS (SELECT STRING_AGG(LOWER(partner), '|') FROM (SELECT distinct publisher AS partner FROM galaprtn)),

#Union the results from the three Sources, get FTD date and missing FTDs from DWH
      uni      AS (SELECT s.*  REPLACE( IFNULL(customerid, CAST(CAST(src_account_id AS INT64) AS STRING)) AS customerid
                                       ,IFNULL(S.Brand, t.Brand)                            AS Brand
                                       ,IFNULL(Date, t.FTD_date_id)                         AS Date
                                       ,IFNULL(Hour,   EXTRACT(HOUR FROM FTD_DATETIME))     AS Hour
                                       ,IFNULL(Minute, EXTRACT(MINUTE FROM FTD_DATETIME))   AS Minute
                                       ,IFNULL(event_time, EXTRACT(TIME FROM FTD_DATETIME)) AS Event_time
                                       ,IFNULL(ChannelGrouping, 'Direct')                   AS ChannelGrouping
                                       ,IFNULL(s.Conversion, 'Deposit')                     AS Conversion
                                       ,IFNULL(Click_conversion, 1)                         AS Click_conversion
                                       ,IFNULL(View_conversion , 0)                         AS View_conversion
                                       ,IFNULL(Conv_medium, 'Web')                          AS Conv_medium
                                       ,IFNULL(lag_hours, 0)                                AS Lag_hours
                                       ,IFNULL(lag_days, 0)                                 AS Lag_days
                                       ,IFNULL(Dataset, 'TD')                               AS Dataset
                                       ,IFNULL(event_value, ROUND(First_Deposit_Amt_GBP,2)) AS event_value
                                       ,IFNULL(currency, 'GBP')                             AS currency)
                                       ,t.ftd_date_id AS FTD_Date


                   FROM(  SELECT * EXCEPT(ranknew)
                          FROM {{ref('gvc_conversions_dcm')}} 
                          WHERE Date >= '2021-02-11' AND REGEXP_CONTAINS(Brand, '(?i)Gala|Bwin|foxy|chee') AND ranknew = 1
                        UNION ALL
                          SELECT * EXCEPT(gclid)
                          FROM {{ref('gvc_conversions_appsflyer')}} WHERE REGEXP_CONTAINS(Brand, '(?i)Gala|Bwin|foxy|chee')
                        UNION ALL
                          SELECT * EXCEPT(rank) FROM ga WHERE rank = 1
                        UNION ALL
                          SELECT distinct * EXCEPT(visitStartTime,visitEndTime,match_ind,gclid)
                          FROM  {{ref('missing_deposits_final')}} 
                          WHERE REGEXP_CONTAINS(brand, '(?i)bwin|cheeky|foxy|gala')
                        ) s

                   FULL OUTER JOIN (SELECT distinct *, 'Deposit' AS conversion
                                    FROM {{ref('td_ftds')}} 
                                    WHERE BRAND IN (SELECT distinct Brand FROM ga)) t
                   ON SAFE_CAST(s.CustomerID AS INT64)= SAFE_CAST(t.src_account_ID AS INT64) AND s.Date = t.FTD_DATE_ID AND t.Brand = s.Brand AND t.conversion = s.conversion
                    ),

#Get country and currency associated w/ CustomerID
      uno      AS ( SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(Source, '(?i)google') AND NOT REGEXP_CONTAINS(Campaign, '(?i)cid|c:')
                                           AND Campaignid IS NOT NULL THEN CAST(CampaignId AS STRING) ELSE Campaign END AS Campaign)
                    FROM(
                          SELECT uni.* REPLACE(CAST(FLOOR(Lag_hours/24) AS INT64) AS Lag_days,
                                               CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)ppc') AND CampaignId IS NULL AND SAFE_CAST(SPLIT(Campaign, '|')[SAFE_OFFSET(7)] AS INT64) IS NOT NULL
                                                    THEN SAFE_CAST(SPLIT(Campaign, '|')[SAFE_OFFSET(7)] AS INT64)
                                                    WHEN CampaignId IS NULL THEN
                                                    CASE WHEN REGEXP_CONTAINS(Campaign, 'p:') THEN SAFE_CAST(SPLIT(SPLIT(Campaign, 'p:')[SAFE_OFFSET(1)], '_')[SAFE_OFFSET(0)] AS INT64)
                                                         WHEN cppid IS NOT NULL THEN cppid END
                                                    ELSE CampaignId END AS CampaignId,
                                               CASE WHEN dataset= 'GA' AND Conversion ='Bet' THEN 'EUR'
                                                    WHEN currency IS NULL AND Account_Currency_Cd IS NOT NULL THEN TRIM(Account_Currency_Cd) ELSE TRIM(currency) END AS currency,
                                               CASE WHEN REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') THEN 'UK'
                                                    WHEN Registration_Country_Cd IS NOT NULL THEN Registration_Country_Cd
                                                    WHEN country  IS NULL AND Country_CD IS NOT NULL THEN Country_CD
                                                    ELSE UPPER(country) END AS country )

                         FROM uni
                         LEFT JOIN (SELECT c.*, src_account_id, Registration_Country_Cd
                                    FROM {{ source('DWPRODVIEWSBI', 'DIM_PLAYER_ACC_CURRENCY_LOG') }}  c
                                    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER') }}  dP
                                      ON dp.Player_Id = c.Player_Id) cu
                                ON SAFE_CAST(cu.src_account_id AS INT64) = SAFE_CAST(CustomerID AS INT64) AND uni.date BETWEEN cu.Effective_From_Date AND cu.Effective_To_Date

                         LEFT JOIN (SELECT c.*, src_account_id
                                    FROM {{ source('DWPRODVIEWSBI', 'DIM_PLAYER_COUNTRY_LOG') }}  c
                                    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER') }}  dP
                                      ON dp.Player_Id = c.Player_Id) co
                                ON SAFE_CAST(co.src_account_id AS INT64) = SAFE_CAST(CustomerID AS INT64) AND uni.date BETWEEN co.Effective_From_Date AND co.Effective_To_Date

                         LEFT JOIN (SELECT distinct SAFE_CAST(campaign_id AS INT64) AS cppid, campaign_name
                                    FROM {{ref('gvc_dim_campaigns')}} )
                                ON LOWER(Campaign) = LOWER(Campaign_name)
                   WHERE CustomerID IS NOT NULL
                     AND Date >= '2021-01-01' AND DATE < CURRENT_DATE()
                   )),

#Convert all the currency different from GBP
     converted AS (SELECT uno.*, CASE WHEN uno.currency IS NOT NULL THEN
                                      CASE WHEN uno.currency = 'GBP' THEN ROUND(event_value,2)
                                           WHEN Exchange_rate IS NOT NULL THEN ROUND(event_value * CAST(Exchange_rate AS FLOAT64),2) END
                                  END AS event_value_GBP
                   FROM uno
                   LEFT JOIN  {{ref('dim_exchange_rates')}} xr
                          ON uno.currency = xr.currency AND uno.Date = xr.Date
                   ),

#Assign FTD date and pNGR values to each CustomerID
     pngrjoin  AS (SELECT s.* EXCEPT(FTD_Date)
                              REPLACE(CASE WHEN REGEXP_CONTAINS(Source, '(?i)Apple|esvdigital')
                                           THEN REGEXP_REPLACE(Campaign, 'c_wm', 'c-wm') ELSE Campaign END AS Campaign,
                                      CASE WHEN REGEXP_CONTAINS(Source, '(?i)Apple') OR Campaign LIKE '%-asa-%' OR (REGEXP_CONTAINS(Source, '(?i)esv') AND NOT REGEXP_CONTAINS(campaign, '(?i)gads') ) THEN 'Apple_ads'
                                           ELSE Source END AS Source),
                          FTD_Date,
                          IF(value_20 = 0, NULL, value_20) AS value_20,
                          CASE WHEN value   IS NOT NULL AND value <> 0 THEN value ELSE Avg_value_0 END AS value,
                          CASE WHEN value       IS NOT NULL AND value <>0 THEN 'actual'
                               WHEN Avg_value_0 IS NOT NULL THEN 'avg'   END AS pngr0_type

                   FROM converted s
                   LEFT JOIN (SELECT distinct * FROM  {{ref('td_clv')}}) p
                          ON SAFE_CAST(s.CustomerID AS INT64) = SAFE_CAST(p.Customer_ID AS INT64) AND s.Date = FTD_DATE_ID AND p.Brand = s.Brand
                   ),

       capp    AS ( SELECT * EXCEPT(bup), ROW_NUMBER() OVER( PARTITION BY Brand2, Campaign_id ORDER BY bup DESC) AS rank
                    FROM(
                          SELECT distinct * EXCEPT(ChannelGrouping, Brand, date, currency), Brand as Brand2, date as date2, CASE WHEN REGEXP_CONTAINS(Campaign_name, '(?i)cid|c:') THEN 1 END AS bup
                          FROM {{ref('gvc_dim_campaigns')}}
                          WHERE CAST(Date AS Date) IS NOT NULL AND REGEXP_CONTAINS(publisher, '(?i)microsoft|oogle|bing|search') )
                    ),

#Get Campaign name for PPC Campaigns having the numeric id in the Campaign field
       rayo    AS (SELECT *, NULL AS Campaign_name
                   FROM pngrjoin
                   WHERE SAFE_CAST(Campaign AS INT64) IS NULL
                      OR (SAFE_CAST(Campaign AS INT64) IS NOT NULL AND (Source IS NULL OR NOT REGEXP_CONTAINS(Source, '(?i)microsoft|oogle|bing|search')))
                   UNION ALL
                   SELECT * EXCEPT (Brand2, Date2, Publisher, Campaign_id, End_Date, cid, newest_rank, gap, rank2)
                            REPLACE(CASE WHEN rank2 = 1 AND Campaign_name IS NOT NULL THEN Campaign_name ELSE Campaign END AS Campaign)
                   FROM( SELECT *, ROW_NUMBER() OVER( PARTITION BY Brand, date, event_time, conversion, CustomerID, ChannelGrouping, Campaign, lag_hours, transaction_id, CAST(event_value AS STRING)
                                                      ORDER BY gap ASC, end_date ASC) rank2
                         FROM(
                              SELECT * EXCEPT(rank), CASE WHEN date BETWEEN date2 AND end_date THEN 0
                                                          WHEN date > date2 THEN DATE_DIFF(date, end_date, DAY)
                                                          WHEN date < date2 THEN 1000
                                                     END AS gap
                              FROM (SELECT * FROM pngrjoin WHERE SAFE_CAST(Campaign AS INT64) IS NOT NULL AND REGEXP_CONTAINS(Source, '(?i)microsoft|oogle|bing|search'))
                              LEFT JOIN capp
                              ON Campaign = Campaign_id
                              WHERE rank=1
                              )
                        ) WHERE rank2 = 1
                   ),

       final AS (SELECT d.* EXCEPT (FTD_Date, Value, Value_20, keyword, pngr0_type)
                            REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign,'(?i)xus')                                     THEN 'AppNexus'
                                         WHEN REGEXP_CONTAINS(Campaign,'(?i)Dv360')                                   THEN 'DV360'
                                         WHEN REGEXP_CONTAINS(Campaign,'(?i)uac')                                     THEN 'Google_UAC'
                                         WHEN REGEXP_CONTAINS(Campaign,'(?i)youtube|c:70792')                         THEN 'Youtube'
                                         ELSE Source END AS Source,
                                    CASE WHEN REGEXP_CONTAINS(keyword,'odds') OR (NOT REGEXP_CONTAINS(campaign,'c:') AND REGEXP_CONTAINS(keyword,'c:'))
                                         THEN keyword ELSE campaign END AS campaign)

                       ,CASE WHEN FTD_DATE IS NOT NULL THEN 1 ELSE 0                                                         END AS FTD_date
                       ,CASE WHEN Value    IS NOT NULL THEN pngr0_type                                                       END AS pNGR_0_type
                       ,ROUND(Value,2)                                                                                           AS pNGR_0
                       ,ROUND(Value_20,2)                                                                                        AS pNGR_21
                       ,CASE WHEN keyword = '' OR REGEXP_CONTAINS(keyword,'not set|provided') THEN NULL ELSE keyword         END AS keyword
                       ,CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)dfa') THEN SPLIT(Campaign, ':')[SAFE_OFFSET(2)]             END AS cpid
                       ,CASE WHEN wm_tracking IS NULL OR CAST(wm_tracking AS STRING) NOT IN(SELECT distinct tracker FROM trac) THEN 0 ELSE 1 END AS tracker_excl

                 FROM rayo d
                 ),

#Dim_Campaigns cleaning: transforming hyphens into pipes, removing spaces
        camp2 AS (SELECT *, ROW_NUMBER() OVER( PARTITION BY Brand, cid, cp2 ORDER BY end_date DESC) AS Rank
                  FROM( SELECT Brand, cid, end_date, ChannelGrouping, Campaign_name, CASE WHEN REGEXP_CONTAINS(Campaign_name,'cid|c:')
                                       THEN SPLIT(TRIM(LOWER(REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(Campaign_name, '|')[SAFE_OFFSET(1)], '  ', ''), ' ', '')
                                                             )), '&')[SAFE_OFFSET(0)] END AS cp2
                        FROM( SELECT distinct *
                                     REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign_name,'c:') THEN REGEXP_REPLACE(Campaign_name, '-', '|') ELSE Campaign_name END AS Campaign_name,
                                             CASE WHEN cid IS NULL THEN 888 ELSE cid END AS cid)

                              FROM {{ref('gvc_dim_campaigns')}}
                              WHERE CAST(Date AS Date) IS NOT NULL AND cid IS NOT NULL
                                AND REGEXP_CONTAINS(brand, '(?i)Bwin|Gala'))
                       )),

         makrt AS ( SELECT vu.* EXCEPT(cp1, cp2, cid2, Campaign_n, end_date)
                             REPLACE(COALESCE(rr.campaign_name, COALESCE(Campaign_n, Campaign)) AS Campaign,
                                     COALESCE(Publisher, source) AS source,
                                     LOWER(medium) AS medium)

                    FROM(  SELECT  ru.*, cp2
                                  ,CASE WHEN REGEXP_CONTAINS(adcontent, 'cid|c:') AND (NOT REGEXP_CONTAINS(Campaign, '(?i)cid|c:') OR Campaign IS NULL)
                                        THEN c.Campaign_name END AS Campaign_n
                                  ,CASE WHEN REGEXP_CONTAINS(adcontent, 'cid|c:') AND (NOT REGEXP_CONTAINS(Campaign, '(?i)cid|c:') OR Campaign IS NULL)
                                        THEN end_date END AS end_date

                           FROM (SELECT *, CASE WHEN REGEXP_CONTAINS(adcontent,'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                                WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SUBSTR(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)],1,5) AS INT64) END AS cid2
                                         ,TRIM(LOWER(REGEXP_REPLACE(REGEXP_REPLACE(CASE WHEN REGEXP_CONTAINS(Campaign,'c:')
                                                       THEN REGEXP_REPLACE(Campaign, '-', '|') ELSE Campaign END, '  ', ''),' ', ''))) cp1
                                 FROM final) ru

                           LEFT JOIN (SELECT * FROM camp2 WHERE rank = 1)c
                                  ON (cp1 = cp2 AND cid2 = cid AND c.Brand = ru.Brand)
                         ) vu
                    LEFT JOIN (SELECT * FROM ptnr2 WHERE Rank = 1) rr
                         ON vu.cid2 = rr.cid AND rr.Brand = vu.Brand
                    ),

#Getting the most represented country associated with each CustomerID
            vi AS(  SELECT *
                    FROM( SELECT *, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY CNT DESC) RANK
                          FROM(
                               SELECT distinct CustomerID, country, COUNT(0) CNT
                               FROM uno
                               WHERE country IS NOT NULL
                               GROUP BY 1,2)
                        ) WHERE rank=1
                  )


SELECT m.* EXCEPT (Campaign_name, cpid, tracker_excl)
           REPLACE(IFNULL(m.country, vi.country) AS country,
                  CASE WHEN LENGTH(source)<4 THEN   UPPER(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(source, '_markt', 'markt'),  '_media', '-media') , '_')[SAFE_OFFSET(0)])
                                             ELSE INITCAP(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(source, '_markt', 'markt'),  '_media', '-media') , '_')[SAFE_OFFSET(0)]) END AS source,

                  CASE WHEN (REGEXP_CONTAINS(Source,  '(?i)vod|youtube|cpv') AND NOT REGEXP_CONTAINS(Source, '(?i)vodafone'))
                         OR (REGEXP_CONTAINS(Campaign,'(?i)vod|youtube|cpv') AND NOT REGEXP_CONTAINS(Campaign, '(?i)vodafone'))          THEN 'Display - VOD'

                       WHEN (REGEXP_CONTAINS(Source, '(?i)partner') AND NOT REGEXP_CONTAINS(Source, '(?i).com|.net'))
                         OR (REGEXP_CONTAINS(Campaign,'(?i)partner') AND NOT REGEXP_CONTAINS(source,'(?i)facebook|influencer'))
                      --   OR (Brand = 'Bwin' AND NOT REGEXP_CONTAINS(source, '(?i)ampproject') AND REGEXP_CONTAINS(LOWER(source), (SELECT * FROM ptnr2) ))
                         OR (NOT REGEXP_CONTAINS(source, '(?i).com|bonus.') AND (REGEXP_CONTAINS(source, '(?i)-odds') OR REGEXP_CONTAINS(keyword, '(?i)-odds')
                              OR REGEXP_CONTAINS(campaign, '(?i)_odds|-odds')))
                         OR (REGEXP_CONTAINS(Brand, '(?i)gala') AND REGEXP_CONTAINS(LOWER(source), (SELECT * FROM ptnr3) ))              THEN 'Display - Partners'

                       WHEN (REGEXP_CONTAINS(Campaign, '(?i)prog') AND NOT REGEXP_CONTAINS(Campaign, '(?i)programma'))
                         OR (medium = 'cpm' AND REGEXP_CONTAINS(Campaign, '(?i)prog')) OR REGEXP_CONTAINS(Campaign, '(?i)programmatic')
                         OR REGEXP_CONTAINS(Source, '(?i)dv360|appnexus|tradedesk|verizon')     THEN 'Display - Programmatic'
                       WHEN REGEXP_CONTAINS(Campaign,'(?i)dcm-direct|forza')                    THEN 'Display - Partners'
                       WHEN (REGEXP_CONTAINS(Source, '(?i)display') AND REGEXP_CONTAINS(Source, '(?i)other'))
                         OR REGEXP_CONTAINS(Source, '(?i)dcm') OR REGEXP_CONTAINS(Campaign, '(?i)dcm')
                         OR (REGEXP_CONTAINS(Campaign, '(?i)display|steve') AND NOT REGEXP_CONTAINS(Campaign, '(?i)cpc|facebook') )
                         OR ((NOT REGEXP_CONTAINS(Campaign, '(?i)social') OR Campaign IS NULL) AND tracker_excl <>1
                         AND medium = 'cpm')                                                    THEN 'Display - Other'


                       WHEN REGEXP_CONTAINS(Campaign, '(?i)uac|apple') OR REGEXP_CONTAINS(Source, '(?i)uac|apple_ads') THEN
                       CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)Competitor')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'comp' THEN 'UAC - Competitor'
                            WHEN REGEXP_CONTAINS(Campaign,'(?i)Generic|search_non_brand|-gen-')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'gen'  THEN 'UAC - Generic'
                            WHEN REGEXP_CONTAINS(Campaign, '(?i)Brand')
                              OR SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)] = 'bnd'  THEN 'UAC - Brand'
                            ELSE 'UAC - Other'                                                  END

                       WHEN Source = 'referral|facebook'                                        THEN 'Affiliate'
                       WHEN REGEXP_CONTAINS(Source, 'grandstand')                               THEN 'CRM - Grandstand'
                       WHEN REGEXP_CONTAINS(campaign, '(?i)facebook') AND REGEXP_CONTAINS(campaign, '(?i)crm') THEN 'CRM - Social'
                       WHEN REGEXP_CONTAINS (medium, '(?i)mail') AND Campaign IS NOT NULL       THEN 'CRM - Email'
                       WHEN REGEXP_CONTAINS (Source, '(?i)crm' ) OR medium = 'push'       THEN
                       CASE WHEN medium = 'email'                                               THEN 'CRM - Email'
                            WHEN medium = 'push'                                                THEN 'CRM - Push'
                            ELSE 'CRM - Other'                                                  END

                       WHEN medium IN ('cpc', 'b', 'e', 'p') AND NOT REGEXP_CONTAINS(Source,'(?i)facebook|gazzetta')
                         OR ( (REGEXP_CONTAINS(Source, '(?i)oogle|ppc|bing') AND NOT REGEXP_CONTAINS(Source, '(?i)bingo')) AND (NOT REGEXP_CONTAINS(Campaign, 'not set|notset') OR Campaign IS NULL))
                         OR (REGEXP_CONTAINS(Source,'(?i)search_') AND NOT REGEXP_CONTAINS(Source, '(?i)coral|adbro|gala|pineapple|support'))
                         OR (REGEXP_CONTAINS(ChannelGrouping, 'blue|Direct') AND SUBSTR(Campaign,1,1) = '3')   THEN
                       CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)Competitor')
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'comp')  THEN 'PPC - Competitor'
                            WHEN REGEXP_CONTAINS(Campaign,'(?i)Generic|search_non_brand|non brand') OR Campaign LIKE '%|gen|%'
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'gen' )  THEN 'PPC - Generic'
                            WHEN REGEXP_CONTAINS(Campaign, '(?i)Brand')
                              OR REGEXP_CONTAINS(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)], 'bnd' )  THEN 'PPC - Brand'
                            ELSE 'PPC - Other'                                                  END

                       WHEN REGEXP_CONTAINS(Source, 'amp.org|ampproject.org')
                        AND REGEXP_CONTAINS(Source, r'(?i) Gala|oral|adbrokes')                 THEN 'PPC - Other'
                       WHEN (REGEXP_CONTAINS(SPLIT(Source,'|')[SAFE_OFFSET(0)],'(?i)twitter|t.co$|snap|faceboo|youtub|instag|social')
                             AND NOT REGEXP_CONTAINS(Source, 'social.bet|social.bwin'))
                         OR REGEXP_CONTAINS(Campaign,'(?i)twitter|snap|faceboo|youtub|instag|social')  THEN
                       CASE WHEN Campaign <> '(not set)'                                        THEN 'Social - Paid'
                            ELSE 'Social - Organic'                                             END

                       WHEN medium = 'organic'                                             THEN
                       CASE WHEN REGEXP_CONTAINS(Source,'(?i)google')                           THEN 'Organic - Google'
                            WHEN REGEXP_CONTAINS(Source,'(?i)bing'  )                           THEN 'Organic - Bing'
                            WHEN REGEXP_CONTAINS(Source,'(?i)yahoo' )                           THEN 'Organic - Yahoo'
                            ELSE 'Organic - Other'                                              END

                       WHEN medium = 'Affiliate'
                         OR SAFE_CAST(SPLIT (Source,'_') [SAFE_OFFSET(0)] AS INT64) IS NOT NULL
                         OR REGEXP_CONTAINS(Source,'tradedoubler')                              THEN 'Affiliate'
                       WHEN REGEXP_CONTAINS(Campaign, '(?i)display') OR REGEXP_CONTAINS(Source, '(?i)display') THEN
                       CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)prog')                          THEN 'Display - Programmatic'
                            ELSE 'Display - Other' END

                       WHEN (ChannelGrouping IN ('Direct', 'Referral', '(Other)', 'blue') OR medium = 'referral')
                        AND NOT REGEXP_CONTAINS(Source, 'mail')    THEN
                       CASE WHEN REGEXP_CONTAINS(Source, 'yahoo' )                              THEN 'Organic - Yahoo'
                            WHEN REGEXP_CONTAINS(Source,
                                 'googlesyndication|ads.google|doubleclick')                    THEN 'Display - Other'
                            WHEN REGEXP_CONTAINS(Source, 'bing$|bing.com')                      THEN 'Organic - Bing'
                            WHEN REGEXP_CONTAINS(Source, 'search|yandex|dogpile|duckgo')        THEN 'Organic - Other'
                            WHEN tracker_excl = 1                                               THEN 'Affiliate'
                            WHEN wm_tracking IN (SELECT distinct * FROM ptnr)                   THEN 'Display - Partners'
                            WHEN REGEXP_CONTAINS(Source, (SELECT * FROM excl))                  THEN 'Direct'
                            WHEN REGEXP_CONTAINS(Source,'googleweblight|mail.')                 THEN 'Referral - Other'
                            WHEN medium = '(none)' AND Source = '(direct)'                      THEN 'Direct'
                            WHEN Campaign LIKE '%IOS|FIB%'                                      THEN 'Other'
                            ELSE 'Referral - Other'                                         END

                      ELSE 'Direct'
                  END AS ChannelGrouping
                  )
        ,ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Date, Hour, Minute ORDER BY Click_conversion DESC, Lag_days ASC, Lag_hours ASC) AS dup_rank

        FROM (SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(campaign, '(?i)twitter')         THEN 'Twitter'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)youtube|gvid')    THEN 'Youtube'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)gads')            THEN 'Google_Ads'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)evolutionpeople') THEN 'Evolution People'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)gazzetta')        THEN 'Gazzetta'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)forza')     OR REGEXP_CONTAINS(source, '(?i)forza')         THEN 'Forza App'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)youtube')   OR REGEXP_CONTAINS(source, '(?i)youtube')       THEN 'Youtube'
                                    WHEN REGEXP_CONTAINS(source, '(?i)protothe|proto-thema|proto_thema') OR REGEXP_CONTAINS(campaign, '(?i)protothe|Proto_Thema') THEN 'Protothema'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)vod|yout') AND REGEXP_CONTAINS(Brand, '(?i)gala|foxy|bing') THEN 'VOD'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)TTD')             THEN 'TradeDesk'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)hybtheo')         THEN 'Hybrid Theory'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)taboola')         THEN 'Taboola'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)verizon')         THEN 'Verizon'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)amazon')          THEN 'Amazon'
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)_DSP_')           THEN 'DSP'
                                    WHEN LOWER(campaign) LIKE '%e2_%'                     THEN 'E2 Online'
                                    WHEN LOWER(campaign) LIKE '%+%'                       THEN SPLIT(campaign, 'ACQ_DIRECT+')[SAFE_OFFSET(1)]
                                    WHEN REGEXP_CONTAINS(campaign, '(?i)internal|test|in_house') AND REGEXP_CONTAINS(campaign, '(?i)dcm|display')      THEN 'DCM'
                                    WHEN (dataset = 'DCM' OR (REGEXP_CONTAINS(campaign, '(?i)display|partner') AND NOT REGEXP_CONTAINS(campaign, '(?i)influencer'))  )
                                     AND REGEXP_CONTAINS(campaign, '(?i)c:') AND Brand = 'Bwin' THEN
                                    CASE WHEN Campaign LIKE '%|%' THEN SPLIT(campaign, '|')[SAFE_OFFSET(1)] ELSE SPLIT(campaign, '-')[SAFE_OFFSET(1)] END
                                    WHEN dataset = 'DCM' OR LOWER(source) = 'dfa'         THEN 'DCM'
                                    ELSE source END AS source)
              FROM makrt) m
        LEFT JOIN vi USING (CustomerID)
