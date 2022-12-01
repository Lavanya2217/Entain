WITH   lal AS ( SELECT Date, Brand, ChannelGrouping, Publisher, cid
                      ,MAX(campaign_name) AS Campaign_n
                      ,ROUND(SUM(spend),2)         AS Spend
                      ,ROUND(SUM(clicks),-1)       AS Clicks
                      ,ROUND(SUM(impressions))     AS Impressions

               FROM(   SELECT * REPLACE(CAST(Date AS DATE) AS Date),
                              CASE WHEN REGEXP_CONTAINS(Campaign, '(?i)cid') THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                                   WHEN REGEXP_CONTAINS(Campaign, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                              END AS cid
                       FROM {{ref('lc_campaigns_costs')}}
                       WHERE Date > '2020-01-20' AND google_excl IS NULL)

               WHERE cid IS NOT NULL AND SAFE_CAST(cid AS INT64) NOT IN (52840,00000)
               GROUP BY 1,2,3,4,5,campaign_name),

       mar AS( SELECT * REPLACE( CASE WHEN source = 'dfa' AND (NOT REGEXP_CONTAINS(Campaign, 'notset|not set') OR campaign IS NULL) THEN '' ELSE source END AS source)
                      ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                            WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                            WHEN REGEXP_CONTAINS(adcontent, 'cid') THEN SAFE_CAST(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)] AS INT64)
                            WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)] AS INT64)
                            END AS cid
                      ,CASE WHEN ChannelGrouping = 'Display - Partners' AND adcontent IS NOT NULL AND NOT REGEXP_CONTAINS(adcontent, 'cid')
                            THEN adcontent END AS partner_name
                      ,CASE WHEN REGEXP_CONTAINS(source, 'oogle|earc') AND (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL)
                             AND (NOT REGEXP_CONTAINS(ChannelGrouping, 'UAC') OR ChannelGrouping IS NULL)
                            THEN 1 END AS google_excl
               FROM {{ref('lc_conversions_unique_ledger')}}),

       far AS( SELECT distinct Brand, cid, IF(REGEXP_CONTAINS(ChannelGrouping, 'artner') AND REGEXP_CONTAINS(Brand, '(?i)gala'), '' ,source) AS source
               FROM mar
               WHERE Date> '2019-12-31' AND cid IS NOT NULL AND SAFE_CAST(cid AS INT64) NOT IN (52840,00000) AND google_excl IS NULL
               UNION ALL
               SELECT distinct Brand, cid, IF(REGEXP_CONTAINS(ChannelGrouping, 'artner') AND REGEXP_CONTAINS(Brand, '(?i)gala'), '' ,publisher) AS source FROM lal)


 SELECT distinct brand, cid, 1 AS index
 FROM(
        SELECT distinct Brand, cid, COUNT(distinct source) AS sources
        FROM far
        GROUP BY 1,2)
 WHERE sources >1
