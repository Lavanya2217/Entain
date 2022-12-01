WITH   BwinGA     AS(  SELECT * FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       GalaGA     AS(  SELECT * FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') 
                                                                                                 AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy')),
bla AS(
            SELECT *
                    , CONCAT(Brand, '_', SessionID) AS id
                    ,SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})')))
                                END AS INT64)                                         AS wm_tracking
            FROM(
                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,'Bwin'                                                                                                           AS Brand
                        ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))                                                              AS sessionID
                        ,MAX(fullVisitorId)                                                                                               AS fullVisitorId
                        ,MAX(visitNumber)                                                                                                 AS Number
                        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS Date
                        ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS visitStartTime
                        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                         AS session_start
                        ,trafficSource.source                                                                                             AS source
                        ,trafficSource.medium                                                                                             AS medium
                        ,trafficSource.campaign                                                                                           AS campaign
                        ,trafficSource.keyword                                                                                            AS keyword
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId

                FROM BwinGA t, t.hits AS hits
                GROUP BY 2,3,6,7,8,9,10,11,12,13,14,15,16
           UNION ALL
                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                              WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                         ELSE 'Gala Spins' END                                                                                            AS Brand
                        ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))                                                              AS sessionID
                        ,MAX(fullVisitorId)                                                                                               AS fullVisitorId
                        ,MAX(visitNumber)                                                                                                 AS Number
                        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS Date
                        ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS visitStartTime
                        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                         AS session_start
                        ,trafficSource.source                                                                                             AS source
                        ,trafficSource.medium                                                                                             AS medium
                        ,trafficSource.campaign                                                                                           AS campaign
                        ,trafficSource.keyword                                                                                            AS keyword
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId

                FROM GalaGA t, t.hits AS hits
                GROUP BY 2,3,6,7,8,9,10,11,12,13,14,15,16
           UNION ALL
                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Foxy Bingo' ELSE 'Foxy Casino' END             AS Brand
                        ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))                                                              AS sessionID
                        ,MAX(fullVisitorId)                                                                                               AS fullVisitorId
                        ,MAX(visitNumber)                                                                                                 AS Number
                        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS Date
                        ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS visitStartTime
                        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                         AS session_start
                        ,trafficSource.source                                                                                             AS source
                        ,trafficSource.medium                                                                                             AS medium
                        ,trafficSource.campaign                                                                                           AS campaign
                        ,trafficSource.keyword                                                                                            AS keyword
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId

                FROM FoxyGA t, t.hits AS hits
                GROUP BY 2,3,6,7,8,9,10,11,12,13,14,15,16
           UNION ALL
                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,'Cheeky Bingo'                                                                                                  AS Brand
                        ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))                                                              AS sessionID
                        ,MAX(fullVisitorId)                                                                                               AS fullVisitorId
                        ,MAX(visitNumber)                                                                                                 AS Number
                        ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS Date
                        ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                             AS visitStartTime
                        ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                         AS session_start
                        ,trafficSource.source                                                                                             AS source
                        ,trafficSource.medium                                                                                             AS medium
                        ,trafficSource.campaign                                                                                           AS campaign
                        ,trafficSource.keyword                                                                                            AS keyword
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId

                FROM CheekyGA t, t.hits AS hits
                GROUP BY 2,3,6,7,8,9,10,11,12,13,14,15,16
                )),

rop AS( SELECT * FROM bla INNER JOIN {{ source('exclusions_lists_gvc', 'exclusion_list_Referrals') }}  e ON bla.source LIKE src),
tra AS( SELECT distinct SAFE_CAST(Tracker_id AS INT64) AS tracker
        FROM {{ref('affiliates_list')}}
     UNION ALL
        SELECT distinct SAFE_CAST(campaign_id AS INT64) AS tracker
        FROM {{ref('gvc_dim_campaigns')}}
        WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND ChannelGrouping = 'Display - Partners' AND NOT REGEXP_CONTAINS(Publisher, '(?i)dcmm') AND SAFE_CAST(campaign_id AS INT64) IS NOT NULL
        ),

ver AS( SELECT * EXCEPT(excl, src, category) FROM rop
        UNION ALL
        SELECT *, NULL AS value    FROM bla WHERE id NOT IN (SELECT distinct id FROM rop) ),

huh AS( SELECT  Date_added
                ,Brand
                ,Date
                ,sessionID
                ,fullVisitorId
                ,Number
                ,session_start
                ,source
                ,medium
                ,campaign
                ,keyword
                ,visitStartTime
                ,ChannelGrouping
                ,landing
                ,wm_tracking
                ,campaignid
                ,adContent

        FROM ver
        WHERE (wm_tracking IS NULL OR wm_tracking < 1 OR wm_tracking NOT IN (SELECT distinct tracker FROM tra))
          AND (ChannelGrouping = 'Direct' OR value = 1)
         )



SELECT distinct * EXCEPT(RANK)
FROM(

      SELECT huh.Date_added
             ,huh.Brand             AS Brand
             ,huh.Date              AS Date
             ,huh.sessionID         AS sessionID
             ,huh.fullVisitorId     AS fullVisitorId
             ,huh.Number            AS Number
             ,y.source              AS source
             ,y.medium              AS medium
             ,y.adContent           AS adContent
             ,y.ChannelGrouping     AS ChannelGrouping
             ,y.campaignid          AS campaignid
             ,y.campaign            AS campaign
             ,y.keyword             AS keyword
             ,y.landing             AS landing
             ,y.wm_tracking         AS wm_tracking
             ,huh.source            AS old_source
             ,huh.medium            AS old_medium
             ,huh.adContent         AS old_adContent
             ,huh.ChannelGrouping   AS old_ChannelGrouping
             ,huh.campaignid        AS old_campaignid
             ,huh.campaign          AS old_campaign
             ,huh.keyword           AS old_keyword
             ,huh.landing           AS old_landing
             ,huh.wm_tracking       AS old_wm_tracking
             ,DATE_DIFF(DATE(huh.session_start), DATE(y.session_start),DAY) AS days_oldness
             ,DATETIME_DIFF(huh.session_start, y.session_start,HOUR) AS hours_oldness
             ,ROW_NUMBER() OVER ( PARTITION BY huh.sessionID ORDER BY y.session_start DESC) AS rank

      FROM huh
      JOIN (SELECT distinct *
            FROM bla WHERE ChannelGrouping <> 'Direct' AND id NOT IN (SELECT distinct id FROM rop)
            ) y
      ON huh.Brand = y.Brand AND huh.fullVisitorId = y.fullVisitorId AND huh.session_start > y.session_start AND DATE_ADD(DATE(huh.session_start), INTERVAL -90 DAY) <= DATE(y.session_start)
     ) WHERE rank =1
