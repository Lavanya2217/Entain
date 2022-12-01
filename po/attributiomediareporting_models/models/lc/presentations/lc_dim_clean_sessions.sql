WITH bla AS(
            SELECT *, CONCAT(Brand, '_', SessionID) AS id
                    ,SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})')))
                                END AS INT64)                                         AS wm_tracking
            FROM(
                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,'Coral'                                                                                                          AS Brand
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
                        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                          AS CD108
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId
                FROM {{ source('Coral_GA', 'ga_sessions_*') }} t, t.hits AS hits
                WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
                GROUP BY 2,3,6,7,8,9,10,11,12,14,15,16,17

          UNION ALL

                SELECT REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')                                 AS Date_added
                        ,'Ladbrokes'                                                                                                      AS Brand
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
                        ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=108 ))                                          AS CD108
                        ,ChannelGrouping
                        ,hits.page.pagepath                                                                                               AS landing
                        ,trafficSource.adContent                                                                                          AS adContent
                        ,trafficSource.adwordsClickInfo.campaignId                                                                        AS campaignId
                FROM {{ source('Lads_GA', 'ga_sessions_*') }} t, t.hits AS hits
                WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '')
                GROUP BY 2,3,6,7,8,9,10,11,12,14,15,16,17

                )
    ),

rop AS( SELECT * FROM bla INNER JOIN {{ source('exclusions_lists_lc', 'exclusion_list') }} e ON source LIKE e.src),
tra AS( SELECT distinct TrackerID  AS tracker
        FROM {{ source('exclusions_lists_lc', 'exclusion_list_Affiliates_final') }}
           UNION ALL
        SELECT distinct Tracker_id AS tracker
        FROM {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }}
        ),

ver AS( SELECT * EXCEPT(excl, src) FROM rop
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
      JOIN (SELECT distinct * FROM bla WHERE ChannelGrouping <> 'Direct' AND id NOT IN (SELECT distinct id FROM rop) ) y
      ON huh.Brand = y.Brand AND huh.fullVisitorId = y.fullVisitorId AND huh.session_start > y.session_start AND DATE_ADD(DATE(huh.session_start), INTERVAL -90 DAY) <= DATE(y.session_start)
)WHERE rank =1
