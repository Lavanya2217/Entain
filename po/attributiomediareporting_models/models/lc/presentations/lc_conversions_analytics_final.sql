WITH      
cleanfirstpagiew
                    AS( SELECT *, SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{6})')))
                                END AS INT64)                                         AS wm_tracking
                        FROM(
                                    SELECT 'Ladbrokes'                                                       AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM {{ source( 'Lads_GA', 'ga_sessions_*') }} t, t.hits AS hits
                                     WHERE _TABLE_SUFFIX BETWEEN "20200101" AND "20231231"
                                       AND channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 2,3
                        UNION ALL
                                    SELECT 'Coral'                                                           AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM {{ source( 'Coral_GA', 'ga_sessions_*') }} t, t.hits AS hits
                                     WHERE _TABLE_SUFFIX BETWEEN "20200101" AND "20231231"
                                       AND channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 2,3)
                         ),
                         
ga_conversions
                    AS( SELECT distinct *
                        FROM(
                           SELECT distinct r.* EXCEPT(visitStartDate)
                                      REPLACE(CASE WHEN s.ChannelGrouping IS NOT NULL THEN s.ChannelGrouping ELSE r.ChannelGrouping END AS ChannelGrouping,
                                              CASE WHEN s.Campaign        IS NOT NULL THEN s.Campaign        ELSE r.Campaign        END AS Campaign,
                                              CASE WHEN s.medium          IS NOT NULL THEN s.medium          ELSE r.medium          END AS medium,
                                              CASE WHEN s.source          IS NOT NULL THEN s.source          ELSE r.source          END AS source,
                                              CASE WHEN s.keyword         IS NOT NULL THEN s.keyword         ELSE r.keyword         END AS keyword,
                                              CASE WHEN s.wm_tracking     IS NOT NULL THEN s.wm_tracking     ELSE r.WM_tracking     END AS WM_tracking,
                                              CASE WHEN s.landing         IS NOT NULL THEN s.landing         ELSE r.landing         END AS landing,
                                              CASE WHEN s.days_oldness    IS NOT NULL THEN s.days_oldness    ELSE Lag_days          END AS Lag_days,
                                              CASE WHEN s.hours_oldness   IS NOT NULL THEN s.hours_oldness   ELSE Lag_hours         END AS Lag_hours,
                                              CASE WHEN s.hours_oldness   IS NOT NULL THEN NULL              ELSE CD108             END AS CD108
                                              )
                           FROM {{ref('lc_conversions_analytics')}} r
                           LEFT JOIN (SELECT distinct * FROM {{ref('lc_dim_clean_sessions')}}) s
                           ON CONCAT(CONCAT(r.fullVisitorId, '_', CAST(r.visitId AS STRING))) = s.sessionID AND s.Date = visitStartDate
                           WHERE rank = 1)
                           ),



           ganull   AS( SELECT distinct * FROM ga_conversions   WHERE Customer IS NULL    ),

           ga       AS( SELECT distinct * FROM ga_conversions   WHERE Customer IS NOT NULL),

           sessions AS( SELECT distinct Brand, sessionID, CustomerID  FROM {{ref('lc_dim_customer_sessions')}}),

           visitors AS( SELECT distinct Brand, fullVisitorId, FIRST_VALUE (CustomerID) OVER( PARTITION BY fullvisitorID ORDER BY date_added DESC) AS CustomerID
                        FROM {{ref('lc_dim_customer_sessions')}}
                        ),

           joined   AS( SELECT x.*  REPLACE( c.CustomerID AS Customer)
                        FROM ganull x
                        LEFT JOIN sessions c
                        ON x.Brand = c.Brand AND CONCAT(x.fullVisitorId, '_', CAST(x.visitId AS STRING)) = c.sessionID
                        ),

           joined2  AS( SELECT yy.* REPLACE( z.CustomerID AS Customer)
                        FROM joined yy
                        LEFT JOIN visitors z ON yy.Brand=z.Brand AND z.fullVisitorId = yy.fullVisitorId
                        WHERE Customer IS NULL
                        ),

           uni      AS( SELECT * FROM joined WHERE Customer IS NOT NULL
                        UNION ALL
                        SELECT * FROM joined2
                        UNION ALL
                        SELECT * FROM ga)




SELECT * EXCEPT(rank, rankrep)
         REPLACE(CASE WHEN NOT REGEXP_CONTAINS(campaign, 'cid|c:') AND REGEXP_CONTAINS(keyword, 'cid:|c:') THEN keyword ELSE campaign END AS campaign)
FROM(
      SELECT uni.* REPLACE(CASE WHEN s.wm_tracking IS NOT NULL THEN s.wm_tracking ELSE uni.wm_tracking END AS wm_tracking)
             , CASE WHEN Conversion = 'Bet' THEN 1 ELSE ROW_NUMBER () OVER( PARTITION BY Customer, Conversion, Event_time) END AS rankrep
      FROM uni
      LEFT JOIN (SELECT distinct * FROM cleanfirstpagiew WHERE wm_tracking IS NOT NULL) s
      ON uni.Brand = s.brand AND CONCAT(uni.fullVisitorid, '_', uni.visitId) = s.sessionID

    ) WHERE rankrep = 1
