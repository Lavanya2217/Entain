WITH   BwinGA     AS(  SELECT * FROM {{ source( 'Bwin_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     CheekyGA     AS(  SELECT * FROM {{ source( 'Cheeky_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       GalaGA     AS(  SELECT * FROM {{ source( 'Gala_GA'  , 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
       FoxyGA     AS(  SELECT * FROM {{ source( 'Foxy_GA'  , 'ga_sessions_*') }} t, t.hits  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') 
                                                                                                 AND REGEXP_CONTAINS(hits.page.hostname, '(?i)foxy')),

cleanfirstpagiew   AS( SELECT *, SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{6})')))
                                END AS INT64)                                         AS wm_tracking
                        FROM(
                                    SELECT 'Bwin'                                                            AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM BwinGA t, t.hits AS hits
                                     WHERE channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 2,3
                        UNION ALL
                                    SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' ) THEN 'Gala Bingo'
                                                WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)casino') THEN 'Gala Casino'
                                           ELSE 'Gala Spins' END                                             AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM GalaGA t, t.hits AS hits
                                     WHERE channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 1,2,3
                        UNION ALL
                                    SELECT CASE WHEN REGEXP_CONTAINS(hits.page.hostname, '(?i)bingo' )
                                                THEN 'Foxy Bingo' ELSE 'Foxy Casino' END                     AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM FoxyGA t, t.hits AS hits
                                     WHERE channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 1,2,3
                        UNION ALL
                                    SELECT 'Cheeky Bingo'                                                   AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM CheekyGA t, t.hits AS hits
                                     WHERE channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 1,2,3
                                     )
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
                                              CASE WHEN s.hours_oldness   IS NOT NULL THEN s.hours_oldness   ELSE Lag_hours         END AS Lag_hours
                                              )
                           FROM {{ref('gvc_conversions_analytics')}} r
                           LEFT JOIN (SELECT distinct * FROM {{ref('gvc_dim_clean_sessions')}}) s
                           ON CONCAT(CONCAT(r.fullVisitorId, '_', CAST(r.visitId AS STRING))) = s.sessionID AND s.Date = visitStartDate
                           WHERE rank = 1)
                           ),




           ganull   AS( SELECT distinct * FROM ga_conversions   WHERE CustomerID IS NULL    ),

           ga       AS( SELECT distinct * FROM ga_conversions   WHERE CustomerID IS NOT NULL),

           sessions AS( SELECT distinct Brand, sessionID,     CustomerID  FROM {{ref('gvc_dim_customer_sessions')}}),

           visitors AS( SELECT distinct Brand, fullVisitorId, FIRST_VALUE (CustomerID) OVER( PARTITION BY fullvisitorID ORDER BY date_added DESC) AS CustomerID
                        FROM {{ref('gvc_dim_customer_sessions')}}
                        ),

           joined   AS( SELECT x.*  REPLACE( c.CustomerID AS CustomerID)
                        FROM ganull x
                        LEFT JOIN sessions c
                        ON x.Brand = c.Brand AND CONCAT(x.fullVisitorId, '_', CAST(x.visitId AS STRING)) = c.sessionID
                        ),

           joined2  AS( SELECT yy.* REPLACE( z.CustomerID AS CustomerID)
                        FROM joined yy
                        LEFT JOIN visitors z ON yy.Brand=z.Brand AND z.fullVisitorId = yy.fullVisitorId
                        WHERE yy.CustomerID IS NULL
                        ),

           uni      AS( SELECT * FROM joined WHERE CustomerID IS NOT NULL
                        UNION ALL
                        SELECT * FROM joined2
                        UNION ALL
                        SELECT * FROM ga),


        final       AS( SELECT * EXCEPT(rank, rankrep)
                                 REPLACE(CASE WHEN REGEXP_CONTAINS(keyword, 'c:') THEN keyword
                                              WHEN REGEXP_CONTAINS(landing, 'c:') AND campaign ='(not set)'
                                              THEN SPLIT(SPLIT(SPLIT(REGEXP_REPLACE(landing, '(?i)tduid=', 'tdpeh='), 'tdpeh=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)], '^p')[SAFE_OFFSET(0)]
                                              ELSE campaign END AS campaign,

                                         CASE WHEN REGEXP_CONTAINS(landing, 'utm_source=') AND REGEXP_CONTAINS(source, 'direct|not set|none')
                                              THEN SPLIT(SPLIT(landing,'utm_source=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]
                                              ELSE source END AS source,
                                         CASE WHEN REGEXP_CONTAINS(landing, 'utm_medium=') AND REGEXP_CONTAINS(medium, 'direct|not set|none')
                                              THEN SPLIT(SPLIT(landing,'utm_medium=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]
                                              ELSE medium END AS medium
                                         )
                                 ,CASE WHEN REGEXP_CONTAINS(campaign, '(?i)dfa') THEN SPLIT(campaign, ':')[SAFE_OFFSET(2)] END AS cpid
                        FROM(
                              SELECT uni.* REPLACE(CASE WHEN uni.wm_tracking IS NULL AND s.wm_tracking IS NOT NULL THEN s.wm_tracking ELSE uni.wm_tracking END AS wm_tracking,
                                                   CASE WHEN uni.wm_tracking IS NULL AND s.landing     IS NOT NULL THEN s.landing     ELSE uni.landing     END AS landing)
                                     , CASE WHEN Conversion = 'Bet' THEN 1 ELSE ROW_NUMBER () OVER( PARTITION BY CustomerID, Conversion, Event_time) END AS rankrep
                              FROM uni

                              LEFT JOIN (SELECT distinct * FROM cleanfirstpagiew WHERE wm_tracking IS NOT NULL) s
                                     ON uni.Brand = s.brand AND CONCAT(uni.fullVisitorid, '_', uni.visitId) = s.sessionID
                            )
                        WHERE rankrep = 1
                        )

SELECT final.* EXCEPT(cpid)
               REPLACE(CASE WHEN cpid IS NOT NULL AND camp IS NOT NULL THEN camp ELSE campaign END AS campaign,
                       CASE WHEN REGEXP_CONTAINS(website, '(?i)gala|foxy|cheeky') THEN REGEXP_REPLACE(CONCAT(LOWER(Brand), '.com'), ' ','') ELSE website END AS website)
FROM final
LEFT JOIN (SELECT distinct Campaign_ID , Campaign AS camp
           FROM {{ source('DCM_UK', 'p_match_table_campaigns_785192') }}
           WHERE NOT REGEXP_CONTAINS(Campaign, '22778|20982|61489') )s
       ON s.Campaign_ID = cpid
