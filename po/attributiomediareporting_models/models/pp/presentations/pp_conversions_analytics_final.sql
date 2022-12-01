WITH PartyCasinoGA    AS(  SELECT * FROM {{ source( 'PartyCasino_GA'  , 'ga_sessions_*') }}         WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),
     PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),

cleanfirstpagiew   AS( SELECT *, SAFE_CAST(CASE WHEN REGEXP_CONTAINS(landing, '(?i)trackerid=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)trackerid=([0-9]{6})')))
                                WHEN REGEXP_CONTAINS(landing, '(?i)wm=')
                                THEN IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{8})'),
                                            IFNULL(REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{7})'),REGEXP_EXTRACT(landing, r'(?i)wm=([0-9]{6})')))
                                END AS INT64)                                         AS wm_tracking
                        FROM(
                                    SELECT 'Party Casino'                                                    AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM PartyCasinoGA t, t.hits AS hits
                                     WHERE channelgrouping IN( 'Direct', 'Referral', '(Other)')
                                       AND hits.hitnumber = 1 AND REGEXP_CONTAINS(hits.page.pagepath, '(?i)trackerid|wm=')
                                     GROUP BY 2,3
                        UNION ALL
                                    SELECT 'Party Poker'                                                    AS Brand
                                           ,CONCAT(fullVisitorId, '_', CAST(visitId AS STRING))              AS sessionID
                                           ,hits.page.pagepath                                               AS landing
                                      FROM PartyPokerGA t, t.hits AS hits
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
                           FROM {{ref('pp_conversions_analytics')}} r
                           LEFT JOIN (SELECT distinct * FROM {{ref('pp_dim_clean_sessions')}}) s
                           ON CONCAT(CONCAT(r.fullVisitorId, '_', CAST(r.visitId AS STRING))) = s.sessionID AND s.Date = visitStartDate
                           WHERE rank = 1)
                           ),




           ganull   AS( SELECT distinct * FROM ga_conversions   WHERE CustomerID IS NULL    ),

           ga       AS( SELECT distinct * FROM ga_conversions   WHERE CustomerID IS NOT NULL),

           sessions AS( SELECT distinct Brand, sessionID,     CustomerID  FROM {{ref('pp_dim_customer_sessions')}}),

           visitors AS( SELECT distinct Brand, fullVisitorId, FIRST_VALUE (CustomerID) OVER( PARTITION BY fullvisitorID ORDER BY date_added DESC) AS CustomerID
                        FROM {{ref('pp_dim_customer_sessions')}}
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
                        SELECT * FROM ga)

      , fp_join as (
      SELECT * EXCEPT(rank, rankrep)
               REPLACE(CASE WHEN (NOT REGEXP_CONTAINS(campaign, 'c:') OR campaign IS NULL) AND REGEXP_CONTAINS(keyword, 'c:') THEN keyword
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
      FROM(
            SELECT uni.* REPLACE(CASE WHEN s.wm_tracking IS NOT NULL THEN s.wm_tracking ELSE uni.wm_tracking END AS wm_tracking,
                                 CASE WHEN s.landing     IS NOT NULL THEN s.landing     ELSE uni.landing     END AS landing)
                   , CASE WHEN Conversion = 'Bet' THEN 1 ELSE ROW_NUMBER () OVER( PARTITION BY CustomerID, Conversion, Event_time) END AS rankrep
            FROM uni

            LEFT JOIN (SELECT distinct * FROM cleanfirstpagiew WHERE wm_tracking IS NOT NULL) s
                   ON uni.Brand = s.brand AND CONCAT(uni.fullVisitorid, '_', uni.visitId) = s.sessionID
          )
      WHERE rankrep = 1
      )


SELECT a.*
        REPLACE(CASE WHEN b.ChannelGrouping IS NOT NULL THEN b.ChannelGrouping ELSE a.ChannelGrouping END AS ChannelGrouping,
                CASE WHEN b.Campaign        IS NOT NULL THEN b.Campaign        ELSE a.Campaign        END AS Campaign,
			          CASE WHEN b.campaignId      IS NOT NULL THEN b.campaignId      ELSE a.campaignId      END AS campaignId,
                CASE WHEN b.medium          IS NOT NULL THEN b.medium          ELSE a.medium          END AS medium,
                CASE WHEN b.source          IS NOT NULL THEN b.source          ELSE a.source          END AS source,
                CASE WHEN b.keyword         IS NOT NULL THEN b.keyword         ELSE a.keyword         END AS keyword,
			          CASE WHEN b.adContent       IS NOT NULL THEN b.adContent       ELSE a.adContent       END AS adContent,
                CASE WHEN b.website 		    IS NOT NULL THEN b.website  		   ELSE a.website 	      END AS website,
                CASE WHEN b.adWords_gclid   IS NOT NULL THEN b.adWords_gclid   ELSE a.gclid 	        END AS gclid
                )
    , b.match_rule
		, case when b.match_rule in (2,3,4) then 'Estimated' else 'Actual' end as matching
FROM
    fp_join a
LEFT JOIN
    {{ref('pp_pokervc_registrations')}} b
on
    a.VisitId = b.registration_visitid
    and a.customerid = b.registration_customerid
    and a.conversion = 'Registration'
    and match_rule <> 0
