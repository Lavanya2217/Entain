WITH sess AS ( SELECT * REPLACE(CAST(date AS DATE) AS Date) FROM {{ref('lc_sessions_count_cids')}} )



    SELECT c.* REPLACE(session_count AS visits)
    FROM(
            SELECT * EXCEPT(crm_count,cid)
                   ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)affiliate') THEN SAFE_CAST(cid AS INT64) ELSE NULL END AS GA_wm_track
                   ,cid AS cid, 'cid'  AS join_type
            FROM {{ref('lc_costsconv_cids')}}

        UNION ALL
            SELECT * EXCEPT(crm_count,cid)
                   ,CASE WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)affiliate') THEN SAFE_CAST(cid AS INT64) ELSE NULL END AS GA_wm_track
                   ,cid AS cid, 'cid'  AS join_type
            FROM {{ref('lc_costsconv_aff')}}
        )c
    LEFT JOIN( SELECT Date, Brand, cid, COUNT(distinct sessionid) AS session_count
               FROM sess WHERE join_type = 'cid'
               GROUP BY 1,2,3) s
    ON c.Brand = s.Brand AND c.date = s.date AND c.cid = s.cid


UNION ALL
    SELECT p.* EXCEPT(crm_count,campaignid) REPLACE(session_count AS visits)
           , NULL AS GA_wm_track, NULL AS cid,'adwords' AS join_type
    FROM {{ref('lc_costsconv_google_ppc')}} p
    LEFT JOIN( SELECT Date, Brand, campaignid, COUNT(distinct sessionid) AS session_count
               FROM sess WHERE join_type = 'ppc'
               GROUP BY 1,2,3) s
    ON p.Brand = s.Brand AND p.date = s.date AND p.campaignid = s.campaignid



UNION ALL
    SELECT n.* EXCEPT(crm_count, GA_wm_track)
           ,GA_wm_track, NULL AS cid, 'name' AS join_type
    FROM {{ref('lc_costsconv_nocids')}} n
