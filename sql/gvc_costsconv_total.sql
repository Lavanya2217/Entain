WITH sess AS ( SELECT distinct * REPLACE(CAST(date AS DATE) AS Date) FROM {{ref('gvc_sessions_count_cids')}} )

    SELECT c.* EXCEPT(crm_count, cid, cid2)
               REPLACE(session_count AS visits)
           , NULL AS country, CASE WHEN LENGTH(SAFE_CAST(c.cid AS STRING)) > 5 THEN c.cid end AS GA_wm_track, c.cid AS cid, 'cid' AS join_type
    FROM (SELECT *, CASE WHEN LENGTH(SAFE_CAST(cid AS STRING)) > 5 THEN 0 ELSE cid END AS cid2
          FROM {{ref('gvc_costsconv_cids')}}) c
    LEFT JOIN( SELECT Date, Brand, cid, COUNT(distinct sessionid) AS session_count
               FROM sess WHERE join_type = 'cid'
               GROUP BY 1,2,3) s
    ON c.Brand = s.Brand AND c.date = s.date AND c.cid2 = s.cid

UNION ALL

    SELECT c.* EXCEPT(crm_count, cid, GA_wm_track, country, rank)
               REPLACE(CASE WHEN rank <> 1 THEN visits ELSE session_count END AS visits)
           , c.country, GA_wm_track AS GA_wm_track, GA_wm_track AS cid, 'aff'  AS join_type
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Brand, date, GA_wm_track, country ORDER BY campaign) rank
          FROM {{ref('gvc_costsconv_aff')}} ) c
    LEFT JOIN( SELECT Date, Brand, cid, country, COUNT(distinct sessionid) AS session_count
               FROM sess WHERE join_type = 'cid'
               GROUP BY 1,2,3,4) s
    ON c.Brand = s.Brand AND c.date = s.date AND c.ga_wm_track = s.cid AND c.country=s.country


UNION ALL
    SELECT p.* EXCEPT(crm_count,campaignid) REPLACE(session_count AS visits)
           , NULL AS country, NULL AS GA_wm_track, NULL AS cid,'adwords' AS join_type
    FROM {{ref('gvc_costsconv_google_ppc')}} p
    LEFT JOIN( SELECT Date, Brand, campaignid, COUNT(distinct sessionid) AS session_count
               FROM sess WHERE join_type = 'ppc'
               GROUP BY 1,2,3) s
    ON p.Brand = s.Brand AND p.date = s.date AND p.campaignid = s.campaignid



UNION ALL
    SELECT n.* EXCEPT(crm_count, GA_wm_track)
           ,GA_wm_track, NULL AS cid, 'name' AS join_type
    FROM {{ref('gvc_costsconv_nocids')}} n
