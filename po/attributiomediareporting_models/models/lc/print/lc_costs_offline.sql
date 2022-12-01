WITH coral     AS (SELECT * FROM {{ source('offline_marketing', 'coral_offline_cost') }} ),
     lads      AS (SELECT * FROM {{ source('offline_marketing', 'lads_offline') }} ),
     galabingo AS (SELECT * FROM {{ source('offline_marketing', 'gala_bingo_offline') }} ),
     galaspins AS (SELECT * FROM {{ source('offline_marketing', 'gala_spins_offline') }} ),
     foxy      AS (SELECT * FROM {{ source('offline_marketing', 'foxy_offline') }} )

,uni AS(
SELECT 'Coral'     AS Brand, date, 'TV'    AS Channel, SUM(TV_est__spend)    AS Spend, SUM(TV_est__impacts_reach)    AS Reach
FROM coral    
GROUP BY 1,2    
    
UNION ALL    
    
SELECT 'Coral'     AS Brand, date, 'Radio' AS Channel, SUM(Radio_est__spend) AS Spend, SUM(Radio_est__impacts_reach) AS Reach
FROM coral    
GROUP BY 1,2    
    
UNION ALL    
    
SELECT 'Coral'     AS Brand, date, 'OOH'   AS Channel, SUM(OOH_est__spend)   AS Spend, SUM(OOH_est__impacts_reach)   AS Reach
FROM coral    
GROUP BY 1,2    
    
UNION ALL    
    
SELECT 'Coral'     AS Brand, date, 'Press' AS Channel, SUM(Press_est__spend) AS Spend, SUM(Press_est__reach)         AS Reach
FROM coral
GROUP BY 1,2

---

UNION ALL

SELECT 'Ladbrokes' AS Brand, date, 'TV'    AS Channel, SUM(TV_est__spend)    AS Spend, SUM(TV_est__impacts_reach)    AS Reach
FROM lads
GROUP BY 1,2

UNION ALL

SELECT 'Ladbrokes' AS Brand, date, 'Radio' AS Channel, SUM(Radio_est__spend) AS Spend, SUM(Radio_est__impacts_reach) AS Reach
FROM lads
GROUP BY 1,2

UNION ALL

SELECT 'Ladbrokes' AS Brand, date, 'OOH'   AS Channel, SUM(OOH_est__spend)   AS Spend, SUM(OOH_est__impacts_reach)   AS Reach
FROM lads
GROUP BY 1,2

UNION ALL

SELECT 'Ladbrokes' AS Brand, date, 'Press' AS Channel, SUM(Press_est__spend) AS Spend, SUM(Press_est__reach)         AS Reach
FROM lads
GROUP BY 1,2

----

UNION ALL

SELECT 'Gala Bingo' AS Brand, date, 'TV'    AS Channel, SUM(TV_est__spend)    AS Spend, SUM(TV_est__impacts_reach)    AS Reach
FROM galabingo
GROUP BY 1,2

UNION ALL

SELECT 'Gala Bingo' AS Brand, date, 'Radio' AS Channel, SUM(Radio_est__spend) AS Spend, SUM(Radio_est__impacts_reach) AS Reach
FROM galabingo
GROUP BY 1,2

UNION ALL

SELECT 'Gala Bingo' AS Brand, date, 'OOH'   AS Channel, SUM(OOH_est__spend)   AS Spend, SUM(OOH_est__impacts_reach)   AS Reach
FROM galabingo
GROUP BY 1,2

UNION ALL

SELECT 'Gala Bingo' AS Brand, date, 'Press' AS Channel, SUM(Press_est__spend) AS Spend, SUM(Press_est__reach)         AS Reach
FROM galabingo
GROUP BY 1,2

----

UNION ALL

SELECT 'Gala Spins' AS Brand, date, 'TV'    AS Channel, SUM(TV_est__spend)    AS Spend, SUM(TV_est__impacts_reach)    AS Reach
FROM galaspins
GROUP BY 1,2

UNION ALL

SELECT 'Gala Spins' AS Brand, date, 'Radio' AS Channel, SUM(Radio_est__spend) AS Spend, SUM(Radio_est__impacts_reach) AS Reach
FROM galaspins
GROUP BY 1,2

UNION ALL

SELECT 'Gala Spins' AS Brand, date, 'OOH'   AS Channel, SUM(OOH_est__spend)   AS Spend, SUM(OOH_est__impacts_reach)   AS Reach
FROM galaspins
GROUP BY 1,2

UNION ALL

SELECT 'Gala Spins' AS Brand, date, 'Press' AS Channel, SUM(Press_est__spend) AS Spend, SUM(Press_est__reach)         AS Reach
FROM galaspins
GROUP BY 1,2

----

UNION ALL

SELECT 'Foxy' AS Brand, date, 'TV'    AS Channel, SUM(TV_est__spend)    AS Spend, SUM(TV_est__impacts_reach)    AS Reach
FROM foxy
GROUP BY 1,2

UNION ALL

SELECT 'Foxy' AS Brand, date, 'Radio' AS Channel, SUM(Radio_est__spend) AS Spend, SUM(Radio_est__impacts_reach) AS Reach
FROM foxy
GROUP BY 1,2

UNION ALL

SELECT 'Foxy' AS Brand, date, 'OOH'   AS Channel, SUM(OOH_est__spend)   AS Spend, SUM(OOH_est__impacts_reach)   AS Reach
FROM foxy
GROUP BY 1,2

UNION ALL

SELECT 'Foxy' AS Brand, date, 'Press' AS Channel, SUM(Press_est__spend) AS Spend, SUM(Press_est__reach)         AS Reach
FROM foxy
GROUP BY 1,2

)

SELECT * FROM uni WHERE DATE  <= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)