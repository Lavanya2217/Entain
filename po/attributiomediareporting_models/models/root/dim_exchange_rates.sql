WITH fx AS(
            SELECT NULL AS Index
                  ,TRIM(From_Currency_CD) AS Currency
                  ,CONCAT(TRIM(From_Currency_CD), TRIM(tO_Currency_CD)) AS Conversion
                  ,calendar_Date AS Date
                  ,Rate AS Exchange_rate
                  
            FROM {{ source('DWPRODVIEWSBI', 'DAILY_FX_RATE') }} ex
            WHERE ex.calendar_Date >= '2021-01-01'  
              AND ex.wh_time_zone_id = 1
              AND ex.platform_cd = 2
              AND ex.txn_group_cd = 'nocharge'
              AND (REGEXP_CONTAINS(ex.to_Currency_Cd, '(?i)GBP') OR ( REGEXP_CONTAINS(ex.to_Currency_Cd, '(?i)USD') AND REGEXP_CONTAINS(ex.to_Currency_Cd, '(?i)EUR') ))
            )  
  


SELECT * REPLACE(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS date)
FROM fx WHERE Date = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY) 

UNION ALL

SELECT * FROM fx 