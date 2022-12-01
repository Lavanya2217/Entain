WITH     ftds AS( SELECT * FROM {{ref('td_ftds')}} )

  ,pNGR_DAY_1 AS( SELECT *, ROW_NUMBER () OVER (PARTITION BY player_id ORDER BY Snapshot_date) AS rank
                  FROM( SELECT f.* EXCEPT(src_account_ID)
                               ,SAFE_CAST(src_account_ID AS INT64) AS Customer_id
                               ,model_description_id
                               ,model_score AS Value
                               ,IFNULL(Snapshot_date, FTD_DATE_ID) AS Snapshot_date 
                        FROM ftds f
                        LEFT JOIN (SELECT * FROM {{ source('DWPRODVIEWSPPMBI', 'MODEL_DAILY_SCORES') }} 
                                   WHERE model_description_id = 9
                                     AND Snapshot_date >= '2020-01-01') a
                               ON f.player_id = a.player_id
                        
                  ) WHERE 1=1
                 ) 
,pNGR_DAY_21 AS( SELECT f.* EXCEPT(src_account_ID)
                        ,SAFE_CAST(src_account_ID AS INT64) AS Customer_id
                        ,model_description_id
                        ,model_score AS Value
                        ,IFNULL(Snapshot_date, FTD_DATE_ID) AS Snapshot_date FROM ftds f
                  LEFT JOIN {{ source('DWPRODVIEWSPPMBI', 'MODEL_DAILY_SCORES') }} a
                  ON f.player_id = a.player_id
                  WHERE model_description_id = 69
                  AND Snapshot_date >= '2020-01-01'
                  ) 

  ,CLV_final AS( SELECT FTD_DATE_ID, Player_id, Business_Unit, FTD_DATETIME, First_Deposit_Date, First_Deposit_Amt_GBP, Brand, Customer_id,
                        SUM(IF(model_description_id= 9, ROUND(value * CAST(Exchange_rate AS FLOAT64),2),0)) AS Value,
                        SUM(IF(model_description_id=69, ROUND(value * CAST(Exchange_rate AS FLOAT64),2),0)) AS Value_20
                 FROM(
                        SELECT * EXCEPT(Snapshot_date, rank)
                        FROM pNGR_DAY_1 WHERE Rank =1
                        UNION ALL
                        SELECT * EXCEPT(Snapshot_date)
                        FROM pNGR_DAY_21)
                        
                 LEFT JOIN {{ref('dim_exchange_rates')}} xr
                 ON 'EUR' = xr.currency AND CAST(First_Deposit_Date AS STRING) = CAST(xr.Date AS STRING)
                 GROUP BY 1,2,3,4,5,6,7,8
                 )

SELECT f.*, Avg_value_0
FROM CLV_final f
LEFT JOIN( SELECT Brand, FTD_DATE_ID, ROUND(AVG(Value),2) Avg_value_0
           FROM CLV_final
           WHERE Value <> 0
           GROUP BY 1,2) a
USING(brand, FTD_DATE_ID)

