WITH td AS( SELECT *, ROW_NUMBER() OVER (PARTITION BY src_account_ID, Model_Description_Id ORDER BY diff) AS rank
            FROM (SELECT *, DATE_DIFF(pn.Snapshot_Date, p.FTD_DATE_ID, DAY) AS diff
                  FROM (SELECT * FROM {{ source('pNGR', 'pNGR_0') }} 
                        WHERE EXTRACT(YEAR FROM Snapshot_Date) >2020 
                      UNION ALL
                        SELECT * FROM {{ source('pNGR', 'pNGR_21') }} ) pn
                  FULL OUTER JOIN {{ref('lc_td_ftds')}} p
                  USING(Player_Id))                                   
           )

    ,final AS(

              SELECT *
              FROM(SELECT DISTINCT
                       CASE WHEN Brand_seq =   4  THEN 'Coral'
                            WHEN Brand_seq =  14  THEN 'Ladbrokes'
                            WHEN Brand_seq =   2  THEN 'Gala Casino'
                            WHEN Brand_seq =   3  THEN 'Gala Bingo'
                            WHEN Brand_seq = 101  THEN 'Gala Spins'
                            END AS Brand
                       ,customer_ID
                       ,SAFE_CAST(Old_IMS_Customer_ID AS INT64) AS Old_IMS_Customer_ID
                       ,CAST(CONCAT(SUBSTR(CAST(FTD_DATE_ID AS STRING),1,4), '-', SUBSTR(CAST(FTD_DATE_ID AS STRING),5,2), '-',
                             SUBSTR(CAST(FTD_DATE_ID AS STRING),7,2)) AS DATE) AS FTD_DATE_ID
                       ,SUM(CASE WHEN PredictionNumber = 0  THEN value ELSE 0 END) AS Value_0
                       ,SUM(CASE WHEN PredictionNumber = 20 THEN value ELSE 0 END) AS Value_20

                FROM {{ source('pNGR_old', 'pngr_lc') }} 
                WHERE PredictionNumber IN (0,20) 
                GROUP BY 1,2,3,4
                )
           WHERE (Brand IN ('Coral', 'Ladbrokes') AND  ftd_date_id < '2021-01-01')


         UNION ALL

           SELECT DISTINCT Brand
                  ,CAST( Src_Account_Id	 AS INT64) AS Customer_id
                  ,NULL AS Old_IMS_Customer_ID
                  ,FTD_DATE_ID
                  ,SUM(CASE WHEN Model_Description_Id = 82 THEN Model_Score ELSE 0 END) AS Value_0
                  ,SUM(CASE WHEN Model_Description_Id = 91 THEN Model_Score ELSE 0 END) AS Value_20
           FROM td
           WHERE rank = 1
           GROUP BY 1,2,3,4
          )


SELECT f.*, Avg_value_0
FROM final f
LEFT JOIN( SELECT Brand, FTD_DATE_ID, ROUND(AVG(Value_0),2) Avg_value_0
           FROM FINAL WHERE ABS(Value_0) >0
           GROUP BY 1,2) a

USING(brand, FTD_DATE_ID)

