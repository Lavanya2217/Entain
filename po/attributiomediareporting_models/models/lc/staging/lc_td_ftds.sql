SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(BRAND, '(?i)galaspins') THEN CONCAT('Gala ', SPLIT(brand, 'Gala')[SAFE_OFFSET(1)]) ELSE Brand END AS Brand)
FROM(
 SELECT  dpa.Player_id,
         Front_End_Desc AS Brand,
         DATE(TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR))  AS FTD_DATE_ID,
         dpa.src_account_ID

    FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER') }} dpa
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
      ON  fe.front_end_cd = dpa.front_end_cd
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_FIRST_LAST') }} dpafl
      ON  dpafl.player_id = dpa.player_id

   WHERE 1=1
     AND REGEXP_CONTAINS(Front_End_Desc, '(?i)oral|adbrokes|gala')
     AND EXTRACT(YEAR FROM First_Deposit_Date) >= 2020
     
UNION ALL

  SELECT  dpa.Player_id,
         Front_End_Desc AS Brand,
         DATE(TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR))  AS FTD_DATE_ID,
         dpa.src_account_ID        

    FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }} dpa
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
      ON  fe.front_end_cd = dpa.front_end_cd
    JOIN {{ source('DWPRODVIEWSMSTR', 'FTD') }} ftd
      ON  ftd.player_id = dpa.player_id
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }} ds
      ON  ds.wh_skin_id = dpa.registration_wh_skin_id


 WHERE REGEXP_CONTAINS(Front_End_Desc, '(?i)oral|adbrokes|gala')
   AND ftd.FTD = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)    
          
)         
    
 