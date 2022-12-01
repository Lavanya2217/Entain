SELECT * EXCEPT(Front_End_Desc, Primary_Brand_Cd, Front_End_CD, DWH_Brand_Desc), 
         CASE WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)bwin|premium')  
                OR REGEXP_CONTAINS(Front_End_Desc,   '(?i)bwin|premium')          THEN 'Bwin'
              WHEN REGEXP_CONTAINS(Front_End_CD, 'ch')                            THEN 'Cheeky Bingo'
              WHEN REGEXP_CONTAINS(Front_End_CD, 'fb')                            THEN 'Foxy Bingo'
              WHEN REGEXP_CONTAINS(Front_End_CD, 'fc')                            THEN 'Foxy Casino'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)sportingbet')           THEN 'SportingBet'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)giocod')                THEN 'Gioco Digitale'
              WHEN REGEXP_CONTAINS(DWH_Brand_Desc,   '(?i)party casino')
                OR (DWH_Brand_Desc = 'party' AND Front_End_Desc = 'PARTYES')      THEN 'Party Casino'
              WHEN REGEXP_CONTAINS(DWH_Brand_Desc, '(?i)party poker')
                OR REGEXP_CONTAINS(Front_End_Desc, '(?i)partypoker|party poker')  THEN 'Party Poker'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)galaspins')             THEN 'Gala Spins'
              WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)Sportingbet')           THEN 'Sportingbet'
              WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)Cashcade')              THEN 'Cashcade'
              WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)Borgata')               THEN 'Borgata'
              WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)BetMGM')                THEN 'BetMGM'
              WHEN REGEXP_CONTAINS(Primary_Brand_Cd, '(?i)Cozy')                  THEN 'Cozy'
              ELSE Front_End_Desc END AS Brand
FROM(
 SELECT  
         DATE(TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR))  AS FTD_DATE_ID,
         dpa.Player_id,
         Front_End_Desc,
         ds.Primary_Brand_Cd,
         dpa.Front_End_CD,
         DWH_Brand_Desc,
         ds.Business_Unit,
         TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR)        AS FTD_DATETIME,
         dpa.src_account_ID,
         First_Deposit_Date,
         First_Deposit_Amt_GBP         

    FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER') }}  dpa
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }}  fe
      ON  fe.front_end_cd = dpa.front_end_cd
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_FIRST_LAST') }}  dpafl
      ON  dpafl.player_id = dpa.player_id
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }} ds
      ON  ds.wh_skin_id = dpa.registration_wh_skin_id 
    JOIN {{ source('DWPRODVIEWSMSTR', 'FD_PLAYER_ACQUISITION') }} paq
      ON  paq.player_id = dpa.player_id AND summary_date = First_Deposit_Date

   WHERE 1=1
     AND NOT REGEXP_CONTAINS(Front_End_Desc, '(?i)oral|adbrokes')
     AND paq.new_depositor_qty>0 
     AND EXTRACT(YEAR FROM First_Deposit_Date) >= 2021
  
UNION ALL

  SELECT  DATE(TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR))  AS FTD_DATE_ID,
          ftd.Player_id,
          Front_End_Desc,
          ds.Primary_Brand_Cd,
          dpa.Front_End_CD,
          DWH_Brand_Desc,
          ds.Business_Unit,
          TIMESTAMP_ADD(First_Deposit_Timestamp, INTERVAL -2 HOUR)        AS FTD_DATETIME,
          ftd.src_account_ID,
          DATE(First_Deposit_Timestamp) AS First_Deposit_Date,
          First_Deposit_Amt_GBP         

    FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }} dpa
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
      ON  fe.front_end_cd = dpa.front_end_cd
    JOIN {{ source('DWPRODVIEWSMSTR', 'FTD') }} ftd
      ON  ftd.player_id = dpa.player_id
    JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }}  ds
      ON  ds.wh_skin_id = dpa.registration_wh_skin_id
 
 WHERE NOT REGEXP_CONTAINS(Front_End_Desc, '(?i)oral|adbrokes')
   AND ftd.FTD = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)

)


