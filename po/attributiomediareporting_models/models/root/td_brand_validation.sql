SELECT CAST(src_account_id AS INT64) AS account_id
       , CASE WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)bwin|premium')  
                OR REGEXP_CONTAINS(Front_End_Desc,   '(?i)bwin|premium')          THEN 'Bwin'
              WHEN REGEXP_CONTAINS(dpa.Front_End_CD, 'ch')                        THEN 'Cheeky Bingo'
              WHEN REGEXP_CONTAINS(dpa.Front_End_CD, 'fb')                        THEN 'Foxy Bingo'
              WHEN REGEXP_CONTAINS(dpa.Front_End_CD, 'fc')                        THEN 'Foxy Casino'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)sportingbet')           THEN 'SportingBet'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)giocod')                THEN 'Gioco Digitale'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)partypoker|party poke') THEN 'Party Poker'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)party')                 
                OR REGEXP_CONTAINS(DWH_Brand_Desc,   '(?i)party casino')          THEN 'Party Casino'
              WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)galaspins')             THEN 'Gala Spins'
              WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Sportingbet')        THEN 'Sportingbet'
              WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Cashcade')           THEN 'Cashcade'
              WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Borgata')            THEN 'Borgata'
              WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)BetMGM')             THEN 'BetMGM'
              WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Cozy')               THEN 'Cozy'
              ELSE Front_End_Desc END AS Brand
              


FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }} dpa
LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }} ds
       ON  ds.wh_skin_id = dpa.registration_wh_skin_id       
LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
       ON  fe.front_end_cd = dpa.front_end_cd