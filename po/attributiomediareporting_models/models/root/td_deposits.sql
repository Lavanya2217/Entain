select *, row_number() over (partition by Brand order by transaction_ts, src_account_id, player_id) dep_id
from
    (
    SELECT
        TIMESTAMP(extract(date from txn_timestamp)||' '||extract(time from txn_timestamp)||' '||'Europe/Gibraltar') as transaction_ts,
        date(TIMESTAMP(extract(date from txn_timestamp)||' '||extract(time from txn_timestamp)||' '||'Europe/Gibraltar')) as transaction_date,
        CASE
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)bwin|premium')
            OR REGEXP_CONTAINS(Front_End_Desc,   '(?i)bwin|premium')        THEN 'Bwin'
        WHEN REGEXP_CONTAINS(fe.Front_End_CD, 'ch')                         THEN 'Cheeky Bingo'
        WHEN REGEXP_CONTAINS(fe.Front_End_CD, 'fb')                         THEN 'Foxy Bingo'
        WHEN REGEXP_CONTAINS(fe.Front_End_CD, 'fc')                         THEN 'Foxy Casino'
        WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)sportingbet')           THEN 'SportingBet'
        WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)giocod')                THEN 'Gioco Digitale'
        WHEN REGEXP_CONTAINS(DWH_Brand_Desc,   '(?i)party casino')
          OR (DWH_Brand_Desc = 'party' and Front_End_Desc = 'PARTYES')      THEN 'Party Casino'
        WHEN REGEXP_CONTAINS(DWH_Brand_Desc, '(?i)party poker')
          OR REGEXP_CONTAINS(Front_End_Desc, '(?i)partypoker|party poker')  THEN 'Party Poker'
        WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)galaspins')             THEN 'Gala Spins'
        WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)ladbrokes')             THEN 'Ladbrokes'
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)Sportingbet')        THEN 'Sportingbet'
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)Cashcade')           THEN 'Cashcade'
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)Borgata')            THEN 'Borgata'
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)BetMGM')             THEN 'BetMGM'
        WHEN REGEXP_CONTAINS(fc.Primary_Brand_Cd, '(?i)Cozy')               THEN 'Cozy'
        ELSE Front_End_Desc
        END AS Brand,
        DWH_Brand_Desc,
        Front_End_Desc,
        trim(src_account_id) as src_account_id,
        fc.player_id as player_id,
        deposit_amt_gbp
    FROM {{ source('DWPRODVIEWSMSTR', 'F_PLAYER_CASHIER_TXN') }} fc
    LEFT JOIN
    (SELECT * FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER') }} WHERE Registration_Date<=current_date-2
    UNION ALL 
     SELECT * FROM {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }} WHERE Registration_Date>current_date-2) dp
    ON
        FC.Player_Id=DP.Player_Id
    LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_ACCOUNT') }} dpa
    ON
        FC.Player_Id=DPA.Player_Id
    LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }} ds
    ON
        FC.Wh_Skin_Id=dS.Wh_Skin_Id
    LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
    ON
        FE.front_end_cd = dp.front_end_cd
    WHERE
        fc.deposit_qty>0
        AND src_account_id IS NOT NULL
    AND  FC.txn_date >= '2021-01-01'
    )
--WHERE
  --(regexp_contains(brand, '(?i)Coral|Ladbrokes') and transaction_date >= '2020-01-20') or transaction_date >= '2021-01-01'