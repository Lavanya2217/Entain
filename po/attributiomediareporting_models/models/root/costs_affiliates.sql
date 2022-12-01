WITH aff AS (
      SELECT 
        FC.Summary_Date AS date_aff,
        CASE WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)bwin|premium')  
               OR REGEXP_CONTAINS(Front_End_Desc,   '(?i)bwin|premium')          THEN 'Bwin'
             WHEN REGEXP_CONTAINS(dp.Front_End_CD, 'ch')                         THEN 'Cheeky Bingo'
             WHEN REGEXP_CONTAINS(dp.Front_End_CD, 'fb')                         THEN 'Foxy Bingo'
             WHEN REGEXP_CONTAINS(dp.Front_End_CD, 'fc')                         THEN 'Foxy Casino'
             WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)sportingbet')           THEN 'SportingBet'
             WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)giocod')                THEN 'Gioco Digitale'
             WHEN REGEXP_CONTAINS(DWH_Brand_Desc,   '(?i)party casino')
               OR (DWH_Brand_Desc = 'party' AND Front_End_Desc = 'PARTYES')      THEN 'Party Casino'
             WHEN REGEXP_CONTAINS(DWH_Brand_Desc, '(?i)party poker')
               OR REGEXP_CONTAINS(Front_End_Desc, '(?i)partypoker|party poker')  THEN 'Party Poker'
             WHEN REGEXP_CONTAINS(Front_End_Desc,   '(?i)galaspins')             THEN 'Gala Spins'
             WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Sportingbet')        THEN 'Sportingbet'
             WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Cashcade')           THEN 'Cashcade'
             WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Borgata')            THEN 'Borgata'
             WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)BetMGM')             THEN 'BetMGM'
             WHEN REGEXP_CONTAINS(ds.Primary_Brand_Cd, '(?i)Cozy')               THEN 'Cozy'
             ELSE Front_End_Desc END AS Brand_aff,
        ACQ.Source_of_Acquisition_Desc AS acquisition_channel,
        db.src_beneficiary_id AS beneficiary_id,
        T.Src_Tracker_Cd AS tracker_id,  
        src_account_id, 
        LOWER(Registration_Country_Cd) AS Country_aff,
        SUM(FC.Estimated_aff_referral_cost_amt_gbp) AS Referral_Cost,
        SUM(FC.Estimated_CPA_cost_amt_gbp) AS IA_CPA_COST,
        SUM(FC.Estimated_MGR_cost_amt_gbp) AS Rev_Share
      
      FROM {{ source('DWPRODVIEWSBI', 'DAILY_PLAYER_AFFILIATE_COMMISSION') }}  FC
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_BENEFICIARY') }}  DB 
         ON DB.src_beneficiary_id=FC.src_beneficiary_id
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }}  DP 
         ON FC.Player_Id=DP.Player_Id
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_ACCOUNT') }}  dpa 
         ON FC.Player_Id=DPA.Player_Id
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SKIN') }}  dS 
         ON FC.Wh_Skin_Id=dS.Wh_Skin_Id
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }} fe
         ON FE.front_end_cd = dp.front_end_cd
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SOURCE_OF_ACQUISITION') }} ACQ 
         ON ACQ.Wh_Source_Of_Acquisition_Id=DB.Wh_Source_Of_Acquisition_Id
      LEFT JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_TRACKER') }}  T 
         ON T.Wh_Tracker_Id=FC.Wh_Tracker_Id
      WHERE DPA.Internal_Ind = 0 
       -- AND FC.Wh_Skin_Id IN (544,541,542,543,565,566,567,568)
        AND dS.ACTIVE_YN='Y'
        AND db.Status_Cd NOT IN ('Z', 'IN', 'T' )
        AND FC.Summary_Date >= '2021-01-01'
        AND (ABS(FC.Estimated_aff_referral_cost_amt_gbp) > 0 OR ABS(FC.Estimated_CPA_cost_amt_gbp) > 0 OR ABS(FC.Estimated_MGR_cost_amt_gbp) > 0)
      GROUP BY 1,2,3,4,5,6,7
     )


SELECT *, IF(IA_CPA_COST<0,0, IA_CPA_COST) + Rev_Share AS Costs
FROM aff
WHERE REGEXP_CONTAINS(acquisition_channel, '(?i)aff')
