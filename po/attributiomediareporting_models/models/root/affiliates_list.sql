WITH trackers AS(
    SELECT dt.src_tracker_cd                AS Tracker_id
           ,dsoa.Source_of_Acquisition_Desc
           ,dbS.Affiliate_Source_Desc       AS Beneficiary_name
           ,db.Login_Name_Txt
           ,db.Src_Beneficiary_Id           AS Beneficiary_Id
           ,db.Wh_Affiliate_Id              AS Wh_Affiliate_Id
           ,dfe.front_end_business_desc     AS Brand
           ,db.Status_Cd
           
    FROM  {{ source('DWPRODVIEWSMSTR', 'DIM_TRACKER') }} dt
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_ACCOUNT') }}          dpa ON (dpa.wh_tracker_id = dt.wh_tracker_id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_DEAL') }}                     dd ON (dd.wh_deal_id = dt.wh_deal_id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_BENEFICIARY') }}              db ON (db.Wh_Affiliate_Id = dd.Wh_Affiliate_Id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_BENEFICIARY_AGENT') }}      dba1 ON (dba1.Agent_ID = db.Agent_ID)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_BENEFICIARY_SOURCE') }}      dbs ON (db.Wh_Affiliate_Source_Id = dbS.Wh_Affiliate_Source_Id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_SOURCE_OF_ACQUISITION') }}  dsoa ON (dsoa.Wh_Source_of_Acquisition_Id = db.Wh_Source_of_Acquisition_Id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_PLAYER_NON_US') }}            dp ON (dpa.player_id = dp.player_id)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_FRONT_END') }}               dfe ON (dfe.front_end_cd = dp.front_end_cd)
    LEFT OUTER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_BENEFICIARY_STATUS') }}      dbt ON (dbt.Status_Cd = db.Status_Cd)
    
    WHERE 1=1 
    AND (NOT REGEXP_CONTAINS(dfe.front_end_business_desc, '(?i)Ladbrokes|Coral') or dfe.front_end_business_desc IS NULL) 
    --AND dp.front_end_cd NOT in ('ld','cl')
    AND REGEXP_CONTAINS(dsoa.Source_of_Acquisition_Desc, '(?i)aff') 
    AND db.Status_Cd NOT IN ('Z', 'IN', 'T' )
    GROUP BY 1,2,3,4,5,6,7,8)
    


SELECT *
FROM trackers