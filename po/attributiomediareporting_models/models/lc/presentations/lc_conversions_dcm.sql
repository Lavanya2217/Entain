WITH dcm_import AS(
              SELECT  CASE WHEN REGEXP_CONTAINS(Activity, '(?i)oral')               THEN 'Coral'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Lad' )               THEN 'Ladbrokes'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)FoxyBin|Foxy B')     THEN 'Foxy Bingo'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Foxy')               THEN 'Foxy Casino'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Gala Casino|GalaC')  THEN 'Gala Casino'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Gala Spins')         THEN 'Gala Spins'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Cheeky Bingo')       THEN 'Cheeky Bingo'
                           ELSE 'Gala Bingo' END AS Brand


                     ,CASE WHEN REGEXP_CONTAINS(Activity, '(?i)gistration')                THEN 'Registration'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Bet')                       THEN 'Bet'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)App Purchas|FTD|eposit')    THEN 'Deposit'
                           ELSE 'Other' END AS Conversion
                     ,Activity
                     ,a.Activity_ID
                     ,EXTRACT(DATETIME FROM TIMESTAMP_MICROS(a.Event_time))               AS Activity_Timedate
                     ,EXTRACT(DATE FROM TIMESTAMP_MICROS(a.Event_time))                   AS Activity_Date
                     ,SPLIT(TRIM(CAST(EXTRACT(TIME FROM TIMESTAMP_MICROS(a.Event_time)) AS STRING)), '.')[SAFE_OFFSET(0)]     AS Activity_Time
                     ,a.Campaign_id
                     ,SAFE_CAST(REGEXP_REPLACE(SUBSTR(Other_Data, STRPOS(Other_Data, 'u2'), 12), 'u2=|;', '') AS INT64) AS player_id
                     ,CASE WHEN REGEXP_CONTAINS(Other_data, 'af_revenue')
                            THEN SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(SPLIT(Other_data, '}')[SAFE_OFFSET(0)], 'af_revenue')[SAFE_OFFSET(1)],1,8), r'\..*|[^0-9]' ,'') AS INT64)
                           WHEN REGEXP_CONTAINS(other_data, 'u30') THEN SAFE_CAST(REGEXP_EXTRACT(Other_data, 'u30=([0-9.,]+)') AS FLOAT64)
                           WHEN REGEXP_CONTAINS(other_data, 'u14') THEN SAFE_CAST(REGEXP_EXTRACT(Other_data, 'u14=([0-9.,]+)') AS FLOAT64)
                      END AS dep_value
                     ,CASE WHEN event_sub_type = 'POSTCLICK' THEN 1 ELSE 0 END AS Click_through_Conversions
                     ,CASE WHEN event_sub_type = 'POSTVIEW'  THEN 1 ELSE 0 END AS View_through_Conversions
                     ,EXTRACT(DATETIME FROM TIMESTAMP_MICROS(CAST(Interaction_Time AS INT64))) AS Interaction_Time
                     ,CASE WHEN MAX(REGEXP_EXTRACT(Activity, 'App')) IS NOT NULL THEN 'App' ELSE 'Web' END AS Conv_medium
                     ,CASE WHEN Other_Data LIKE '%ithdraw%' THEN 1 ELSE 0 END AS withdraw
                     ,CASE WHEN REGEXP_CONTAINS(other_data, 'u31')           THEN SPLIT(SPLIT(SPLIT(Other_data, 'u31=')[SAFE_OFFSET(1)], ';')[SAFE_OFFSET(0)], ',')[SAFE_OFFSET(0)]
                           WHEN REGEXP_CONTAINS(LOWER(other_data), 'txnid=') THEN SPLIT(SPLIT(LOWER(other_data), 'txnid=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]
                           END AS transaction_id
                     ,CASE WHEN REGEXP_CONTAINS(other_data, 'u27') THEN SAFE_CAST(REGEXP_EXTRACT(Other_data, 'u27=([0-9.,]+)') AS INT64)
                           WHEN REGEXP_CONTAINS(other_data, '(?i)wm=') THEN SAFE_CAST(SPLIT(SPLIT(LOWER(other_data), 'wm=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)] AS INT64)
                      END AS wm_tracking

              FROM (SELECT distinct * FROM {{ref('lc_dcm_import_dailynew')}}) a
              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13, Other_Data
              ),

    transformed_dcm AS(

              SELECT Brand
                     ,Activity
                     ,Activity_Date AS Date
                     ,EXTRACT(HOUR FROM Activity_Timedate) AS Hour
                     ,Activity_Time AS Event_Time
                     ,'blue' AS ChannelGrouping
                     ,'' AS medium
                     ,'' AS source
                     ,CAST(0 AS INT64) AS VisitId
                     ,Campaign AS Campaign
                     ,CAST(player_id AS STRING)  AS CustomerID
                     ,Conversion
                     ,dep_value
                     ,DATETIME_DIFF(Activity_Timedate, Interaction_Time, DAY)  AS Lag_days
                     ,DATETIME_DIFF(Activity_Timedate, Interaction_Time, HOUR) AS Lag_hours
                     ,MAX(View_through_Conversions          ) AS View_conversion
                     ,MAX(Click_through_Conversions         ) AS Click_conversion
                     ,Conv_medium
                     ,'DCM' AS Dataset
                     ,withdraw
                     ,transaction_id
                     ,wm_tracking


              FROM dcm_import  a
              JOIN {{ source('DCM_UK', 'p_match_table_campaigns_785192') }} c
              ON a.Campaign_id = c.Campaign_id
              WHERE Conversion <> 'Other'

              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,18,19,20,21,22
              ),

    final AS(
              SELECT distinct * EXCEPT(Activity)
                    ,ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Event_Time ORDER BY Lag_days ASC, Lag_hours ASC) AS rank
                    ,CASE WHEN Brand ='Coral' AND NOT campaign LIKE '%|cor|%' AND NOT campaign LIKE '%-cor-%' THEN
                          CASE WHEN campaign LIKE '%|lad|%' OR campaign LIKE '%-lad-%' OR REGEXP_CONTAINS(campaign, '(?i)ladbrokes|lad') THEN 1 END
                          WHEN Brand ='Ladbrokes' AND NOT campaign LIKE '%|lad|%' AND NOT campaign LIKE '%-lad-%' THEN
                          CASE WHEN campaign LIKE '%|cor|%' OR campaign LIKE '%-cor-%' OR REGEXP_CONTAINS(campaign, '(?i)coral') THEN 1 END
                     END AS exclusion
                   ,SUBSTR(SPLIT(REGEXP_REPLACE(Campaign,'ie-','uk-') , '-uk-')[SAFE_OFFSET(1)],1,10) AS cr_date
                   
              FROM transformed_dcm
              WHERE REGEXP_CONTAINS(Brand, '(?i)lad|coral|gala|foxy|cheeky')
               AND (NOT REGEXP_CONTAINS(Campaign,'BidManager_Campaign') OR Campaign IS NULL) AND withdraw <1
             )





SELECT * EXCEPT(newconv, idin, length, creation_date),
         ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, transaction_id, Date, Hour, Minute
                                 ORDER BY Lag_days ASC, Lag_hours ASC, Click_conversion DESC, newconv DESC, creation_date DESC, event_value DESC, idin DESC, length DESC, campaign) AS ranknew
FROM (   SELECT * EXCEPT(rank, dep_value, withdraw, transaction_id, exclusion, wm_tracking, cr_date)
                , '' AS adContent
                , '' AS keyword
                ,CASE WHEN Conversion != 'Registration' THEN dep_value END AS event_value
                ,IF(transaction_id = 'undefined', NULL, REGEXP_REPLACE(REGEXP_REPLACE(transaction_id, '%2F', '/'), '%2C', ',')) AS transaction_id
                ,wm_tracking AS wm_tracking
                ,NULL AS campaignId
                ,CASE WHEN CAST(SUBSTR(Event_Time, 7,2) AS INT64) > 49 THEN CAST(SUBSTR(Event_Time, 4,2) AS INT64) +1 ELSE CAST(SUBSTR(Event_Time, 4,2) AS INT64) END AS Minute
                ,CASE WHEN REGEXP_CONTAINS(campaign, '(?i)c:') THEN 2
                      WHEN Campaign LIKE '%|%' THEN 1 ELSE 0 END AS newconv
                ,CAST(CONCAT(SUBSTR(cr_date, 7,10),SUBSTR(cr_date, 4,2),SUBSTR(cr_date, 1,2)) AS INT64) AS creation_date
                ,CASE WHEN Campaign LIKE '%ID_%' THEN 1 ELSE 0 END AS idin
                ,LENGTH(campaign) AS length

         FROM final
         WHERE exclusion IS NULL
      )