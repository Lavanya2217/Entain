WITH dcm_import AS(
              SELECT  CASE WHEN REGEXP_CONTAINS(activity, '(?i)sportingbet') THEN 'Sportingbet'
                           WHEN REGEXP_CONTAINS(activity, '(?i)partycasino') THEN 'Party Casino'
                           WHEN REGEXP_CONTAINS(activity, '(?i)partypoker')  THEN 'Party Poker'
                           WHEN REGEXP_CONTAINS(activity, '(?i)vistabet')    THEN 'Vistabet'
                           WHEN REGEXP_CONTAINS(activity, '(?i)bwin|af_FTD') THEN 'Bwin'
                           END AS Brand

                     ,CASE WHEN REGEXP_CONTAINS(Activity, '(?i)gistration|reg')            THEN 'Registration'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)App Purchas|FTD|eposit|SD') THEN 'Deposit'
                           WHEN REGEXP_CONTAINS(Activity, '(?i)Bet')                       THEN 'Bet'
                           ELSE 'Other' END AS Conversion
                     ,Activity
                     ,a.Activity_ID
                     ,EXTRACT(DATETIME FROM TIMESTAMP_MICROS(a.Event_time))               AS Activity_Timedate
                     ,EXTRACT(DATE FROM TIMESTAMP_MICROS(a.Event_time))                   AS Activity_Date
                     ,SPLIT(TRIM(CAST(EXTRACT(TIME FROM TIMESTAMP_MICROS(a.Event_time)) AS STRING)), '.')[SAFE_OFFSET(0)]     AS Activity_Time
                     ,a.Campaign_id
                     ,SAFE_CAST(REGEXP_REPLACE(SUBSTR(Other_Data, STRPOS(Other_Data, 'u14'), 13), 'u14=|;', '') AS INT64) AS player_id
                     ,SAFE_CAST(SPLIT(SPLIT(other_data, 'u15=')[SAFE_OFFSET(1)], ';')[SAFE_OFFSET(0)] AS FLOAT64) AS event_value
                     ,SPLIT(SPLIT(other_data, 'u16=')[SAFE_OFFSET(1)], ';')[SAFE_OFFSET(0)]                AS currency
                     ,CASE WHEN event_sub_type = 'POSTCLICK' THEN 1 ELSE 0 END                             AS Click_through_Conversions
                     ,CASE WHEN event_sub_type = 'POSTVIEW'  THEN 1 ELSE 0 END                             AS View_through_Conversions
                     ,EXTRACT(DATETIME FROM TIMESTAMP_MICROS(CAST(Interaction_Time AS INT64)))             AS Interaction_Time
                     ,CASE WHEN MAX(REGEXP_EXTRACT(Activity, 'App')) IS NOT NULL THEN 'App' ELSE 'Web' END AS Conv_medium
                     ,CASE WHEN Other_Data LIKE '%ithdraw%' THEN 1 ELSE 0 END                              AS withdraw
                     ,SPLIT(SPLIT(other_data, 'https://')[SAFE_OFFSET(1)], '/')[SAFE_OFFSET(0)]            AS website
                     ,SPLIT(SPLIT(LOWER(other_data), 'txnid=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]       AS transaction_id
                     ,SAFE_CAST(IFNULL(SPLIT(SPLIT(LOWER(other_data), 'wm=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)], site_id_dcm) AS INT64) AS wm_tracking
                     ,CASE WHEN country_code ="" THEN NULL ELSE country_code END                           AS country
                     ,site_DCM

              FROM (SELECT distinct * 
                    FROM  {{ref('gvc_dcm_import_dailynew')}}
                    WHERE activity_id NOT IN ("10987297", "10983793", "10983796", "7734412", "7697111", "7727914")
                       OR (activity_id  IN ("10987297", "10983793", "10983796", "7734412", "7697111", "7727914") AND REGEXP_CONTAINS(Activity, r'\['))
                    ) a

              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21
              ),

    transformed_dcm AS(

              SELECT Brand
                     ,Activity
                     ,Activity_Date AS Date
                     ,EXTRACT(HOUR FROM Activity_Timedate) AS Hour
                     ,TIME_TRUNC(EXTRACT(TIME FROM SAFE_CAST(Activity_Timedate AS TIMESTAMP)), SECOND) AS Event_time 
                     ,'blue' AS ChannelGrouping
                     ,'' AS medium
                     ,CASE WHEN REGEXP_CONTAINS(campaign, '(?i)TTD')             THEN 'TradeDesk'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)forza')           THEN 'ForzaApp'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)evolutionpeople') THEN 'EvolutionPeople'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)gazzetta')        THEN 'Gazzetta'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)internal|test')   THEN 'DCM'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)taboola')         THEN 'Taboola'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)verizon')         THEN 'Verizon'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)amazon')          THEN 'Amazon'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)_DSP_')           THEN 'DSP'
                           WHEN REGEXP_CONTAINS(campaign, '(?i)facebook')        THEN 'Facebook'
                           WHEN LOWER(campaign) LIKE '%e2_%'                     THEN 'E2 Online'
                           WHEN LOWER(campaign) LIKE '%+%'                       THEN SPLIT(campaign, 'ACQ_DIRECT+')[SAFE_OFFSET(1)]     
                           WHEN REGEXP_CONTAINS(campaign, '(?i)c:')              THEN SPLIT(REGEXP_REPLACE(campaign, '|', '-'), '-')[SAFE_OFFSET(1)]
                           ELSE Site_DCM                                                                 
                      END AS source
                     ,CAST(0 AS INT64) AS VisitId
                     ,Campaign AS Campaign
                     ,CAST(player_id AS STRING)  AS CustomerID
                     ,Conversion
                     ,DATETIME_DIFF(Activity_Timedate, Interaction_Time, DAY)  AS Lag_days
                     ,DATETIME_DIFF(Activity_Timedate, Interaction_Time, HOUR) AS Lag_hours
                     ,MAX(View_through_Conversions          ) AS View_conversion
                     ,MAX(Click_through_Conversions         ) AS Click_conversion
                     ,Conv_medium
                     ,'DCM'       AS Dataset
                     ,''          AS adcontent
                     ,''          AS keyword
                     ,CASE WHEN Conversion ='Bet' THEN event_value/100 ELSE event_value END AS event_value
                     ,currency
                     ,transaction_id
                     ,wm_tracking
                     ,NULL                   AS CampaignId
                     ,country
                     ,REGEXP_REPLACE(SUBSTR(website, STRPOS(website,'.')+1, LENGTH(website)), 'www.|sports.|10004', '')                     AS website
                     ,withdraw


              FROM dcm_import  a
              JOIN (SELECT distinct * FROM {{ source('DCM_GVC', 'p_match_table_campaigns_8807') }} )   c
              ON a.Campaign_id = c.Campaign_id
              WHERE Conversion <> 'Other' 
              GROUP BY 1,2,3,4,5,6,7,8,10,11,12,13,14,17,18,21,22,23,24,26,27,28 
              ),


final AS(
          SELECT * EXCEPT(Activity) 
                 , ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Event_Time ORDER BY Lag_days ASC, Lag_hours ASC) AS rank
          FROM transformed_dcm
          WHERE REGEXP_CONTAINS(Brand, '(?i)party|Bwin') AND (NOT REGEXP_CONTAINS(campaign, '(?i)sptbet|sportingbet') OR campaign IS NULL)
        
       UNION ALL
       
         SELECT * EXCEPT(transaction_id, wm_tracking, CampaignId,minute, ranknew)
                  REPLACE(EXTRACT(HOUR FROM Event_Time) AS Hour)
               ,'GBP'          AS currency
               ,transaction_id AS transaction_id
               ,wm_tracking    AS wm_tracking
               ,NULL           AS CampaignId
               ,'UK'           AS country
               ,CONCAT(LOWER(REGEXP_REPLACE(Brand, ' ', '')), '.com') AS website
               ,0 AS withdraw
               ,1 AS rank
               
         FROM (SELECT * REPLACE(CAST(Event_Time AS TIME) AS Event_Time)
               FROM {{ref('lc_conversions_dcm')}} 
               WHERE NOT REGEXP_CONTAINS(Brand, '(?i)coral|lad')
                 AND Campaign NOT LIKE '%|cor|%' AND Campaign NOT LIKE '%|lad|%'
                 AND NOT REGEXP_CONTAINS(Campaign, '(?i)ladb|coral|-cor-|-lad-|BidManager')
                 AND Date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
               )
           )
          
SELECT * EXCEPT(newconv, idin, length, creation_date)
         REPLACE(CASE WHEN LENGTH(source)<4 THEN UPPER(source) ELSE source END AS source)
         ,ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, transaction_id, Date, Hour, Minute
                                  ORDER BY Lag_days ASC, Lag_hours ASC, Click_conversion DESC, newconv DESC, creation_date DESC, event_value DESC, idin DESC, length DESC, Campaign) AS ranknew

FROM (   SELECT * EXCEPT(rank, withdraw, cr_date)
                  REPLACE( IF(transaction_id = 'undefined', NULL, REGEXP_REPLACE(REGEXP_REPLACE(transaction_id, '%2F', '/'), '%2C', ',')) AS transaction_id)
                ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute
                ,IF(Campaign LIKE '%|%', 1, 0) AS newconv
                ,SAFE_CAST(CONCAT(SUBSTR(cr_date, 7,10),SUBSTR(cr_date, 4,2),SUBSTR(cr_date, 1,2)) AS INT64) AS creation_date
                ,CASE WHEN Campaign LIKE '%ID_%' THEN 1 ELSE 0 END AS idin
                ,LENGTH(Campaign) AS length

         FROM (SELECT distinct *,
                      CONCAT(SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(5)], '-',SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(6)],
                         '-',SPLIT(REGEXP_REPLACE(Campaign, '-', '|'), '|')[SAFE_OFFSET(7)]) AS cr_date
               FROM final
               WHERE (NOT REGEXP_CONTAINS(Campaign,'BidManager_Campaign|20982|61849|22778') OR Campaign IS NULL)
                 AND withdraw <1)
         WHERE CustomerID IS NOT NULL)         

