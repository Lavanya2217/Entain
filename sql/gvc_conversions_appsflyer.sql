WITH
af AS(
         SELECT a.* EXCEPT(bundle_id)
                    REPLACE(CASE WHEN NOT REGEXP_CONTAINS(Campaign, 'c:') AND REGEXP_CONTAINS(Campaign_name, 'c:') THEN Campaign_name ELSE Campaign END AS Campaign,
                            SAFE_CAST(campaignid AS INT64) AS campaignid)
                   ,CASE WHEN REGEXP_CONTAINS(bundle_id, 'bwin.de') THEN 'de'
                         WHEN REGEXP_CONTAINS(bundle_id, 'de') AND REGEXP_CONTAINS(bundle_id, 'premium') THEN 'premium.de'
                         WHEN LENGTH(SPLIT(bundle_id, '.')[SAFE_OFFSET(0)]) = 2 THEN SPLIT(bundle_id, '.')[SAFE_OFFSET(0)]
                         WHEN LENGTH(SPLIT(bundle_id, '.')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(bundle_id, '.')) - 1)]) = 2
                         THEN SPLIT(bundle_id, '.')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(bundle_id, '.')) - 1)]
                         WHEN REGEXP_CONTAINS(bundle_id,'sports.bwin.de2') THEN 'de'
                         WHEN LENGTH(SPLIT(REGEXP_REPLACE(bundle_id, 'bwin.',''), '.')[SAFE_OFFSET(1)]) = 2
                         THEN SPLIT(REGEXP_REPLACE(bundle_id, 'bwin.',''), '.')[SAFE_OFFSET(1)]
                         WHEN LENGTH(SPLIT(REGEXP_REPLACE(bundle_id, 'com.bwinlabs.betdroid_|com.bwinlabs.bwin_',''), '-')[SAFE_OFFSET(0)]) = 2
                         THEN SPLIT(REGEXP_REPLACE(bundle_id, 'com.bwinlabs.betdroid_|com.bwinlabs.bwin_',''), '-')[SAFE_OFFSET(0)]
                         ELSE 'com' END AS website
         FROM(
           SELECT CASE WHEN REGEXP_CONTAINS(bundle_id, '(?i)ladbrokes.pt')            THEN 'Other'
                       WHEN (REGEXP_CONTAINS(app_name, '(?i)gala') AND REGEXP_CONTAINS(app_name, '(?i)casino'))
                         OR (REGEXP_CONTAINS(bundle_id,'(?i)gala') AND REGEXP_CONTAINS(bundle_id, '(?i)casino'))
                         OR REGEXP_CONTAINS(app_name, '(?i)Gala Casino' )             THEN 'Gala Casino'
                       WHEN (REGEXP_CONTAINS(app_name,'(?i)gala') AND REGEXP_CONTAINS(app_name, '(?i)spins'))
                         OR REGEXP_CONTAINS(app_name, '(?i)Gala Spins'  )             THEN 'Gala Spins'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)Gala Bingo'  )             THEN 'Gala Bingo'
                       WHEN REGEXP_CONTAINS(bundle_id,'(?i)ladbrokes'   )
                         OR REGEXP_CONTAINS(app_name, '(?i)ladbrokes'   )             THEN 'Ladbrokes'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)coral'       )             THEN 'Coral'

                       WHEN REGEXP_CONTAINS(app_name, '(?i)Foxy Bingo' )              THEN 'Foxy Bingo'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)Foxy')                     THEN 'Foxy Casino'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)PartyCasino|partyslots')   THEN 'Party Casino'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)partypoker' )              THEN 'Party Poker'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)Gioco Digitale')           THEN 'Gioco Digitale'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)Gamebookers')              THEN 'Gamebookers'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)Sportingbet')              THEN 'Sportingbet'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)slotsclub')                THEN 'Slotsclub'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)borgata')                  THEN 'Borgata'
                       WHEN REGEXP_CONTAINS(bundle_id, '(?i)mgm')
                         OR REGEXP_CONTAINS(app_name, '(?i)mgm|liquidsportsbook')     THEN 'MGM'
                       WHEN REGEXP_CONTAINS(bundle_id,'(?i)bwin|bpremium|de.premium')
                         OR REGEXP_CONTAINS(app_name, '(?i)bwin|bpremium|de.premium') THEN 'Bwin'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)New Jersey|Party Casino NJ|PartyCasino NJ|usnjpoker.casino') THEN 'PartyCasino NJ'
                       WHEN REGEXP_CONTAINS(app_name, '(?i)New Jersey|Party Casino NJ|PartyCasino NJ|usnjpoker')        THEN 'PartyPoker NJ'
                   END AS Brand
                  ,EXTRACT(DATE FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Date
                  ,EXTRACT(HOUR FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Hour
                  ,EXTRACT(TIME FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Event_time
                  ,'blue' AS ChannelGrouping
                  ,CASE WHEN REGEXP_CONTAINS(media_source, '_int|troop') AND NOT REGEXP_CONTAINS(media_source, 'google') THEN 'disp' ELSE IF(af_prt = 'null', '', af_prt) END AS medium
                  ,CASE WHEN media_source = 'Apple Search Ads'  THEN 'Apple_Ads'
                        WHEN media_source = 'Facebook Ads'      THEN 'Facebook'
                        WHEN media_source = 'Twitter'           THEN 'Twitter'
                        WHEN media_source = 'googleadwords_int' THEN 'Google_Ads'
                        WHEN REGEXP_CONTAINS(media_source, '_int|troop|TV'  ) THEN REGEXP_REPLACE(media_source, '_int', '')
                        WHEN REGEXP_CONTAINS(media_source, 'Coral|Lads|Gala') THEN 'internal'
                        ELSE media_source END AS source
               -- ,CASE WHEN device_category = 'tablet' THEN 'tablet'
               --       ELSE 'mobile' END AS device
                  ,CAST(0 AS INT64) AS VisitId
                  ,CASE WHEN REGEXP_CONTAINS(media_source, 'Coral|Lads|Gala') THEN REGEXP_REPLACE(media_source, '(?i)CoralS|LadsS|GalaS|galab|galac', '')
                        ELSE Campaign END AS Campaign
                  ,CASE WHEN customer_user_id != 'null' THEN CAST(customer_user_id AS STRING)
                        WHEN SAFE_CAST(SPLIT(SPLIT(event_value, ':')[SAFE_OFFSET(1)],'"')[SAFE_OFFSET(1)] AS INT64) IS NOT NULL
                        THEN SAFE_CAST(SAFE_CAST(SPLIT(SPLIT(event_value, ':')[SAFE_OFFSET(1)],'"')[SAFE_OFFSET(1)] AS INT64) AS STRING)
                        ELSE 'null'
                    END AS CustomerID
                  ,CASE WHEN REGEXP_CONTAINS(Event_Name, '(?i)purchase|eposit') THEN 'Deposit'
                        WHEN REGEXP_CONTAINS(Event_Name, '(?i)registration')    THEN 'Registration'
                        ELSE 'Bet'
                        END AS Conversion
                  ,CASE WHEN Attributed_Touch_Time IS NULL OR Attributed_Touch_Time = 'null' THEN
                        CASE WHEN TIMESTAMP_DIFF(Event_Time, install_time, HOUR)>72 THEN 100    ELSE 99 END
                        ELSE TIMESTAMP_DIFF(SAFE_CAST(Event_Time AS TIMESTAMP), SAFE_CAST(Attributed_Touch_Time AS TIMESTAMP), DAY)
                   END AS Lag_days
                  ,CASE WHEN Attributed_Touch_Time IS NULL OR Attributed_Touch_Time = 'null' THEN
                        CASE WHEN TIMESTAMP_DIFF(Event_Time, install_time, HOUR)>72 THEN 100*24 ELSE 99*24 END
                        ELSE TIMESTAMP_DIFF(SAFE_CAST(Event_Time AS TIMESTAMP), SAFE_CAST(Attributed_Touch_Time AS TIMESTAMP), HOUR)
                   END AS Lag_hours
                  ,CASE WHEN Attributed_Touch_Type = 'impression' THEN 1 ELSE 0 END      AS View_conversion
                  ,CASE WHEN Attributed_Touch_Type = 'click' THEN 1 ELSE 0 END           AS Click_conversion
                  ,'App'       AS Conv_medium
                  ,'AppsFlyer' AS Dataset
                  ,''          AS adcontent
                  ,CASE WHEN af_keywords = 'null' THEN '' ELSE af_keywords END AS keyword
                  ,CASE WHEN event_revenue = 'null' THEN
                        CASE WHEN REGEXP_CONTAINS(event_value, 'deposit_amount')
                             THEN SAFE_CAST(REGEXP_REPLACE(SPLIT(event_value,'amount":')[SAFE_OFFSET(1)], '"|}', '') AS FLOAT64)
                             ELSE IFNULL(SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)],1,
                                                                  STRPOS(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)], ',')), r'\..*|[^0-9]', '') AS FLOAT64),
                                         SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)],1,
                                                                  STRPOS(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)], '}')), r'\..*|[^0-9]', '') AS FLOAT64)) END
                   ELSE SAFE_CAST(REGEXP_REPLACE(event_revenue, r'\..*|[^0-9]', '') AS FLOAT64)
                   END AS event_value
                   ,event_revenue_currency                       AS currency
                   , ''                                          AS transaction_id
                   ,CASE WHEN af_sub1 <> 'null' THEN SAFE_CAST(af_sub1 AS INT64)  END AS wm_tracking
                   ,CASE WHEN af_c_id <> 'null' THEN af_c_id  END AS CampaignId
                   ,country_code                                 AS country
                   ,REGEXP_REPLACE(REGEXP_REPLACE(bundle_id, 'bwin.de2', 'bwin.de'), '_beta','') AS bundle_id

              FROM {{ source('AppsFlyerGVC', 'GVC_Appsflyer_locker') }}
              WHERE EXTRACT(DATE FROM SAFE_CAST(Event_Time AS TIMESTAMP)) IS NOT NULL
                AND NOT REGEXP_CONTAINS(app_name,  '(?i)adb|oral|id700000007')
                AND event_name <>'af_all_deposits'
                AND attributed_touch_type <> 'null'
                AND (( NOT REGEXP_CONTAINS(app_name,  '(?i)PartyCasino|partyslots|partypoker') AND REGEXP_CONTAINS(event_name,'(?i)registration|eposit|urchase') )
                  OR ( REGEXP_CONTAINS(app_name,  '(?i)PartyCasino|partyslots|partypoker')
                      AND REGEXP_CONTAINS(event_name,'(?i)registration|eposit')
                      AND app_id IN ('com.partycasino.casino-standalone'
                                    ,'com.partycasino.casino'
                                    ,'com.partycasino.es.casino'
                                    ,'com.partycasino.es.casino-standalone'
                                    ,'de.partyslots.slots-standalone'
                                    ,'id1464496380'
                                    ,'id818432894'
                                    ,'com.partycasino.es.casino'
                                    ,'fr.partypoker.poker'
                                    ,'com.partypoker.poker'
                                    ,'id910125274'
                                    ,'id1420028752'
                                    ,'id1513834346'
                                    ,'id1445031714'
                                    ,'id687740281')  ))
             GROUP BY 1,2,3,4,5,6,7,8,9,10,
                      11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
           ) a

         LEFT JOIN {{ref('gvc_dim_campaigns')}}
                ON campaignid = campaign_id

         WHERE a.Date >= '2021-01-01' AND REGEXP_CONTAINS(a.Brand, '(?i)Bwin|Gala|foxy|cheeky|party')
       ),


 val AS( SELECT af.*, Bv.brand AS realbrand
         FROM af
         LEFT JOIN {{ref('td_brand_validation')}} BV
                ON account_id = SAFE_CAST(customerid AS INT64)
         WHERE NOT REGEXP_CONTAINS(Bv.brand, '(?i)Cashcade|Borgata|MGM|penn')
         ),


camp AS (SELECT * , ROW_NUMBER () OVER (PARTITION BY Campaign_id ORDER BY rara DESC, Campaign_Start_Date DESC) AS rank
         FROM(
              SELECT distinct Campaign, Campaign_id, Campaign_Start_Date, Campaign_End_Date
                     ,CASE WHEN REGEXP_CONTAINS(Campaign, 'cid|c:') THEN 1 ELSE 0 END AS rara
              FROM {{ source('DCM_GVC', 'p_match_table_campaigns_8807') }}) 
         )


SELECT a.* REPLACE(CASE WHEN r.Campaign IS NOT NULL THEN r.Campaign ELSE a.Campaign END AS Campaign,
                   CASE WHEN website = 'premium.de'                 THEN 'bpremium.de'
                        WHEN NOT REGEXP_CONTAINS(Brand, '(?i)bwin') THEN REGEXP_REPLACE(CONCAT(LOWER(Brand), '.', website),' ','')
                        ELSE CONCAT('bwin.', website) END AS website)
                  , '' as gclid
                  ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute

                   FROM (SELECT distinct * EXCEPT(realbrand)
                         FROM val
                         WHERE realbrand = Brand) a
                   LEFT JOIN (SELECT distinct placement_id, Campaign_id
                              FROM {{ source('DCM_GVC', 'p_match_table_placements_8807') }} ) b
                   ON a.Campaign = placement_id
                   LEFT JOIN (SELECT * FROM camp WHERE rank = 1) r
                   ON b.Campaign_id = r.Campaign_id
                   WHERE Date >= '2021-01-01'
                     AND CustomerID <> 'null'
