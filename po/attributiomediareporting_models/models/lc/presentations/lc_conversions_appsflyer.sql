WITH af AS(
SELECT *
FROM(
  SELECT CASE WHEN (REGEXP_CONTAINS(app_name, '(?i)gala') AND REGEXP_CONTAINS(app_name, '(?i)casino'))
                OR (REGEXP_CONTAINS(bundle_id,'(?i)gala') AND REGEXP_CONTAINS(bundle_id, '(?i)casino'))
                OR REGEXP_CONTAINS(app_name, '(?i)Gala Casino' )             THEN 'Gala Casino'
              WHEN (REGEXP_CONTAINS(app_name,'(?i)gala') AND REGEXP_CONTAINS(app_name, '(?i)spins'))
                OR REGEXP_CONTAINS(app_name, '(?i)Gala Spins'  )             THEN 'Gala Spins'
              WHEN REGEXP_CONTAINS(app_name, '(?i)Gala Bingo'  )             THEN 'Gala Bingo'
              WHEN REGEXP_CONTAINS(bundle_id,'(?i)ladbrokes'   )
                OR REGEXP_CONTAINS(app_name, '(?i)ladbrokes'   )             THEN 'Ladbrokes'
              WHEN REGEXP_CONTAINS(app_name, '(?i)coral'       )             THEN 'Coral'
              END AS Brand
         ,EXTRACT(DATE FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Date
         ,EXTRACT(HOUR FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Hour
         ,EXTRACT(TIME FROM SAFE_CAST(Event_Time AS TIMESTAMP)) AS Event_time
         ,'blue' AS ChannelGrouping
         ,CASE WHEN REGEXP_CONTAINS(media_source, '_int|troop') THEN 'disp' ELSE IF(af_prt = 'null', '', af_prt)  END AS medium
         ,CASE WHEN media_source = 'Apple Search Ads'  THEN 'Apple_Ads'
               WHEN media_source = 'Facebook Ads'      THEN 'Facebook'
               WHEN media_source = 'Twitter'           THEN 'Twitter'
               WHEN media_source = 'googleadwords_int' THEN 'Google_Ads'
               WHEN REGEXP_CONTAINS(media_source, '_int|troop|TV'  ) THEN REGEXP_REPLACE(media_source, '_int', '')
               WHEN REGEXP_CONTAINS(media_source, 'Coral|Lads|Gala') THEN 'internal'
               ELSE 'unknown' END AS source
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
         ,CASE WHEN REGEXP_CONTAINS(Event_Name, 'purchase|eposit') THEN 'Deposit'
               ELSE 'Registration'
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
               CASE WHEN REGEXP_CONTAINS(event_name, 'count')
                    THEN SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(event_value,'"amount":')[SAFE_OFFSET(1)],1,
                                                  STRPOS(SPLIT(event_value,'"amount":')[SAFE_OFFSET(1)], ',')), r'\..*|[^0-9]', '') AS INT64)
                    ELSE IFNULL(SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)],1,
                                                         STRPOS(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)], ',')), r'\..*|[^0-9]', '') AS INT64),
                                SAFE_CAST(REGEXP_REPLACE(SUBSTR(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)],1,
                                                         STRPOS(SPLIT(event_value,'"af_revenue":')[SAFE_OFFSET(1)], '}')), r'\..*|[^0-9]', '') AS INT64)) END
          ELSE SAFE_CAST(REGEXP_REPLACE(event_revenue, r'\..*|[^0-9]', '') AS INT64)
          END AS dep_amount
         ,'' AS transaction_id
         ,SAFE_CAST(CASE WHEN SAFE_CAST(af_sub1 AS INT64) IS NOT NULL AND LENGTH(CAST(SAFE_CAST(af_sub1 AS INT64) AS STRING))=7 THEN af_sub1 END AS INT64) AS wm_tracking
         ,SAFE_CAST(CASE WHEN af_c_id <> 'null' THEN af_c_id END AS INT64) AS CampaignId


    FROM {{ source('AppsFlyerGVC', 'GVC_Appsflyer_locker') }}

    WHERE 1=1
      AND (REGEXP_CONTAINS(app_name,  '(?i)adb|oral') OR REGEXP_CONTAINS(bundle_id,  '(?i)adb|oral'))
      AND REGEXP_CONTAINS(event_name,'(?i)registration|eposit|urchase')


    GROUP BY 1,2,3,4,5,6,7,8,9,10,
             11,12,13,14,15,16,17,18,19,20,21,22,23


  ) WHERE Date >= '2020-01-01' AND Brand IS NOT NULL AND Brand <> 'Other'
),


val AS ( SELECT af.*, Bv.brand AS realbrand
         FROM af
         LEFT JOIN {{ref('td_brand_validation')}} BV
                ON account_id = SAFE_CAST(customerid AS INT64)
         WHERE NOT REGEXP_CONTAINS(Bv.brand, '(?i)Cashcade|Borgata|MGM|penn')
        ),



     camp      AS (SELECT * , ROW_NUMBER () OVER (PARTITION BY campaign_id ORDER BY rara DESC, Campaign_Start_Date DESC) AS rank
                   FROM(
                        SELECT distinct campaign, campaign_id, Campaign_Start_Date, Campaign_End_Date
                               ,CASE WHEN REGEXP_CONTAINS(campaign, 'cid|c:') THEN 1 ELSE 0 END AS rara
                        FROM {{ source('DCM_UK', 'p_match_table_campaigns_785192') }})
                   )

SELECT a.* REPLACE(CASE WHEN r.campaign IS NOT NULL THEN r.campaign ELSE a.campaign END AS campaign)
           ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute

 FROM (SELECT distinct * EXCEPT(realbrand)
       FROM val
       WHERE realbrand = Brand) a

 LEFT JOIN (SELECT distinct placement_id, campaign_id FROM {{ source('DCM_UK', 'p_match_table_placements_785192') }} ) b
        ON a.campaign = placement_id
 LEFT JOIN (SELECT * FROM camp WHERE rank = 1) r
        ON b.campaign_id = r.campaign_id
 WHERE Date >= '2020-01-20'
