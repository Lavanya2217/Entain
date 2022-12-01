SELECT distinct activity_date AS date
               ,CASE WHEN REGEXP_CONTAINS(Message_type_desc, '(?i)email') THEN 'CRM - Email'
                     WHEN REGEXP_CONTAINS(Message_type_desc, '(?i)push')  THEN 'CRM - Push'
                     ELSE 'CRM - Other' END AS Channelgrouping
               ,campaign_desc--, LOWER(REGEXP_REPLACE(country_cd, 'GB', 'uk')) AS country
               ,SUM(delivered_qty) AS Delivered

FROM {{ source('DWPRODVIEWSMSTR', 'F_PLR_COMMUNICATION_ACTIVITY') }}  AS com
INNER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_MESSAGE_TYPE') }}  d       ON com.wh_message_type_id = d.wh_message_type_id 
INNER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_CAMPAIGN_SEGMENT') }}  dcs ON com.Wh_Campaign_Segment_Id = dcs.Wh_Campaign_Segment_Id
INNER JOIN {{ source('DWPRODVIEWSMSTR', 'DIM_CAMPAIGN') }}  dc          ON dcs.wh_campaign_id = dc.wh_campaign_id

WHERE activity_date >= '2021-01-01' AND delivered_qty>0

  AND LOWER(campaign_desc) NOT IN ('test', 'test1')

GROUP BY 1,2,3