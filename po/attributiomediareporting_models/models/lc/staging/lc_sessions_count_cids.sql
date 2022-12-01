WITH  CoralGA     AS(  SELECT * FROM {{ source('Coral_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231"),
       LadsGA     AS(  SELECT * FROM {{ source( 'Lads_GA', 'ga_sessions_*') }} WHERE _TABLE_SUFFIX BETWEEN "20200120" AND "20221231"),

     
     
  Uni AS(   
            SELECT "Coral"                                                   AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign
                   ,trafficSource.adwordsClickInfo.campaignId                AS campaignId
                   ,trafficSource.adContent                                  AS adContent
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
            
            FROM CoralGA
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            
            UNION ALL 
            
            SELECT "Ladbrokes"                                               AS Brand
                   ,date                                                     AS Date
                   ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime))     AS visitStartTime
                   ,fullVisitorId
                   ,VisitId
                   ,channelGrouping
                   ,trafficSource.campaign                                   AS campaign                   
                   ,trafficSource.adwordsClickInfo.campaignId                AS campaignId
                   ,trafficSource.adContent                                  AS adContent
                   ,trafficSource.source                                     AS source
                   ,trafficSource.medium                                     AS medium
            
            FROM LadsGA
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            
      )
      
SELECT * REPLACE(CAST(CONCAT(SAFE.SUBSTR(Date, 1,4),'-',SAFE.SUBSTR(Date, 5,2), '-', SAFE.SUBSTR(Date, 7,2)) AS DATE) AS Date, CAST(campaignid AS STRING) AS campaignid)
         ,CONCAT(fullVisitorId, '_', CAST(visitid AS STRING)) AS sessionid,
          CASE WHEN campaignID IS NOT NULL THEN 'ppc'
               WHEN cid IS NOT NULL AND SAFE_CAST(cid AS INT64) NOT IN (52840,00000) THEN 'cid'
               WHEN (NOT REGEXP_CONTAINS(campaign, 'not set|notset') OR campaign IS NULL) THEN 'name'
          END AS join_type
FROM(
      SELECT *, CASE WHEN REGEXP_CONTAINS(campaign, 'cid')  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)    
                     WHEN REGEXP_CONTAINS(Campaign, 'c:' )  THEN SAFE_CAST(SUBSTR(SPLIT(campaign, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64)
                     WHEN REGEXP_CONTAINS(adcontent,'cid')  THEN SAFE_CAST(SPLIT(adcontent, 'cid:')[SAFE_OFFSET(1)] AS INT64)
                     WHEN REGEXP_CONTAINS(adcontent,'c:')   THEN SAFE_CAST(SPLIT(adcontent, 'c:')  [SAFE_OFFSET(1)] AS INT64)
                     END AS cid
      FROM uni)
   