WITH PartyPokerGA     AS(  SELECT * FROM {{ source( 'PartyPoker_GA', 'ga_sessions_*') }}            WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS STRING), '-', '') ),

registrations as
(select "Party Poker"                                                                                         AS Brand
  ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
  ,min(EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64))))                  AS event_time
  ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
  ,t.fullVisitorId
  ,t.VisitId
  ,channelGrouping
  ,trafficSource.campaign                                                                                     AS campaign
  ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
  ,trafficSource.source                                                                                       AS source
  ,trafficSource.medium                                                                                       AS medium
  ,trafficSource.adContent                                                                                    AS adContent
  ,device.deviceCategory                                                                                      AS device
  ,trafficSource.keyword                                                                                      AS keyword
  ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
  ,'Registration'                                                                                             AS Conversion
  ,NULL                                                                                                       AS event_value
  ,hits.transaction.transactionID                                                                             AS transactionID
  ,hits.page.hostname                                                                                         AS website
  ,''                                                                                                         AS trans_currency
  --,hits.page.pagepath                                                                                         AS landing
  --,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
  ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
  ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=12  )                                         AS client
  ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adWords_adGroupId
  ,trafficSource.adWordsClickInfo.campaignid                                                                  AS adWords_campaignid
  ,trafficSource.adWordsClickInfo.creativeId                                                                  AS adWords_creativeId
  ,trafficSource.adWordsClickInfo.criteriaId                                                                  AS adWords_criteriaId
  ,trafficSource.adWordsClickInfo.page                                                                        AS adWords_page
  ,trafficSource.adWordsClickInfo.slot                                                                        AS adWords_slot
  ,trafficSource.adWordsClickInfo.criteriaParameters                                                          AS adWords_criteriaParameters
  ,trafficSource.adWordsClickInfo.adNetworkType                                                               AS adWords_adNetworkType
  ,trafficSource.adWordsClickInfo.gclid as adWords_gclid
  ,trafficSource.isTrueDirect

  ,geoNetwork.city
  ,geoNetwork.country
  ,device.operatingSystem
  ,device.operatingSystemVersion
  ,device.screenResolution
from
  PartyPokerGA t, t.hits hits
WHERE (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=12  )in ('pokervc')
and REGEXP_CONTAINS(hits.eventinfo.eventCategory,"(?i)regis") AND REGEXP_CONTAINS(hits.eventinfo.eventAction,"(?i)success")
GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
),

downloads as
(select "Party Poker" AS Brand
  ,EXTRACT(DATE FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS Date
  ,EXTRACT(TIME FROM TIMESTAMP_SECONDS(visitStartTime + CAST(hits.time/1000 AS INT64)))                       AS event_time
  ,EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime))                                                   AS visitStartTime
  ,t.fullVisitorId
  ,t.VisitId
  ,channelGrouping
  ,trafficSource.campaign                                                                                     AS campaign
  ,trafficSource.adwordsClickInfo.campaignId                                                                  AS campaignId
  ,trafficSource.source                                                                                       AS source
  ,trafficSource.medium                                                                                       AS medium
  ,trafficSource.adContent                                                                                    AS adContent
  ,device.deviceCategory                                                                                      AS device
  ,trafficSource.keyword                                                                                      AS keyword
  ,MAX((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 AND REGEXP_CONTAINS(value,"[0-9]{8}"))) AS CustomerID
  ,'Registration'                                                                                             AS Conversion
  ,NULL                                                                                                       AS event_value
  ,hits.transaction.transactionID                                                                             AS transactionID
  ,hits.page.hostname                                                                                         AS website
  ,''                                                                                                         AS trans_currency
  ,hits.page.pagepath                                                                                         AS landing
  --,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=3   )                                         AS country
  ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=14  )                                         AS tracker
  ,(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=12  )                                         AS client
  ,trafficSource.adWordsClickInfo.adGroupId                                                                   AS adWords_adGroupId
  ,trafficSource.adWordsClickInfo.campaignid                                                                  AS adWords_campaignid
  ,trafficSource.adWordsClickInfo.creativeId                                                                  AS adWords_creativeId
  ,trafficSource.adWordsClickInfo.criteriaId                                                                  AS adWords_criteriaId
  ,trafficSource.adWordsClickInfo.page                                                                        AS adWords_page
  ,trafficSource.adWordsClickInfo.slot                                                                        AS adWords_slot
  ,trafficSource.adWordsClickInfo.criteriaParameters                                                          AS adWords_criteriaParameters
  ,trafficSource.adWordsClickInfo.adNetworkType                                                               AS adWords_adNetworkType
  ,trafficSource.adWordsClickInfo.gclid as adWords_gclid
  ,trafficSource.isTrueDirect
  ,geoNetwork.city
  ,geoNetwork.country
  ,device.operatingSystem
  ,device.operatingSystemVersion
  ,device.screenResolution
from
  PartyPokerGA t, t.hits hits
WHERE (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=15  )='download'
and (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=12  ) in ('desktop')--, 'not available in the datalayer')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,18,19,20,hits.eventInfo.eventValue,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
),

agg as
(select
  download_visitorid,
  registration_visitid,
  registration_customerid,
  download_date,
  registration_date,
  max(download_time) as download_time,
  registration_time,
  download_city,
  registration_city,
  download_country,
  registration_country,
  d_operatingSystem,
  r_operatingSystem,
  d_operatingSystemVersion,
  r_operatingSystemVersion,
  d_screenResolution,
  r_screenResolution,
  channelGrouping,
  campaign,
  campaignId,
  source,
  medium,
  adContent,
  keyword,
  adWords_adGroupId,
  adWords_campaignid,
  adWords_creativeId,
  adWords_criteriaId,
  adWords_page,
  adWords_slot,
  adWords_criteriaParameters,
  adWords_adNetworkType,
  adWords_gclid,
  isTrueDirect,
  tracker,
  website
from
  (select
    d.fullVisitorId as download_visitorid,
    r.visitid as registration_visitid,
    r.customerid as registration_customerid,
    d.date as download_date,
    r.date as registration_date,
    d.event_time as download_time,
    r.event_time as registration_time,
    d.country as download_country,
    r.country as registration_country,
    d.city as download_city,
    r.city as registration_city,
    d.operatingSystem as d_operatingSystem,
    r.operatingSystem as r_operatingSystem,
    d.operatingSystemVersion as d_operatingSystemVersion,
    r.operatingSystemVersion as r_operatingSystemVersion,
    d.screenResolution as d_screenResolution,
    r.screenResolution as r_screenResolution,
    d.channelGrouping,
    d.campaign,
    d.campaignId,
    d.source,
    d.medium,
    d.adContent,
    d.keyword,
    d.adWords_adGroupId,
    d.adWords_campaignid,
    d.adWords_creativeId,
    d.adWords_criteriaId,
    d.adWords_page,
    d.adWords_slot,
    d.adWords_criteriaParameters,
    d.adWords_adNetworkType,
    d.adWords_gclid,
    d.isTrueDirect,
    d.tracker,
    d.website
  from
    registrations r
  left join
    downloads d
  on
    r.date = d.date
  and
    r.event_time > d.event_time
  )q
group by
  download_visitorid,
  registration_visitid,
  registration_customerid,
  download_date,
  registration_date,
  registration_time,
  download_city,
  registration_city,
  download_country,
  registration_country,
  d_operatingSystem,
  r_operatingSystem,
  d_operatingSystemVersion,
  r_operatingSystemVersion,
  d_screenResolution,
  r_screenResolution,
  channelGrouping,
  campaign,
  campaignId,
  source,
  medium,
  adContent,
  keyword,
  adWords_adGroupId,
  adWords_campaignid,
  adWords_creativeId,
  adWords_criteriaId,
  adWords_page,
  adWords_slot,
  adWords_criteriaParameters,
  adWords_adNetworkType,
  adWords_gclid,
  isTrueDirect,
  tracker,
  website
),

matched as
(select * from
  (select *,
  case
    when download_country = registration_country
    and ifnull(download_country,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(registration_country,'') not in ('(not set)', 'not available in the datalayer', '')
  then 1 else 0 end as match_country,
  case
    when download_city = registration_city
    and ifnull(download_city,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(registration_city,'') not in ('(not set)', 'not available in the datalayer', '')
  then 1 else 0 end as match_city,
  case
    when d_operatingSystem||' '||d_operatingSystemVersion = r_operatingSystem||' '||r_operatingSystemVersion
    and ifnull(d_operatingSystem,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(r_operatingSystem,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(d_operatingSystemVersion,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(r_operatingSystemVersion,'') not in ('(not set)', 'not available in the datalayer', '')
  then 1 else 0 end as match_os,
  case
    when d_screenResolution = r_screenResolution
    and ifnull(d_screenResolution,'') not in ('(not set)', 'not available in the datalayer', '')
    and ifnull(r_screenResolution,'') not in ('(not set)', 'not available in the datalayer', '')
  then 1 else 0 end as match_res
  from
    agg
  )q
),

downloads_flattened as
(select
  aa.download_visitorid,
  aa.registration_visitid,
  aa.registration_customerid,
  aa.download_date,
  aa.download_time,
  aa.registration_date,
  aa.registration_time,
  aa.download_city,
  aa.registration_city,
  aa.download_country,
  aa.registration_country,
  aa.d_operatingSystem,
  aa.r_operatingSystem,
  aa.d_operatingSystemVersion,
  aa.r_operatingSystemVersion,
  aa.d_screenResolution,
  aa.r_screenResolution,
  aa.channelGrouping,
  aa.campaign,
  aa.campaignId,
  aa.source,
  aa.medium,
  aa.adContent,
  aa.keyword,
  aa.adWords_adGroupId,
  aa.adWords_campaignid,
  aa.adWords_creativeId,
  aa.adWords_criteriaId,
  aa.adWords_page,
  aa.adWords_slot,
  aa.adWords_criteriaParameters,
  aa.adWords_adNetworkType,
  aa.adWords_gclid,
  aa.isTrueDirect,
  aa.tracker,
  aa.website,
  aa.matchcount as matchLevel,
  bb.matches as totalMatches,
  match_country,
  match_city,
  match_os,
  match_res,
  case
    when aa.matchcount = 4 and bb.matches = 1 then 1
    when TIME_DIFF(aa.registration_time, aa.download_time, MINUTE) <= 60 then
        case
            when aa.matchcount = 4 and bb.matches > 1 then 2
            when aa.matchcount = 3 and match_res = 0 then 3
            when aa.matchcount = 3 and match_city = 0
                and (registration_city in ('(not set)','') or download_city in ('(not set)',''))  then 4
            else 0
        end
    else 0
  end as match_rule
from
  (select
    bb.*,
    rank() over (partition by  registration_visitid order by rnk2, rnk) as rnk3
  from
    (select
      cc.*,
      rank() over (partition by  download_visitorid order by matchcount desc, time_diff(registration_time, download_time, MINUTE) asc) as rnk2
    from
      (select *,
        rank() over (partition by  registration_visitid, registration_customerid, download_date, registration_date order by match_country+match_city+match_os+match_res desc, download_time desc) as rnk,
        match_country+match_city+match_os+match_res as matchcount
      from
          matched
      ) cc
    ) bb
  ) aa
left join
  (select
    registration_visitid,
    registration_customerid,
    download_date,
    registration_date,
    match_country+match_city+match_os+match_res as matchcount,
    count(*) as matches
  from
    matched
  group by 1,2,3,4,5
  ) bb
on
  aa.registration_visitid = bb.registration_visitid
  and aa.registration_customerid = bb.registration_customerid
  and aa.matchcount = bb.matchcount
where
  rnk3=1
)

select * from downloads_flattened
