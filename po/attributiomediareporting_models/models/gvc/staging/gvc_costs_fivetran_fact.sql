with 
_7stars as(  
  SELECT CAST(DATE(date) AS DATE) as date,
         'pc'||'_'||regexp_replace(lower(Platform),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign),r'| |_|-|\.','') as x_campaign_id   ,   -- no ID, use name
         Platform as x_account_id, -- no ID, use name
        sum(Spend) as spend ,
        sum(Link_Clicks) as clicks,	
        sum(Impressions) as  impressions	
  FROM {{ source('gvc_offline_marketing', 'PartyCasino_Brand_7stars') }}  
  where CAST(DATE(date) AS DATE) >='2019-01-01'
  and (Spend>0 or Link_Clicks>0 or impressions>0)
  group by 1,2,3  
),

amobee as(
  SELECT CAST(DATE(date) AS DATE) as date,
         line_item_id as x_campaign_id   ,  
         advertiser_id as x_account_id,
        sum(total_cost) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions	
  FROM  {{ source('gvc_tomorrow', 'Amobee') }} 
  where CAST(DATE(date) AS DATE) >='2019-01-01'
  and (total_cost>0 or clicks>0 or impressions>0)
  group by 1,2,3  
),

apple_bwin_masteracc as( 
  SELECT  CAST(DATE(a.date) AS DATE) as date,
         a.id x_campaign_id,  
         c.organization_id as x_account_id,
        sum(a.local_spend_amount) as spend ,
        sum(a.taps) as clicks,	
        sum(a.impressions) as  impressions
        		
  FROM  {{ source('gvc_apple_search_ads_bwin_masteracc', 'campaign_report') }} 	as a
  left join
  (select  max(modification_time) as max_date,id,organization_id 
   from {{ source('gvc_apple_search_ads_bwin_masteracc', 'campaign_history') }}  group by 2,3) as c
  on a.id=c.id
  
  where CAST(DATE(a.date) AS DATE)>='2019-01-01'
  group by 1,2,3   
),

apple_gvc_services_ltd as(  
  SELECT  CAST(DATE(a.date) AS DATE) as date,
       a.id x_campaign_id,  
       c.organization_id as x_account_id,
      sum(a.local_spend_amount) as spend ,
      sum(a.taps) as clicks,	
      sum(a.impressions) as  impressions
      		
  FROM  {{ source('gvc_apple_search_ads_gvc_services_ltd', 'campaign_report') }} 	as a
  left join
  (select  max(modification_time) as max_date,id,organization_id 
   from {{ source('gvc_apple_search_ads_gvc_services_ltd', 'campaign_history') }}  group by 2,3) as c
  on a.id=c.id

where CAST(DATE(a.date) AS DATE)>='2019-01-01'
group by 1,2,3
),

apple_moblinglobalzoomltd as(  
  SELECT  CAST(DATE(a.date) AS DATE) as date,
       a.id x_campaign_id,  
       c.organization_id as x_account_id,
      sum(a.local_spend_amount) as spend ,
      sum(a.taps) as clicks,	
      sum(a.impressions) as  impressions
      		
  FROM  {{ source('gvc_apple_search_ads_moblinglobalzoomltd', 'campaign_report') }} 	as a
  left join
  (select  max(modification_time) as max_date,id,organization_id 
   from {{ source('gvc_apple_search_ads_moblinglobalzoomltd', 'campaign_history') }}  group by 2,3) as c
  on a.id=c.id

where CAST(DATE(a.date) AS DATE)>='2019-01-01'
group by 1,2,3
),

apple_ppuk as(  
  SELECT  CAST(DATE(a.date) AS DATE) as date,
       a.id x_campaign_id,  
       c.organization_id as x_account_id,
      sum(a.local_spend_amount) as spend ,
      sum(a.taps) as clicks,	
      sum(a.impressions) as  impressions

      		
  FROM  {{ source('gvc_apple_search_ads_ppuk', 'campaign_report') }} 	as a
  left join
  (select  max(modification_time) as max_date,id,organization_id 
   from {{ source('gvc_apple_search_ads_ppuk', 'campaign_history') }}  group by 2,3) as c
  on a.id=c.id

where CAST(DATE(a.date) AS DATE)>='2019-01-01'
group by 1,2,3
),

apple_partycasino as(  
  
  SELECT  CAST(DATE(a.date) AS DATE) as date,
         a.id x_campaign_id,  
         c.organization_id as x_account_id,
        sum(a.local_spend_amount) as spend ,
        sum(a.taps) as clicks,	
        sum(a.impressions) as  impressions
        		
  FROM  {{ source('gvc_apple_search_ads_partycasino', 'campaign_report') }} 	as a
  left join
  (select  max(modification_time) as max_date,id,organization_id 
   from {{ source('gvc_apple_search_ads_partycasino', 'campaign_history') }}  group by 2,3) as c
  on a.id=c.id
  
  where CAST(DATE(a.date) AS DATE)>='2019-01-01'
  group by 1,2,3
),

appnexus as(  
  SELECT distinct CAST(DATE(day) AS DATE) as date,
         line_item_id as x_campaign_id,   
         advertiser_id   as x_account_id,
         sum(total_cost_buying_currency) as spend ,
         sum(clicks) as clicks,	
         sum(imps) as  impressions
        		
  FROM 
      (SELECT DISTINCT * EXCEPT(_modified,_directory,_fivetran_synced ) 
      from {{ source('gvc_fivetran_email', 'appnexus') }} )
  where CAST(DATE(day) AS DATE)>='2019-01-01'
  group by 1,2,3
  

),

appnexus_gvc_tomorrow as(  
  
SELECT CAST(DATE(date) AS DATE) as date,
       Line_item_ID as x_campaign_id   ,  
       advertiser_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,	
      sum(impressions) as  impressions	
FROM  {{ source('gvc_tomorrow', 'AppNexus') }} 
where CAST(DATE(date) AS DATE) >='2019-01-01'
group by 1,2,3
),

appsflyer_facebook as(  
  SELECT CAST(DATE(date) AS DATE) as date,
       Campaign_Name__AppsFlyer as x_campaign_id   ,  
	   case when regexp_contains(lower(Campaign_Name__AppsFlyer),'[^a-z]uk') then 171250260389816
					when regexp_contains(lower(Campaign_Name__AppsFlyer),'[^a-z]es') then 1274030889436455
					else 1 end  as x_account_id, ---Mikael Boecasse <Mikael@gvc_tomorrowadvertising.com>
      sum(Total_Revenue__AppsFlyer) as spend ,
      sum(Clicks__AppsFlyer) as clicks,	
      sum(Impressions__AppsFlyer) as  impressions	
FROM  {{ source('gvc_tomorrow', 'APPSFLYER_FACEBOOK') }}  
where Date >='2019-01-01'
group by 1,2,3
having sum(Total_Revenue__AppsFlyer)>0 or sum(Clicks__AppsFlyer)>0 or sum(Impressions__AppsFlyer)>0
),

appsflyer_appnetworks as(  
  SELECT CAST(DATE(date) AS DATE) as date,
    case
        when REGEXP_CONTAINS(Campaign_c__AppsFlyer,'c:')
            then SUBSTR(SPLIT(Campaign_c__AppsFlyer, 'c:')[SAFE_OFFSET(1)],1,5)
      -- create id using brand, publisher and campaign name
        else 'pc'||'_'||regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign_c__AppsFlyer),r'| |_|-|\.','')
        end AS x_campaign_id, -- no ID, check for CID else use created id
    regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.','') as x_account_id, -- no ID, use name
    null as spend ,
    sum(Clicks__AppsFlyer) as clicks,
    sum(Impressions__AppsFlyer) as  impressions
FROM  {{ source('gvc_tomorrow', 'APPSFLYER_APPNETWORKS') }}  
where CAST(DATE(date) AS DATE) >='2019-01-01'
and (Clicks__AppsFlyer>0 or Impressions__AppsFlyer>0)
and regexp_replace(lower(Media_Source_pid__AppsFlyer),r'_int| |_|-|\.','')
  not in (select id from {{ source('exclusions_lists_pp', 'App_Networks_Exclusion_List') }} )-- remove non app networks
group by 1,2,3
union all
SELECT
    case
        when app.Term='daily' then app.Start_Date
        when app.Term<>'daily' and cal.calendar_date<>app.End_Date then cal.calendar_date
        else null
    end as Date
    ,case
        when REGEXP_CONTAINS(Campaign_name,'c:') then SUBSTR(SPLIT(Campaign_name, 'c:')[SAFE_OFFSET(1)],1,5)
        -- create id using brand, publisher and campaign name
        else 'pc'||'_'||regexp_replace(lower(case when regexp_contains(Partner_name,'(?i)mooko') then 'mooko' else Partner_name end),r'_int| |_|-|\.','')
                                            ||'_'||regexp_replace(lower(Campaign_name),r'| |_|-|\.','')
    end AS x_campaign_id -- no ID, check for CID else use created id
    , regexp_replace(lower(case when regexp_contains(Partner_name,'(?i)mooko') then 'mooko' else Partner_name end),r'_int| |_|-|\.','') as x_account_id -- no ID, use name
    , sum(
        case
            when app.Term='daily' then app.actual_spend
            when app.Term='weekly' then app.actual_spend/7
            when app.Term='monthly' then app.actual_spend/30
        end
        ) as spend
    , null as clicks
    , null as impressions
FROM {{ source('gvc_partners_costs', 'PartyCasino_AppNetworks') }}  app
join {{ source('DWPRODVIEWSMSTR', 'DIM_CALENDAR') }}   cal on cal.calendar_date>=app.Start_Date and cal.calendar_date<=app.End_Date
WHERE app.Start_Date >= '2019-01-01'
group by 1,2,3

),

appsflyer_appnetworks_pp as(  
  SELECT
          case
              when app.Term='daily' then app.Start_Date
              when app.Term<>'daily' and cal.calendar_date<>app.End_Date then cal.calendar_date
              else null
          end as Date
          ,case
          when REGEXP_CONTAINS(Campaign_name,'c:')
              then SUBSTR(SPLIT(Campaign_name, 'c:')[SAFE_OFFSET(1)],1,5)
        -- create id using brand, publisher and campaign name
          else 'pp'||'_'||regexp_replace(lower(Partner_name),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign_name),r'| |_|-|\.','')
          end AS x_campaign_id -- no ID, check for CID else use created id
          , regexp_replace(lower(Partner_name),r'_int| |_|-|\.','') as x_account_id -- no ID, use name
          , sum(
              case
                  when app.Term='daily' then app.actual_spend
                  when app.Term='weekly' then app.actual_spend/7
                  when app.Term='monthly' then app.actual_spend/30
              end
              ) as spend
          , null as clicks
          , null as impressions
  FROM {{ source('PartyPoker_Offline', 'Partypoker_Appnetwork_Spend') }}   app
  join {{ source('DWPRODVIEWSMSTR', 'DIM_CALENDAR') }}  cal on cal.calendar_date>=app.Start_Date and cal.calendar_date<=app.End_Date
  WHERE app.Start_Date >= '2019-01-01'
  group by 1,2,3

),


bing_ads as( 
SELECT  CAST(DATE(date) AS DATE) as date, 
       campaign_id x_campaign_id,  
       account_id x_account_id,
      sum(spend) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions
                  
FROM {{ source('gvc_bing_ads', 'campaign_performance_daily_report') }}      
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3

),

connected_tv as(  
  SELECT CAST(DATE(date) AS DATE) as date,
         line_item_id as x_campaign_id   ,  
         advertiser_id as x_account_id,
        sum(total_cost) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions	
  FROM  {{ source('gvc_tomorrow', 'Connected_TV') }}  
  where CAST(DATE(date) AS DATE) >='2019-01-01'
  and (total_cost>0 or clicks>0 or impressions>0)
  group by 1,2,3  
),


dating_apps as(
  SELECT CAST(DATE(date) AS DATE) as date,
         Campaign_ID as x_campaign_id   ,  
         9111610276 as x_account_id,
        sum(cost) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions	
  FROM  {{ source('gvc_tomorrow', 'Dating_Apps') }}   
  where Date >='2019-01-01'
  group by 1,2,3  
),

doubleclick as(
  SELECT  CAST(DATE(date) AS DATE) as date,
         cast(campaign_id as string) as x_campaign_id   ,  
         cast(advertiser_id as string)  as x_account_id,
        sum(media_cost) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions
  FROM {{ source('gvc_double_click_campaign_manager_final', 'fivetran_datasets') }} 
  where (not REGEXP_CONTAINS(advertiser,'(?i)Intertrader|do not use|4269402|Real Madrid 2016|NATURAL SEARCH|TEST'))
  and advertiser_id is not null 
  and date>='2019-01-01'
  group by 1,2,3
),

dv360 as(
  SELECT distinct CONCAT(SUBSTR(date,1,4),"-",SUBSTR(date,6,2),"-",SUBSTR(date,9,2))  as date,
         if(advertiser in ('Coral', 'Ladbrokes'), line_item_id, campaign_id) as x_campaign_id,   
         advertiser_id   as x_account_id,
        sum(total_media_cost_advertiser_currency) as spend ,
         sum(clicks) as clicks,	
         sum(impressions) as  impressions
        		
  FROM {{ source('gvc_google_display_and_video_360', 'fivetran_datasets') }} 
  where CONCAT(SUBSTR(date,1,4),"-",SUBSTR(date,6,2),"-",SUBSTR(date,9,2))>='2019-01-01'
  and date(_fivetran_synced)>'2021-02-01'
  group by 1,2,3 
),

engageya as(
  SELECT CAST(DATE(date) AS DATE) as date,
        Campaign_Id as x_campaign_id   ,  
        198422 as x_account_id, ---Mikael Boecasse <Mikael@gvc_tomorrowadvertising.com>
       sum(Spend) as spend ,
       sum(clicks) as clicks,	
       sum(Views) as  impressions	
 FROM  {{ source('gvc_tomorrow', 'EngageYa') }}  
 where CAST(DATE(date) AS DATE)  >='2019-01-01'
 group by 1,2,3
),

facebook as(
  SELECT CAST(DATE(date) AS DATE) AS date,
        campaign_id as x_campaign_id,   
        account_id  as x_account_id,
        sum(spend) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions,
        sum(inline_link_clicks) as inline_link_clicks
 FROM {{ source('gvc_facebook_hourly', 'fivetran_datasets') }} 
 where CAST(DATE(date) AS DATE)>='2019-01-01'
 group by 1,2,3
),

facebook_additional as(
  SELECT CAST(DATE(date) AS DATE) AS date,
        campaign_id as x_campaign_id,   
        account_id  as x_account_id,
        sum(spend) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions,
        sum(inline_link_clicks) as inline_link_clicks
 FROM {{ source('gvc_facebook_additional', 'fivetran_datasets') }} 
 where CAST(DATE(date) AS DATE)>='2019-01-01'
 and account_id!=548349136005374
 group by 1,2,3
),

forza as(
  SELECT CAST(DATE(date) AS DATE) as date,
        '00_forza' as x_campaign_id   ,  
        null as x_account_id,
       sum(cost) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as impressions	
 FROM {{ source('gvc_tomorrow', 'Forza') }}  
 where CAST(DATE(date) AS DATE) >='2019-01-01'
 and (cost>0 or clicks>0 or impressions>0)
 group by 1,2,3
),

google_ads_bwin_mcc as( 
SELECT  CAST(DATE(date) AS DATE) as date, 
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions
                
FROM {{ source('gvc_google_ads_campaign_performance_bwin_mcc', 'fivetran_datasets') }}   
group by 1,2,3
),

google_ads_bwin_performance_max_GR as(
SELECT  CAST(DATE(day) AS DATE) as date, 
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
       sum(cost) as spend,
       sum(safe_cast(REGEXP_REPLACE(clicks, ',', '') as INT64)) as clicks,
       sum(safe_cast(REGEXP_REPLACE(impr_, ',', '') as INT64)) as impressions

FROM {{ source('gvc_fivetran_email', 'google_ads_performancemax_gr') }}  
where CAST(DATE(day) AS DATE)>='2021-01-01'
group by 1,2,3
),

google_ads_camp_mcc_partypoker as(
SELECT  CAST(DATE(date) AS DATE) as date, 
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions
         
FROM {{ source('gvc_google_ads_campaign_performance_mcc_partypoker', 'fivetran_datasets') }}  
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

google_ads_party_casino as(
SELECT  CAST(DATE(date) AS DATE) as date,
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions    
FROM {{ source('gvc_google_ads_campaign_performance_party_casino', 'fivetran_datasets') }}  
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

google_ads_mcc_partycasino as(
SELECT  CAST(DATE(date) AS DATE) as date,
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions    

FROM {{ source('gvc_google_ads_campaign_performance_mcc_partycasino', 'fivetran_datasets') }}   
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3 
),

google_ads_party_gaming as(
SELECT  CAST(DATE(date) AS DATE) as date, 
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,  
      sum(impressions) as  impressions
                 
FROM {{ source('gvc_google_ads_campaign_performance_party_gaming', 'fivetran_datasets') }}   
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

hubble as(
  SELECT CAST(DATE(date) AS DATE) as date,
         'pc'||'_'||regexp_replace(lower(Platform),r'_int| |_|-|\.','')||'_'||regexp_replace(lower(Campaign),r'| |_|-|\.','') as x_campaign_id   ,   -- no ID, use name
         Platform as x_account_id, -- no ID, use name
        sum(Spend) as spend ,
        sum(Link_Clicks) as clicks,	
        sum(Impressions) as  impressions	
  FROM  {{ source('gvc_offline_marketing', 'PartyCasino_Brand_Hubble') }} 
  where CAST(DATE(date) AS DATE) >='2019-01-01'
  and (Spend>0 or Link_Clicks>0 or impressions>0)
  group by 1,2,3 
),

missmarcadores as(
  SELECT CAST(DATE(date) AS DATE) as date,
        '00_marcadores' as x_campaign_id   ,  
        null as x_account_id,
       sum(Cost_EU) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions	
 FROM  {{ source('gvc_tomorrow', 'MissMarcadores') }}  
 where CAST(DATE(date) AS DATE) >='2019-01-01'
 and (Cost_EU>0 or clicks>0 or impressions>0)
 group by 1,2,3
),

n365 as(
SELECT CAST(DATE(date) AS DATE) as date,
       LineItemID as x_campaign_id   ,  
       AdvertiserID as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,	
      sum(impressions) as  impressions	
FROM (select distinct * from {{ source('gvc_tomorrow', 'N365') }}  )
where CAST(DATE(date) AS DATE)  >='2019-01-01'
group by 1,2,3
 
),

n365_pp as(
  SELECT 
         DATE(date) as Date
         ,Campaign_Id AS x_campaign_id 
         , null as x_account_id
         , Sum(Spend) as Spend
         , Sum(Imps) as Impressions
         , Sum(Clicks)  as Clicks
 FROM {{ source('PartyPoker_Offline', 'N365_Data3') }} 
 WHERE DATE(date) >= '2019-01-01' and Spend is not null
 group by 1,2,3
),

outbrain as(
  SELECT CAST(DATE(date) AS DATE) as date,
        Campaign_ID as x_campaign_id   ,  
        '002ba7f38deb835306baf8afb590879236' as x_account_id, ---Mikael Boecasse <Mikael@gvc_tomorrowadvertising.com>
       sum(Spend) as spend ,
       sum(Clicks) as clicks,	
       sum(Impressions) as  impressions	
 FROM  {{ source('gvc_tomorrow', 'OutBrain') }} 
 where CAST(DATE(date) AS DATE)  >='2019-01-01'
 group by 1,2,3
),

smadex as(
  select 
  CAST(left(date_time,10) AS DATE) as date ,
  campaign_id as x_campaign_id,   
  account_id  as x_account_id,
  sum(total_spend) as spend ,
  sum(clicks) as clicks,	
  sum(impressions) as  impressions
  from {{ source('gvc_smadex', 'historical_data') }} 
  where CAST(left(date_time,10) AS DATE)>='2019-01-01'
  group by 1,2,3 
),

sky_adsmart as(
  SELECT 
         DATE(start_Date) as Date
         , null AS x_campaign_id 
         , null as x_account_id
         , Sum(SkyAdsmart_est__spend) as Spend
         , Sum(SkyAdsmart_TV_est__impacts_reach) as Impressions
         , null  as Clicks
 FROM  {{ source('gvc_offline_marketing', 'PartyPoker_UK') }}
 WHERE DATE(start_Date) >= '2019-01-01' 
 group by 1,2,3
),

snapchat_ads as( 
SELECT CAST(DATE(date) AS DATE) AS date, 
       campaign_id   as x_campaign_id, 
       ad_account_id x_account_id,
       (sum(spend)/1000000) as spend ,
       sum(swipes) as clicks, 
       sum(impressions) as  impressions
                  
FROM {{ source('gvc_snapchat_ads', 'campaign_hourly_report') }} as a
left join   
(select max(updated_at) as max_date,id,ad_account_id from {{ source('gvc_snapchat_ads', 'campaign_history') }}  group by 2,3) as c      
on a.campaign_id=c.id   

where CAST(DATE(date) AS DATE)>='2019-01-01'
      AND (impressions>0 or spend>0 or swipes>0) 
group by 1,2,3

 
),

taboola as(
  SELECT CAST(DATE(date) AS DATE) as date,
        Campaign_ID as x_campaign_id   ,  
        1286926 as x_account_id, ---Mikael Boecasse <Mikael@gvc_tomorrowadvertising.com>
       sum(Spent) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions	
 FROM  {{ source('gvc_tomorrow', 'Taboola') }} 
 where CAST(DATE(date) AS DATE)  >='2019-01-01'
 group by 1,2,3
),

tradedesk as(
  SELECT  CAST(DATE(date) AS DATE) as date,
        Campaign_ID as x_campaign_id,  
        Advertiser_ID as x_account_id,
       sum(Advertiser_Cost__Adv_Currency_) as spend ,
       sum(Clicks) as clicks,	
       sum(impressions) as  impressions
       		
 FROM {{ source('gvc_tradedesk_', 'raw_data_V2') }} 
 where CAST(DATE(date) AS DATE)>='2019-01-01'
 and Clicks>0 or Advertiser_Cost__Adv_Currency_>0 or impressions>0
 group by 1,2,3
),

twitter as(
  SELECT CAST(DATE(date) AS DATE) AS date,
        campaign_id as x_campaign_id,
        account_id  as x_account_id,
        (sum(billed_charge_local_micro)/1000000) as spend ,
        sum(clicks) as clicks,	
        sum(impressions) as  impressions
       		
 FROM {{ source('gvc_twitter_ads_final', 'campaign_report') }} 
 where CAST(DATE(date) AS DATE)>='2019-01-01'
       AND (impressions>0 or billed_charge_local_micro>0 or clicks>0)
 group by 1,2,3
),

verizon_dsp_gvc as(
  SELECT day as date,
        campaign_id as x_campaign_id   ,  
        advertiser_id as x_account_id,
       sum(advertiser_spending) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions
 
       		
 FROM (SELECT * REPLACE(CAST(CONCAT(SUBSTR(DAY,7,4),"-",SUBSTR(DAY,1,2),"-",SUBSTR(DAY,4,2)) AS DATE) AS Day) 
       FROM {{ source('gvc_fivetran_email', 'verizon_dsp') }})
 where day >='2019-01-01'
 group by 1,2,3
), 

viber as(
  SELECT CAST(DATE(date) AS DATE) as date,
        LineItem as x_campaign_id   ,  ---we dont have line item id
        null as x_account_id,
       sum(cost) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions	
 FROM  {{ source('gvc_tomorrow', 'Viber') }} 
 where CAST(DATE(date) AS DATE) >='2019-01-01'
 and (cost>0 or clicks>0 or impressions>0)
 group by 1,2,3
)




select distinct
        date
        ,x_publisher
        ,x_campaign_id
        ,x_account_id
       ,spend
       ,clicks
       ,impressions

from (

  select
          x_account_id as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from _7stars
  
  union all
  
  select
          "amobee" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from amobee
  
  union all
  
  select
          "apple" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from apple_bwin_masteracc
  
  union all
  
  select
          "apple" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from apple_gvc_services_ltd
  
  union all
  
  select
          "apple" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from apple_moblinglobalzoomltd
  union all
  
  select
          "apple" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from apple_ppuk
  
  union all
  
  select
          "apple" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from apple_partycasino
  
  union all
  
  select
          "appnexus" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from appnexus
  
  union all
  
  select
          "appnexus" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from appnexus_gvc_tomorrow
  
  union all
  
  select
          "appsflyer_facebook" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from appsflyer_facebook
  
  union all
  
  select
          cast(x_account_id as string) as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from appsflyer_appnetworks
  
  union all
  
  select
          cast(x_account_id as string) as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from appsflyer_appnetworks_pp
  
  union all
  
  select
          "bing" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from bing_ads
  
  union all
  
  select
         "connected_tv" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from connected_tv
  
  union all
  
  select
         "dating_apps" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from dating_apps
  
  union all
  
  select
          "doubleclick" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from doubleclick
  
  union all
  
  select
          "dv360" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from dv360
  
  union all
  
  select
          "engageya" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from engageya
  
  union all
  
  select
          "facebook" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,inline_link_clicks
         ,impressions
  from facebook
  
  union all
  
  select
         "facebook" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,inline_link_clicks
         ,impressions
  from facebook_additional
  
  union all
  
  select
         "forza" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from forza
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_bwin_mcc
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_bwin_performance_max_GR
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_camp_mcc_partypoker
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_party_casino
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_mcc_partycasino
  
  union all
  
  select
          "google" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from google_ads_party_gaming
  
  union all
  
  select
          x_account_id as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from hubble
  
  union all
  
  select
         "missmarcadores" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from missmarcadores
  
  union all
  
  select
          "n365" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from n365
  
  union all
  
  select
          "n365" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from n365_pp
  
  union all
  
  select
         "outbrain" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from outbrain
  
  union all
  
  select
          "smadex" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from smadex
  
  union all
  
  select
          "sky_adsmart" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from sky_adsmart
  
  union all
  
  select
          "snapchat" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from snapchat_ads
  
  union all
  
  select
         "taboola" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from taboola
  
  union all
  
  select
          "tradedesk" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from tradedesk
  
  union all
  
  select
          "twitter" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from twitter
  
  union all
  
  select
          "verizon_dsp" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from verizon_dsp_gvc
  
  union all
  
  select
         "viber" as x_publisher
         ,cast(date as date) as date
         ,cast(x_campaign_id as string) as x_campaign_id
         ,cast(x_account_id as string) as x_account_id
         ,spend
         ,clicks
         ,impressions
  from viber
  )