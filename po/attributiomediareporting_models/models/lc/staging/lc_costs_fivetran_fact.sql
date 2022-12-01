with
ppc_apple as(
SELECT  CAST(DATE(a.date) AS DATE) as date,
       a.id x_campaign_id,  
       c.organization_id as x_account_id,
      sum(a.local_spend_amount) as spend ,
      sum(a.taps) as clicks,	
      sum(a.impressions) as  impressions,
      sum(a.conversions) as conversions
      		
FROM {{ source('apple_search_ads', 'campaign_report') }} as a
left join
(select  max(modification_time) as max_date,id,organization_id 
 from {{ source('apple_search_ads', 'campaign_history') }}  group by 2,3) as c
on a.id=c.id

where CAST(DATE(a.date) AS DATE)>='2019-01-01'
group by 1,2,3),

ppc_apple_cashcade as(
SELECT  CAST(DATE(a.date) AS DATE) as date,
       a.campaign_id x_campaign_id,  
       c.organization_id as x_account_id,
      sum(a.local_spend_amount) as spend ,
      sum(a.taps) as clicks,	
      sum(a.impressions) as  impressions,
      sum(a.conversions) as conversions
      		
FROM {{ source('apple_search_ads_cashcade', 'search_term_report') }} 	as a
left join
(select  max(modification_time) as max_date,id,organization_id 
 from {{ source('apple_search_ads_cashcade', 'campaign_history') }} group by 2,3) as c
on a.campaign_id=c.id

where CAST(DATE(a.date) AS DATE)>='2019-01-01'

group by 1,2,3
), 

ppc_bing as(
SELECT  CAST(DATE(date) AS DATE) as date,
       campaign_id x_campaign_id,  
       account_id x_account_id,
      sum(spend) as spend ,
      sum(clicks) as clicks,	
      sum(impressions) as  impressions,
      sum(conversions) as conversions
      		
FROM {{ source('bing_ads', 'campaign_performance_daily_report') }} 	
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

ppc_google as(
SELECT  CAST(DATE(date) AS DATE) as date,
       campaign_id as x_campaign_id   ,  
       customer_id as x_account_id,
      sum(cost) as spend ,
      sum(clicks) as clicks,	
      sum(impressions) as  impressions,
      sum(case when not regexp_contains(campaign_name,'UAC|uac') then conversions else 0 end) as conversions
      		
FROM {{ source('google_ads', 'campaign_stats') }} 
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

display_prog_appnexus as(
SELECT distinct CAST(DATE(day) AS DATE) as date,
       line_item_id as x_campaign_id,   
       advertiser_id   as x_account_id,
       sum(case when 
           (insertion_order_name not like '%7 Stars%' or insertion_order_name not like '%7stars%')
            and LOWER(insertion_order_name) LIKE '%youtube%'         
            then (imps*0.0311/1000)+total_cost_buying_currency
       else total_cost_buying_currency end) as spend ,
       sum(clicks) as clicks,	
       sum(imps) as  impressions
      		
FROM {{ source('fivetran_email', 'appnexus_2') }}  
where CAST(DATE(day) AS DATE)>='2019-01-01'
group by 1,2,3
),

google_display_and_video_360_ladbrokes_il as(
SELECT distinct CAST(DATE(date) AS DATE) as date,
       x_campaign_id,  
       advertiser_id   as x_account_id,
       sum(case when 
           (insertion_order not like '%7 Stars%' or insertion_order not like '%7stars%' or insertion_order not like '%7Stars%' or advertiser not in ('VE - Coral','VE - Ladbrokes'))
            and LOWER(insertion_order) LIKE '%youtube%'
            then ((impressions*0.0311/1000)+total_media_cost_advertiser_currency)
       else total_media_cost_advertiser_currency end) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions,

      		
FROM (SELECT *
       ,case when advertiser in ('Ladbrokes','Coral') then Line_item_id else campaign_id  end as x_campaign_id  
       ,case when advertiser in ('Ladbrokes','Coral') then line_item    else campaign     end as x_campaign_name  
FROM {{ source('google_display_and_video_360', 'dv_360_ladbrokes_il') }} )
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

social_facebook as(
SELECT CAST(DATE(date) AS DATE) AS date,
       campaign_id as x_campaign_id,   
       account_id  as x_account_id,
       sum(spend) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions
      		
FROM {{ source('facebook_ads', 'facebook_ads') }} 
where CAST(DATE(date) AS DATE)>='2019-01-01'
group by 1,2,3
),

social_snapchat as(
SELECT CAST(DATE(date) AS DATE) AS date,
       campaign_id   as x_campaign_id, 
       ad_account_id x_account_id,
       (sum(spend)/1000000) as spend ,
       sum(swipes) as clicks,	
       sum(impressions) as  impressions
      		
FROM {{ source('snapchat_ads', 'campaign_hourly_report') }}  as a

left join	
(select max(updated_at) as max_date,id,ad_account_id from {{ source('snapchat_ads', 'campaign_history') }}  group by 2,3) as c	
on a.campaign_id=c.id	

where CAST(DATE(date) AS DATE)>='2019-01-01'
      AND (impressions>0 or spend>0 or swipes>0) 
group by 1,2,3
),


social_twitter as(
SELECT CAST(DATE(date) AS DATE) AS date,
       campaign_id as x_campaign_id,
       account_id  as x_account_id,
       (sum(billed_charge_local_micro)/1000000) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions
      		
FROM {{ source('twitter_ads', 'campaign_report') }} 
where CAST(DATE(date) AS DATE)>='2019-01-01'
      AND (impressions>0 or billed_charge_local_micro>0 or clicks>0)
group by 1,2,3
),

taboola as(
SELECT CAST(DATE(a.date_time) AS DATE) AS date,
       a.campaign_id   as x_campaign_id, 
       b.account_id x_account_id,
       sum(a.spent) as spend ,
       sum(a.clicks) as clicks,	
       sum(a.impressions) as  impressions
      		
FROM {{ source('taboola', 'campaign_site_day_report') }}  as a
left join	
{{ source('taboola', 'campaign') }}   as b
on a.campaign_id=b.id	
where CAST(DATE(a.date_time) AS DATE)>='2019-01-01'
group by 1,2,3
),

outbrain as(
SELECT CAST(day AS DATE) AS date,
       campaign_id   as x_campaign_id, 
       marketer_id x_account_id,
       sum(spend) as spend ,
       sum(clicks) as clicks,	
       sum(impressions) as  impressions
      		
FROM {{ source('outbrain', 'campaign_report') }} as a

left join	
(select max(last_modified) as max_date,id,marketer_id from {{ source('outbrain', 'campaign_history') }} group by 2,3) as c	
on a.campaign_id=c.id	

where CAST(day AS DATE)>='2019-01-01'
group by 1,2,3
),

tradedesk as(
SELECT  CAST(DATE(date) AS DATE) as date,
       Campaign_ID as x_campaign_id,  
       Advertiser_ID as x_account_id,
      sum(Advertiser_Cost__Adv_Currency_) as spend ,
      sum(Clicks) as clicks,	
      sum(impressions) as  impressions
      		
FROM {{ source('tradedesk_lcg', 'tradedesk_lcg') }} 
group by 1,2,3
),

 dim_date AS(   SELECT *
                    FROM UNNEST(GENERATE_DATE_ARRAY('2021-01-01', DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY), INTERVAL 1 DAY)) AS Date),

        costs AS(
                    select CAST(DATE(Date_DT) AS DATE) AS date
                           ,case when REGEXP_CONTAINS(Campaign_name, '(?i)c:')  THEN SAFE_CAST(SUBSTR(SPLIT(Campaign_name, 'c:'  )[SAFE_OFFSET(1)],1,5) AS INT64) 
                                 when REGEXP_CONTAINS(Campaign_name, '(?i)cid') THEN SAFE_CAST(SUBSTR(SPLIT(Campaign_name, 'cid:')[SAFE_OFFSET(1)],1,5) AS INT64)
                           else Tracker_ID end as x_campaign_id
                           ,Ben_ID as x_account_id
                           ,sum(Cost) as spend 
                           ,0 as clicks	
                           ,0 as  impressions
                           
                    --from `lcg-fivetran-dev.display_partner.display_partner_spend_table` 
                    from {{ source('display_partner', 'display_partner_spend_table') }} 
                    where owner is not null and CAST(DATE(Date_DT) AS DATE)>='2020-01-01'
                     and Cost>0 
                    group by 1,2,3),


display_partner as(
SELECT *
FROM(
      SELECT * EXCEPT(min_date)
               REPLACE( CASE WHEN Spend       IS NULL AND Date > DATE_ADD(min_date, INTERVAL -7 DAY) THEN 0 ELSE Spend       END AS Spend,
                        CASE WHEN clicks      IS NULL AND Date > DATE_ADD(min_date, INTERVAL -7 DAY) THEN 0 ELSE clicks      END AS clicks,
                        CASE WHEN impressions IS NULL AND Date > DATE_ADD(min_date, INTERVAL -7 DAY) THEN 0 ELSE impressions END AS impressions)
      
      FROM(SELECT DISTINCT x_campaign_id, x_account_id, MIN(DATE) min_date FROM costs GROUP BY 1,2) cp
      CROSS JOIN dim_date 
      LEFT JOIN costs USING(Date, x_campaign_id, x_account_id)
      ORDER BY 2,1
     ) WHERE spend IS NOT NULL

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
        "apple" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from ppc_apple

union all

select
        "apple" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from ppc_apple_cashcade

union all

select
        "bing" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from ppc_bing

union all

select
        "google" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from ppc_google

union all

select
        "appnexus" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from display_prog_appnexus

union all

select
        "dv360" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from google_display_and_video_360_ladbrokes_il

union all

select
        "facebook" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from social_facebook

union all

select
        "snapchat" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from social_snapchat

union all

select
        "twitter" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from social_twitter

union all

select
        "display_partner" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from display_partner

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
       "outbrain" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from `lcg-fivetran-dev.fact_campaign.outbrain`

union all

select
        "tradedesk" as x_publisher
       ,cast(date as date) as date
       ,cast(x_campaign_id as string) as x_campaign_id
       ,cast(x_account_id as string) as x_account_id
       ,spend
       ,clicks
       ,impressions
from `lcg-fivetran-dev.fact_campaign.tradedesk`

)