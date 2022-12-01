select
        start_date,
        end_date,
        publisher,
        brand,
        country,
        currency,
        term,
        wmid, 
        partner_name,
        campaign_name,
        spend
  
from (

select 
       "appnetworks" as publisher
       ,"FoxyBingo" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_spend) as spend
from {{ source('display_partner', 'FB_partners_historical_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"FoxyCasino" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_spend) as spend
from {{ source('display_partner', 'FC_partners_historical_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaBingo" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_spend) as spend
from {{ source('display_partner', 'GB_partners_historical_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaSpins" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_spend) as spend
from {{ source('display_partner', 'GS_partners_historical_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaCasino" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_spend) as spend
from {{ source('display_partner', 'GC_partners_historical_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"FoxyBingo" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_Spend) as spend
from {{ source('display_partner', 'FB_partners_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"FoxyGames" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_Spend) as spend
from {{ source('display_partner', 'FG_partners_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaBingo" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_Spend) as spend
from {{ source('display_partner', 'GB_partners_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaCasino" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_Spend) as spend
from {{ source('display_partner', 'GC_partners_2021') }}
group by 1,2,3,4,5,6,7,8,9,10

union all 

select 
       "appnetworks" as publisher
       ,"GalaSpins" as brand
       ,cast(start_date as date) as start_date
       ,cast(end_date as date) as end_date
       ,cast(country as string) as country
       ,cast(currency as string) as currency
       ,cast(term as string) as term
       ,cast(wmid as string) as wmid
       ,cast(partner_name as string) as partner_name
       ,cast(campaign_name as string) as campaign_name
       ,sum(actual_Spend) as spend
from {{ source('display_partner', 'GS_partners_2021') }} 
group by 1,2,3,4,5,6,7,8,9,10
)





















