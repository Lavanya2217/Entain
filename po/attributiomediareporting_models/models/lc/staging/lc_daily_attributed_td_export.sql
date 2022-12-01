SELECT 
Brand as Brand,
Date as Date, 
Hour as Hour,
Event_Time as Event_Time,
ChannelGrouping as ChannelGrouping,
Source as Source,
VisitId as VisitId,
Campaign as Campaign,
CustomerID as CustomerID,
Conversion as Conversion,
Lag_days as Lag_days,
Lag_hours as Lag_hours,
View_conversion as View_conversion,
Click_conversion as Click_conversion,
Conv_medium as Conv_medium,
Dataset as Dataset,
adContent as adContent,
event_value as event_value,
transaction_id as transaction_id,
FTD_date as FTD_date,
pNGR_0 as pNGR_0,
keyword as keyword,
Spend as Spend,
GA_wm_track as GA_wm_track,
Campaign_name as Campaign_name,
LCBrand as LCBrand,
SubBrand as SubBrand,
Geoinfo as Geoinfo,
Day as Day,
Month as Month,
Year as Year,
Language as Language,
Objective as Objective,
Channel as Channel,
Offer as Offer,
Type as Type, 
Platform as Platform,
Agency as Agency,
Targeting as Targeting,
Strategy as Strategy,
Medium as Medium,
Campaign_Id as Campaign_Id,
IA_tracking as IA_tracking,
WM_tracking as WM_tracking,
Buy_Type as Buy_Type,
KW_Type as KW_Type,
KW_Theme as KW_Theme,
Event as Event,
Game as Game

FROM (SELECT * FROM {{ref('lc_unique_conversions_view')}}
      WHERE date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      
    --   UNION ALL
      
    --   SELECT * FROM `api-project-786064088220.AttributionMediaReporting_GVC_Dev.Unique_Conversions_view` 
    --   WHERE NOT REGEXP_CONTAINS(Brand, '(?i)bwin|eleven') AND date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      
      )