with spend as (
select distinct
      p.Placement_ID
,Site_ID_DCM

      ,p.Placement_Cost_Structure
      ,max(cast(pc.Placement_Rate as int64)/1000000000) as Placement_Rate
      ,min(cast(CONCAT(SUBSTR(p.Placement_Start_Date,1,4),"-",SUBSTR(p.Placement_Start_Date,5,2),"-",SUBSTR(p.Placement_Start_Date,7,2)) as date))  as             Placement_Start_Date
      ,max(cast(CONCAT(SUBSTR(p.Placement_End_Date,1,4),"-",SUBSTR(p.Placement_End_Date,5,2),"-",SUBSTR(p.Placement_End_Date,7,2)) as date))  as             
      Placement_End_Date
from {{ source('DCM_GVC', 'p_match_table_placements_8807') }}  as p

left join {{ source('DCM_GVC', 'p_match_table_placement_cost_8807') }}   as pc
on p.Placement_ID=pc.Placement_ID 

group by 1,2,3
)
,
impression as (
select EXTRACT(DATE FROM TIMESTAMP_MICROS(i.Event_time)) as Date
      ,i.Campaign_ID
      ,i.Placement_ID
,Site_ID_DCM
      ,sum(case when i.Event_Sub_Type='VIEW' then 1 else 0 end) as Impression


from  {{ source('DCM_GVC', 'p_impression_8807') }}  as i
where  EXTRACT(DATE FROM TIMESTAMP_MICROS(i.Event_time)) >= '2020-01-01'
group by 1,2,3,4
)
,
click as (
select EXTRACT(DATE FROM TIMESTAMP_MICROS(c.Event_time)) as Date
      ,c.Campaign_ID
      , placement_id
      ,Site_ID_DCM
,sum(case when c.Event_Sub_Type='CLICK' then 1 else 0 end) as Click
 from {{ source('DCM_GVC', 'p_click_8807') }}  c
where  EXTRACT(DATE FROM TIMESTAMP_MICROS(c.Event_time)) >= '2020-01-01'
group by 1,2,3,4
)
, daily as(
select i.placement_id,Placement_rate,count(distinct i.date), Placement_rate/count(distinct i.date) as dailycost 
from impression i left join click c on i.placement_id=c.placement_id and i.date=c.date
left join spend as s on i.Site_ID_DCM=s.Site_ID_DCM
where Placement_Cost_Structure in ('Flat Rate - Impressions','Flat Rate - Clicks')
group by 1,2
)

select a.date, 
b.Brand,
Site_DCM,
a.Campaign_ID,
b.Campaign
,sum(impression)  as impression
,sum(click)  as click
,sum(cost) as cost
from
(
    select i.date,i.Campaign_ID,i.Site_ID_DCM
,impression,c.click, 
    sum(case when i.Date between Placement_Start_Date and Placement_End_Date and trim(Placement_Cost_Structure) in ('CPM') then (Impression/1000)*s.Placement_Rate  
             when i.Date between Placement_Start_Date and Placement_End_Date and trim(Placement_Cost_Structure) in ('CPC') then Click*s.Placement_Rate 
             when s.Placement_Cost_Structure='Flat Rate - Impressions' then d.dailycost
             when s.Placement_Cost_Structure='Flat Rate - Clicks' then d.dailycost
            end) as cost
           
from impression i left join click c on i.placement_id=c.placement_id and i.date=c.date
left join spend as s on i.Site_ID_DCM=s.Site_ID_DCM
left join daily d on i.placement_id=d.placement_id
group by 1,2,3,4,5
) A
 left join (
              select distinct a.Campaign_ID,b.Campaign  ,ifnull(a.brand,c.brand) as Brand              
             from (
                     select distinct Campaign_ID,case when regexp_contains(campaign, '(?i)bwin')               then 'Bwin'
              when (regexp_contains(campaign, '(?i)partypoker|prtypkr|Poker') or left(campaign,2)='PP')then 'Party Poker'
              when regexp_contains(campaign, '(?i)partycas|prtycas|Party Casino')   then 'Party Casino'
              when regexp_contains(campaign, '(?i)Sportingbet|sptbet') then 'Sportingbet'
              when regexp_contains(campaign, '(?i)Foxy') then 'Foxy Bingo'
              when regexp_contains(campaign, '(?i)Gala Bingo') then 'Gala Bingo'
              when regexp_contains(campaign, '(?i)Cheeky Bingo') then 'Cheeky Bingo'
              when regexp_contains(campaign, '(?i)GIOCO') then 'GIOCO DIGITALE'
              end as brand,max(_PARTITIONTIME) as max_date
                     from {{ source('DCM_GVC', 'p_match_table_campaigns_8807') }}  
                     where Campaign_End_Date>='2020-01-01'
                     group by 1,2
             ) as a
             left join
              {{ source('DCM_GVC', 'p_match_table_campaigns_8807') }}    as b
              on a.Campaign_ID=b.Campaign_ID and a.max_date=b._PARTITIONTIME
              left join (SELECT distinct  Campaign_Id,
  max(case when regexp_contains(Ad, '(?i)bwin')               then 'Bwin'
              when regexp_contains(Ad, '(?i)partypoker|prtypkr') then 'Party Poker'
              when regexp_contains(Ad, '(?i)partycas|prtycas|Party Casino')   then 'Party Casino'
             when regexp_contains(Ad, '(?i)Sportingbet|sptbet') then 'Sportingbet'
              when regexp_contains(Ad, '(?i)Foxy') then 'Foxy Bingo'
              when regexp_contains(Ad, '(?i)Gala Bingo') then 'Gala Bingo'
              when regexp_contains(Ad, '(?i)Cheeky Bingo') then 'Cheeky Bingo'
              when regexp_contains(Ad, '(?i)GIOCO') then 'GIOCO DIGITALE'
              end) as brand
  from {{ source('DCM_GVC', 'p_match_table_ads_8807') }} 
 group by 1) as c on a.campaign_id=c.campaign_id
  ) as  b
  ON a.Campaign_id = b.Campaign_id
  left join 
  (select distinct Site_ID_DCM,Site_DCM from  {{ source('DCM_GVC', 'p_match_table_sites_8807') }})
  s on a.Site_ID_DCM=s.Site_ID_DCM
  where Brand is not null
  group by 1,2,3,4,5

