WITH 
-- `api-project-786064088220.MediaReportingDCM.p_match_table_placements_785192`
-- `api-project-786064088220.MediaReportingDCM.p_match_table_placement_cost_785192`

 click_placement AS (
select distinct
      p.Placement_ID
--       ,p.Placement
      ,p.Placement_Cost_Structure
      ,max(cast(pc.Placement_Rate as int64)/1000000000) as Placement_Rate
      ,pc.Placement_Comments
      ,min(cast(CONCAT(SUBSTR(p.Placement_Start_Date,1,4),"-",SUBSTR(p.Placement_Start_Date,5,2),"-",SUBSTR(p.Placement_Start_Date,7,2)) as date))  as             Placement_Start_Date
      ,max(cast(CONCAT(SUBSTR(p.Placement_End_Date,1,4),"-",SUBSTR(p.Placement_End_Date,5,2),"-",SUBSTR(p.Placement_End_Date,7,2)) as date))  as             
      Placement_End_Date
      
from {{ source('DCM_UK', 'p_match_table_placements_785192') }}  as p

left join {{  source('DCM_UK', 'p_match_table_placement_cost_785192') }}  as pc
on p.Placement_ID=pc.Placement_ID 
where 
p.Placement_Cost_Structure in ('CPC','Flat Rate - Clicks')
and pc.Placement_Rate>0
group by 1,2,4
)

,temp AS
(
select EXTRACT(DATE FROM TIMESTAMP_MICROS(c.Event_time)) as Date
      ,c.Campaign_ID
      ,c.Site_ID_DCM
      ,c.Placement_ID
--       ,p.Placement
      ,p.Placement_Cost_Structure
      ,p.Placement_Rate
      ,p.Placement_Comments 
      ,case when p.Placement_Cost_Structure='Flat Rate - Clicks' then p.Placement_Rate end as Flat_Cost
      ,p.Placement_Start_Date
      ,p.Placement_End_Date
      ,sum(case when c.Event_Sub_Type='CLICK' then 1 else 0 end) as Click

           
from click_placement as p 
inner join {{ source('DCM_UK', 'p_click_785192') }}  as c
on p.Placement_ID=c.Placement_ID 
where  
--i.Event_time BETWEEN 1579305599999999 AND (DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)+1)*86400000000-1
EXTRACT(DATE FROM TIMESTAMP_MICROS(c.Event_time)) >= '2020-01-01'
group by 1,2,3,4,5,6,7,8,9,10
)

,click_cost_placment AS(
select Date
      ,Campaign_ID
      ,Site_ID_DCM
      ,Placement_ID
--       ,Placement
      ,Placement_Start_Date
      ,Placement_End_Date
      ,Placement_Cost_Structure
      ,Placement_Rate
      ,Placement_Comments 
      ,Flat_Cost
      ,Click
      ,sum(case when Placement_Cost_Structure='CPC'
           and Date between Placement_Start_Date and Placement_End_Date 
           then Click*Placement_Rate  end) as CPC_Cost  
from temp
group by 1,2,3,4,5,6,7,8,9,10,11),




impression_placement AS (
select distinct
      p.Placement_ID
--       ,p.Placement
      ,p.Placement_Cost_Structure
      ,max(cast(pc.Placement_Rate as int64)/1000000000) as Placement_Rate
      ,pc.Placement_Comments
      ,min(cast(CONCAT(SUBSTR(p.Placement_Start_Date,1,4),"-",SUBSTR(p.Placement_Start_Date,5,2),"-",SUBSTR(p.Placement_Start_Date,7,2)) as date))  as             Placement_Start_Date
      ,max(cast(CONCAT(SUBSTR(p.Placement_End_Date,1,4),"-",SUBSTR(p.Placement_End_Date,5,2),"-",SUBSTR(p.Placement_End_Date,7,2)) as date))  as             
      Placement_End_Date
      
from {{  source('DCM_UK', 'p_match_table_placements_785192') }} as p

left join {{  source('DCM_UK', 'p_match_table_placement_cost_785192') }}  as pc
on p.Placement_ID=pc.Placement_ID 
where 
p.Placement_Cost_Structure in ('CPM','Flat Rate - Impressions')
and pc.Placement_Rate>0
group by 1,2,4
)

,temp2 AS
(
select EXTRACT(DATE FROM TIMESTAMP_MICROS(i.Event_time)) as Date
      ,i.Campaign_ID
      ,i.Site_ID_DCM
      ,i.Placement_ID
--       ,p.Placement
      ,p.Placement_Cost_Structure
      ,p.Placement_Rate
      ,p.Placement_Comments 
      ,case when p.Placement_Cost_Structure='Flat Rate - Impressions' then p.Placement_Rate end as Flat_Cost
      ,p.Placement_Start_Date
      ,p.Placement_End_Date
      ,sum(case when i.Event_Sub_Type='VIEW' then 1 else 0 end) as Impression

           
from impression_placement as p 
inner join {{  source('DCM_UK', 'p_impression_785192') }}  as i
on p.Placement_ID=i.Placement_ID 
where  
--i.Event_time BETWEEN 1579305599999999 AND (DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)+1)*86400000000-1
EXTRACT(DATE FROM TIMESTAMP_MICROS(i.Event_time)) >= '2020-01-01'
group by 1,2,3,4,5,6,7,8,9,10
),


impression_cost_placment as(
select Date
      ,Campaign_ID
      ,Site_ID_DCM
      ,Placement_ID
--       ,Placement
      ,Placement_Start_Date
      ,Placement_End_Date
      ,Placement_Cost_Structure
      ,Placement_Rate
      ,Placement_Comments 
      ,Flat_Cost
      ,Impression
      ,sum(case when Placement_Cost_Structure='CPM'
           and Date between Placement_Start_Date and Placement_End_Date 
           then (Impression/1000)*Placement_Rate  end) as CPM_Cost  
from temp2
group by 1,2,3,4,5,6,7,8,9,10,11),











placment_level AS (
select x.Date
      ,x.Campaign_ID
      ,x.Site_ID_DCM
      ,x.Placement_ID
      ,x.Placement_Start_Date
      ,x.Placement_End_Date
      ,x.Placement_Cost_Structure
      ,x.Placement_Rate
      ,x.Placement_Comments
      ,c.Flat_Cost as Flat_Cost_Click
      ,i.Flat_Cost as Flat_Cost_Imp
      ,sum(c.Click) as Click
      ,sum(i.Impression) as Impression 
      ,sum(c.CPC_Cost) as CPC_Cost
      ,sum(i.CPM_Cost) as CPM_Cost
from (										
       select 
       Date
      ,Campaign_ID
      ,Site_ID_DCM
      ,Placement_ID
      ,Placement_Start_Date
      ,Placement_End_Date
      ,Placement_Cost_Structure
      ,Placement_Rate
      ,Placement_Comments
       from click_cost_placment
       union all										
       select 
       Date
      ,Campaign_ID
      ,Site_ID_DCM
      ,Placement_ID
      ,Placement_Start_Date
      ,Placement_End_Date
      ,Placement_Cost_Structure
      ,Placement_Rate
      ,Placement_Comments
       from impression_cost_placment
      ) as x

left join click_cost_placment	 as c
   on  x.Date=c.Date 
   and x.Campaign_ID=c.Campaign_ID 
   and x.Site_ID_DCM=c.Site_ID_DCM 
   and x.Placement_ID=c.Placement_ID 
   and x.Placement_Start_Date=c.Placement_Start_Date
   and x.Placement_End_Date=c.Placement_End_Date
   and x.Placement_Cost_Structure=c.Placement_Cost_Structure
   and x.Placement_Rate=c.Placement_Rate
left join impression_cost_placment	 as i
   on  x.Date=i.Date 
   and x.Campaign_ID=i.Campaign_ID 
   and x.Site_ID_DCM=i.Site_ID_DCM 
   and x.Placement_ID=i.Placement_ID 
   and x.Placement_Start_Date=i.Placement_Start_Date
   and x.Placement_End_Date=i.Placement_End_Date
   and x.Placement_Cost_Structure=i.Placement_Cost_Structure
   and x.Placement_Rate=i.Placement_Rate
   and x.Placement_Comments=i.Placement_Comments
group by 1,2,3,4,5,6,7,8,9,10,11
)

select a.Date
      ,a.Campaign_ID
      ,c.Campaign
      ,a.Flat_Cost_Click
      ,a.Flat_Cost_Imp
      ,sum(a.Click) as Click
      ,sum(a.Impression) as Impression
      ,sum(a.CPC_Cost) as CPC_Cost
      ,sum(a.CPM_Cost) as CPM_Cost  
from placment_level as a
left join (
            select distinct a.Campaign_ID,b.Campaign                     
            from (
                   select distinct Campaign_ID,max(_PARTITIONTIME) as max_date
                   from {{ source('DCM_UK', 'p_match_table_campaigns_785192') }}
                   group by 1
           ) as a
           left join
           {{ source('DCM_UK', 'p_match_table_campaigns_785192') }}   as b
            on a.Campaign_ID=b.Campaign_ID and a.max_date=b._PARTITIONTIME
) as  c
ON a.Campaign_id = c.Campaign_id
group by 1,2,3,4,5