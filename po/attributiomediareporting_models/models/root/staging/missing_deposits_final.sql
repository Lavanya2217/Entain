with  td_deposits as (select * from {{ref('td_deposits')}}),
           ranked as (select * from {{ref('missing_deposits')}})
, matched as
    (
    select
        td_dep_id,
        gda_dep_id,
        td_customerid	,
        gda_customerid	,
        td_brand	,
        gda_brand	,
        td_date	,
        gda_date	,
        td_time	,
        gda_time	,
        time_diff	,
        td_amt_gbp	,
        gda_amt_gbp	,
        amt_diff,
        amt_match,
        --dataset, ChannelGrouping, medium, source, country, website,
        rnk1,rnk2,rnk3,rnk4
    from
        ranked
    where
        rnk1=1 and rnk2=1 and rnk3=1 and rnk4=1 and amt_match = 1 and gda_customerid is not null and ceil(time_diff/60)<=30
    union distinct
    select
        td_dep_id,
        gda_dep_id,
        td_customerid	,
        gda_customerid	,
        td_brand	,
        gda_brand	,
        td_date	,
        gda_date	,
        td_time	,
        gda_time	,
        time_diff	,
        td_amt_gbp	,
        gda_amt_gbp	,
        amt_diff,
        amt_match,
        --dataset, ChannelGrouping, medium, source, country, website,
        rnk1,rnk2,rnk3,rnk4
    from
        ranked
    where
        rnk1=1 and rnk2=1 and rnk3=1 and rnk4=1 and amt_match = 0 and gda_customerid is not null and ceil(time_diff/60)<=30
    )
, unmatched_td_deposits as
    (
    select
        customerid,
        brand,
        transaction_date,
        transaction_time,
        deposit_amt_gbp
    from
        (
        select
            td.customerid,
            td.brand,
            td.transaction_date,
            td.transaction_time,
            td.deposit_amt_gbp,
            m.td_customerid	,
            m.td_brand	,
            m.td_date	,
            m.td_time	,
            m.td_amt_gbp
        from
            (
            select
                src_account_ID as customerid,
                brand,
                transaction_date,
                time(transaction_ts) as transaction_time,
                deposit_amt_gbp
            from
                td_deposits
            ) td
        left join
            matched m
        on
            td.customerid = m.td_customerid
            and td.brand = m.td_brand
            and td.transaction_date = m.td_date
            and td.transaction_time = m.td_time
            and td.deposit_amt_gbp = m.td_amt_gbp
        ) q
    where td_customerid is null
    )
, ga_sessions as
    (
    select
            fullVisitorId,
            visitid,
            max(customerid) customerid,
            brand,
            cast(visitStartTime as timestamp) as visitStartTime,
            cast(visitEndTime as timestamp) as visitEndTime,
            max(case when channelGrouping_rank = 1 then channelGrouping end) as channelGrouping,
            max(campaign) as campaign,
            max(campaignid) as campaignid,
            max(case when source_rank = 1 then source end) as source,
            max(case when medium_rank = 1 then medium end) as medium,
            max(case when adContent_rank = 1 then adContent end) as adContent,
            max(device) as device,
            max(case when keyword_rank = 1 then keyword end) as keyword,
            max(gclid) as gclid,
            max(website) as website,
            max(currency) as currency,
            max(case when country_rank = 1 then country end) as country,
            max(tracker) as tracker
        from
            (
            select
                fullVisitorId,
                customerid,
                brand,
                VisitId,
                date,
                visitStartTime,
                visitEndTime,
                channelGrouping,
                case when ifnull(channelGrouping,'') in ('', '(Other)') then 1 else 0 end as channelGrouping_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(channelGrouping,'') in ('', '(Other)') then 1 else 0 end asc)as channelGrouping_rank,
                campaign,
                campaignId,
                source,
                case when ifnull(source,'') in ('', '(not set)') then 2 when source = '(direct)' then 1 else 0 end as source_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(source,'') in ('', '(not set)') then 2 when source = '(direct)' then 1 else 0 end asc)as source_rank,
                medium,
                case when ifnull(medium,'') in ('', '(not set)', '(none)') then 1 else 0 end as medium_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(medium,'') in ('', '(not set)', '(none)') then 1 else 0 end asc) as medium_rank,
                adContent,
                case when ifnull(adContent,'') in ('', '(not set)') then 1 else 0 end as adContent_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(adContent,'') in ('', '(not set)') then 1 else 0 end asc) as adContent_rank,
                device,
                keyword,
                case when ifnull(keyword,'') in ('', '(not set)') then 2 when keyword in ('(content targeting)', '(automatic matching)') then 1 else 0 end as keyword_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(keyword,'') in ('', '(not set)') then 2 when keyword in ('(content targeting)', '(automatic matching)') then 1 else 0 end asc) as keyword_rank,
                gclid,
                website,
                currency,
                country,
                case when ifnull(country,'') in ('', 'not available in the datalayer') then 1 else 0 end as country_tier,
                rank() over (partition by fullVisitorId, brand, visitid order by case when ifnull(country,'') in ('', 'not available in the datalayer') then 1 else 0 end asc) as country_rank,
                tracker
            from
                (select distinct * from {{ref('dim_ga_sessions')}})
            )q
        group by
            fullVisitorId,
            brand,
            visitid,
            visitStartTime,
            visitEndTime
    )
, joined as
    (
    select
        a.customerid,
        a.brand,
        transaction_date,
        transaction_time,
        cast(a.transaction_date||' '||transaction_time as timestamp) as td_ts,
        deposit_amt_gbp,
        visitStartTime,
        visitEndTime,
        VisitId,
        channelGrouping,
        campaign,
        campaignId,
        source,
        medium,
        adContent,
        device,
        keyword,
        gclid,
        website,
        currency,
        country,
        tracker,
        row_number() over (partition by a.customerid, a.brand, transaction_date,	transaction_time,	deposit_amt_gbp
                    order by
                        case when ifnull(channelGrouping,'') in ('Direct','', '(Other)') then 1 else 0 end
                        , case when ifnull(campaign,'') in ('Direct','', '(not set)') then 1 else 0 end
                        , case when ifnull(source,'') in ('Direct','', '(not set)') then 2 when source = '(direct)' then 1 else 0 end
                        , case when ifnull(medium,'') in ('Direct','', '(not set)', '(none)') then 1 else 0 end
                        , case when ifnull(adContent,'') in ('Direct','', '(not set)') then 1 else 0 end
                        , case when ifnull(keyword,'') in ('Direct','', '(not set)') then 2 when keyword in ('(content targeting)', '(automatic matching)') then 1 else 0 end
                        , case when ifnull(country,'') in ('Direct','', 'not available in the datalayer') then 1 else 0 end
                    ) dedupe_rank,
         --case when cast(a.transaction_date||' '||transaction_time as timestamp) between b.visitStartTime and b.visitEndTime then 1 else 0 end as inside_ind,
         case when b.VisitId is not null then 1 else 0 end as match_ind,
         --case when a.brand = b.brand then 1 else 0 end as brand_match_ind
    from
        unmatched_td_deposits a
    left join
        ga_sessions b
    on
        a.customerid = b.customerid
        --and cast(a.transaction_date||' '||transaction_time as timestamp) between b.visitStartTime and b.visitEndTime
        and cast(a.transaction_date||' '||transaction_time as timestamp) between timestamp_add(b.visitStartTime, interval -60  minute) and timestamp_add(b.visitEndTime, interval 60 minute)
        and a.brand = b.brand
    )
    select
        Brand,
        transaction_date as date,
        extract(hour from transaction_time) as hour,
        transaction_time as event_time,
        case when match_ind = 0 then 'Direct' else channelGrouping end as channelGrouping,
        medium,
        source,
        VisitId,
        visitStartTime,
        visitEndTime,
        Campaign,
        CustomerID,
        'Deposit' as Conversion,
        0 as Lag_days,
        0 as Lag_hours,
        0 as View_conversion,
        1 as Click_conversion,
        'Web' as Conv_medium,
        'TD' as Dataset,
        adcontent,
        keyword,
        deposit_amt_gbp as event_value,
        currency,
        '' as transaction_id,
        SAFE_CAST(tracker AS INT64) as wm_tracking,
        campaignid,
        country,
        website,
        gclid,
        CASE WHEN EXTRACT(SECOND FROM transaction_time) > 49 THEN EXTRACT(MINUTE FROM transaction_time)+1 ELSE EXTRACT(MINUTE FROM transaction_time) END AS Minute,
        match_ind
     from joined where dedupe_rank = 1
