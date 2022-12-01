WITH td_deposits AS( SELECT * FROM {{ref('td_deposits')}}),
     gvc_af      AS( SELECT distinct * FROM {{ref('gvc_conversions_appsflyer')}}        WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2021-01-01'),
     lcg_af      AS( SELECT distinct * FROM {{ref('lc_conversions_appsflyer')}}         WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2020-01-20'),
     gvc_dcm     AS( SELECT distinct * FROM {{ref('gvc_conversions_dcm')}}              WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2021-02-11'),
     lcg_dcm     AS( SELECT distinct * FROM {{ref('lc_conversions_dcm')}}               WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2021-02-11' AND REGEXP_CONTAINS(Brand, '(?i)Coral|Ladbrokes') ),
     gvc_ga      AS (SELECT distinct * FROM {{ref('gvc_conversions_analytics_final')}}   WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2021-01-01'),
    --   pp_ga      AS (SELECT distinct * FROM `api-project-786064088220.AttributionMediaReporting_PP.Conversions_Analytics_final`  WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2021-01-01'),
     lcg_ga      AS (SELECT distinct * FROM {{ref('lc_conversions_analytics_final')}}   WHERE REGEXP_CONTAINS(Conversion, 'eposit') AND Date >= '2020-01-20' AND REGEXP_CONTAINS(Brand, '(?i)Coral|Ladbrokes'))

  ,ga_GVC AS
    (
    -- SELECT *, ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Date, Hour, Minute ORDER BY event_value DESC) AS rank
    -- FROM
    --     (
    --     SELECT CustomerID, Brand, Conversion, Date, event_time, Hour, event_value, currency
    --         ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute
    --     FROM pp_ga
    --     )
    -- UNION ALL
    SELECT *, ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Date, Hour, Minute ORDER BY event_value DESC) AS rank
    FROM
        (
        SELECT CustomerID, Brand, Conversion, Date, event_time, Hour, event_value, currency
            ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute
        FROM gvc_ga
        )
    )
, ga_LCG AS
    (
    SELECT *, ROW_NUMBER() OVER ( PARTITION BY CustomerID, Conversion, Date, Hour, Minute ORDER BY event_value DESC) AS rank
    FROM
        (
        SELECT Customer as CustomerID, Brand, Conversion, Date, event_time, Hour, event_value, 'GBP' as currency
            ,CASE WHEN EXTRACT(SECOND FROM Event_time) > 49 THEN EXTRACT(MINUTE FROM Event_time)+1 ELSE EXTRACT(MINUTE FROM Event_time) END AS Minute
        FROM lcg_ga
        )
    )

, ga_af_dcm_deposits as
    (
    select *, row_number() over (partition by Brand order by date, event_time, CustomerID) dep_id
    from
        (
    --GVC
        SELECT CustomerID, Brand, date, event_time, event_value, currency FROM ga_GVC WHERE rank = 1 and conversion = 'Deposit'
        UNION ALL
        SELECT distinct CustomerID, case when REGEXP_CONTAINS(Campaign, '(?i)bwin') then 'Bwin' else Brand end as Brand, date, event_time, event_value, currency
        FROM gvc_dcm
        WHERE ranknew = 1
        UNION ALL
        SELECT distinct CustomerID, Brand, date, event_time, event_value, currency
        FROM gvc_af

    -- LCG
        UNION ALL
        SELECT CASE WHEN IMS_customer_id IS NOT NULL THEN CAST(tt.Customer_ID AS STRING) ELSE CustomerID END AS CustomerID, d.Brand, date, event_time, event_value, 'GBP' as currency
        FROM
            (
            SELECT CustomerID, Brand, date, cast(event_time as time) event_time, event_value FROM lcg_dcm WHERE ranknew = 1
            UNION ALL
            SELECT CustomerID, Brand, date, event_time, dep_amount as event_value FROM lcg_af
            UNION ALL
            SELECT CustomerID, Brand, date, event_time, event_value FROM ga_LCG WHERE rank = 1
            ) d
        LEFT JOIN
            (SELECT distinct * FROM {{ source('other_lists_lc', 'IMS_CustomerID') }} ) tt
        ON
            d.CustomerID = IMS_customer_id
            AND tt.Brand = d.Brand
        WHERE CustomerID IS NOT NULL
        )
    )

, base as
   (
   select
       td.dep_id as td_dep_id,
       gda.dep_id as gda_dep_id,
       td.customerid as td_customerid,
       gda.customerid as gda_customerid,
       td.brand as td_brand,
       gda.brand as gda_brand,
       td.transaction_date as td_date,
       gda.date as gda_date,
       td.transaction_week as td_week,
       gda.week as gda_week,
       td.transaction_time as td_time,
       gda.event_time as gda_time,
       td.transaction_ts as td_ts,
       gda.ts as gda_ts,
       abs(timestamp_diff(gda.ts, td.transaction_ts, second)) as time_diff,
       td.deposit_amt_gbp as td_amt_gbp,
       gda.event_value_gbp as gda_amt_gbp,
       abs(td.deposit_amt_gbp-gda.event_value_gbp) as amt_diff,
       case when td.deposit_amt_gbp = gda.event_value_gbp then 1 else 0 end as amt_match,
       --dataset, ChannelGrouping, medium, source, country, website,
   from
       (
       select
           dep_id,
           src_account_ID as customerid,
           brand,
           transaction_date,
           transaction_ts,
           time(transaction_ts) as transaction_time,
           date_TRUNC(transaction_date, WEEK) as transaction_week,
           deposit_amt_gbp
       from
           td_deposits
       ) td
   left join
       (
        select distinct
            dep_id,
            CustomerID,
            Brand,
            a.date,
            event_time,
            cast(a.date||' '||event_time as timestamp) as ts,
            date_TRUNC(a.date, WEEK) as week,
            event_value,
            a.currency,
            CASE
                WHEN a.currency IS NOT NULL THEN
                    CASE
                        WHEN a.currency = 'GBP' THEN ROUND(event_value,2)
                        WHEN Exchange_rate IS NOT NULL THEN ROUND(event_value * CAST(Exchange_rate AS FLOAT64),2)
                    END
            END AS event_value_GBP,
            --dataset, ChannelGrouping, medium, source, country, website
        from
            ga_af_dcm_deposits a
        left join
            {{ref('dim_exchange_rates')}} xr
        on
            a.currency = xr.currency
            and a.date = xr.Date
       ) gda
   on
       gda.customerid = td.customerid
       and gda.week = td.transaction_week
       and gda.brand = td.brand
   )
, ranked as
    (
    select *, rank() over (partition by td_customerid, gda_customerid, gda_brand, td_brand, gda_week, td_week, td_ts order by time_diff asc, amt_diff asc) as rnk4
    from
        (select *, rank() over (partition by td_customerid, gda_customerid, gda_brand, td_brand, gda_week, td_week, td_ts order by time_diff asc, amt_match desc) as rnk3
        from
            (select *,
                rank() over (partition by td_customerid, gda_customerid, gda_brand, td_brand, gda_week, td_week, td_ts, amt_match order by time_diff asc) as rnk1,
                rank() over (partition by td_customerid, gda_customerid, gda_brand, td_brand, gda_week, td_week, gda_ts, amt_match order by time_diff asc) as rnk2
            from
                base
            )r
        )r2
    )


SELECT * FROM RANKED
