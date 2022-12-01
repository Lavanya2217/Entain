WITH
     Red  AS(   SELECT l.* REPLACE(CASE WHEN source = 'internal' THEN 'CRM - Other'
                                        WHEN LENGTH(Campaign) < 21 THEN l.ChannelGrouping
                                        WHEN campaign_id IS NOT NULL THEN tt.ChannelGrouping ELSE l.ChannelGrouping
                                        END AS ChannelGrouping,
                                   CASE WHEN LENGTH(Campaign) < 21 OR campaign_id IS NULL THEN l.source
                                        ELSE tt.Publisher                                                                                                END AS source,
                                   SPLIT(SPLIT(transaction_id, ':')[SAFE_OFFSET(0)], ',')[SAFE_OFFSET(0)]                                                    AS transaction_id,
                                   TIME(CAST(SUBSTR(event_time,1,2) AS INT64), CAST(SUBSTR(event_time,4,2) AS INT64), CAST(SUBSTR(event_time,7,2) AS INT64)) AS Event_Time,
                                   CASE WHEN dataset ='TD' THEN 100 ELSE lag_hours END AS lag_hours
                                   )
                      , campaign_id
                FROM (SELECT distinct * FROM {{ref('lc_conversions_ledger')}}) l
                LEFT JOIN (SELECT distinct * FROM {{ref('lc_dim_campaigns')}}) tt
                       ON REGEXP_REPLACE(LOWER(campaign), ' ', '') = REGEXP_REPLACE(LOWER(campaign_name), ' ', '')
             ),

   ptnr2  AS(   SELECT STRING_AGG(LOWER(partner), '|') FROM (SELECT distinct partner from  {{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }} )),

     med  AS(   SELECT r.*  REPLACE (CASE WHEN ChannelGrouping = 'Display - Partners' AND tracker_id IS NOT NULL AND NOT REGEXP_CONTAINS(campaign, 'c:|cid')
                                          THEN CONCAT(tracker_id, '_',Partner, '_',p.Brand, '_',Product, '_', campaign) ELSE campaign END AS campaign,
                                     CASE WHEN ChannelGrouping = 'Display - Partners' AND tracker_id IS NOT NULL AND NOT REGEXP_CONTAINS(campaign, 'c:|cid')
                                          THEN partner ELSE source END AS source)
                FROM red r
                LEFT JOIN (SELECT distinct tracker_id, partner, LOWER(LEFT(brand,3)) AS Brand,
                                  CASE WHEN Product_type LIKE '%#%' THEN ''
                                       WHEN Product_type LIKE 'Spo%' THEN 'sprts'
                                       WHEN Product_type LIKE 'Cas%' THEN 'casi'
                                       WHEN Product_type LIKE 'Bin%' THEN 'bngo' END AS Product
                           FROM{{ source('exclusions_lists_lc', 'exclusion_list_Display_Partners') }}) p
                ON wm_tracking = tracker_id),

     led  AS(   SELECT distinct *
                         REPLACE ( CASE WHEN (REGEXP_CONTAINS(Campaign,'(?i)amp|google') OR REGEXP_CONTAINS(source,'(?i)amp|google')
                                              OR REGEXP_CONTAINS(Campaign,'(?i)gads')  )
                                         AND  REGEXP_CONTAINS(ChannelGrouping,'(?i)ppc')                                               THEN 'Google_Ads'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)vod') OR REGEXP_CONTAINS(source,'(?i)vod')                  THEN 'VOD'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)xus') OR REGEXP_CONTAINS(source,'(?i)xus')                  THEN 'AppNexus'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)youtube')                                                   THEN 'YouTube'
                                        WHEN REGEXP_CONTAINS(source,'(?i)facebook') OR REGEXP_CONTAINS(campaign,'(?i)facebook')        THEN 'Facebook'
                                        WHEN REGEXP_CONTAINS(source,'(?i)aboola') OR REGEXP_CONTAINS(campaign,'(?i)aboola')            THEN 'Taboola'
                                        WHEN REGEXP_CONTAINS(source,'(?i)erizon|veriz|display-ver')
                                          OR REGEXP_CONTAINS(campaign,'(?i)erizon')                                                    THEN 'Verizon'
                                        WHEN REGEXP_CONTAINS(source,'(?i)TradeDesk|ttd') OR REGEXP_CONTAINS(campaign,'(?i)ttd')        THEN 'TradeDesk'
                                        WHEN REGEXP_CONTAINS(source,'(?i)fluent')    OR REGEXP_CONTAINS(campaign,'(?i)fluent')         THEN 'Fluent'
                                        WHEN REGEXP_CONTAINS(source,'(?i)koneo')     OR REGEXP_CONTAINS(campaign,'(?i)koneo')          THEN 'Koneo'
                                        WHEN REGEXP_CONTAINS(source,'(?i)utbrai')    OR REGEXP_CONTAINS(campaign,'(?i)utbrai')         THEN 'Outbrain'
                                        WHEN REGEXP_CONTAINS(source,'(?i)clickwork') OR REGEXP_CONTAINS(campaign,'(?i)clickwork')      THEN 'Clickwork7'
                                        WHEN REGEXP_CONTAINS(source,'(?i)glispa')    OR REGEXP_CONTAINS(campaign,'(?i)glispa')         THEN 'Glispa'
                                        WHEN REGEXP_CONTAINS(source,'(?i)fyber.com') OR REGEXP_CONTAINS(campaign,'(?i)fyber')          THEN 'Fyber'
                                        WHEN REGEXP_CONTAINS(source,'(?i)hingortw')  OR REGEXP_CONTAINS(campaign,'(?i)thingortwo')     THEN 'Thingortwo'
                                        WHEN REGEXP_CONTAINS(source,'(?i)snapchat')  OR REGEXP_CONTAINS(campaign,'(?i)snap')           THEN 'Snapchat'
                                        WHEN REGEXP_CONTAINS(source,'(?i)wakeapp')   OR REGEXP_CONTAINS(campaign,'(?i)wakeapp')        THEN 'Wakeapp'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)360')     OR REGEXP_CONTAINS(source,'(?i)360')              THEN 'DV360'
                                        WHEN NOT REGEXP_CONTAINS(source,'(?i)google')   AND REGEXP_CONTAINS(source,'(?i)_ads')         THEN REGEXP_REPLACE(source, '(?i)_ads', '')
                                        WHEN (REGEXP_CONTAINS(source, 'display-') AND NOT REGEXP_CONTAINS(source, 'other') )
                                          OR REGEXP_CONTAINS(source, 'partner-') THEN INITCAP(SPLIT(Source, '-')[SAFE_OFFSET(1)])
                                   ELSE source END AS source,
                                   CASE WHEN REGEXP_CONTAINS(campaign, '(?i)dv360|appnexus') THEN TRIM(SPLIT(campaign,'ID_')[OFFSET(0)])
                                        WHEN REGEXP_CONTAINS(campaign, '(?i)cro_test')       THEN CONCAT(campaign,'_', SPLIT(source, '-')[OFFSET(0)])
                                        ELSE campaign
                                   END AS campaign,
                                   CASE WHEN REGEXP_CONTAINS(Campaign,'(?i)vod') OR REGEXP_CONTAINS(source,'(?i)vod')                  THEN 'Display - VOD'
                                   ELSE ChannelGrouping END AS ChannelGrouping)
                FROM med),

     raa  AS(   SELECT *
                FROM led
                WHERE Conversion NOT IN ('Bet', 'Registration') AND Dataset <> 'GA'
                ),

     maa  AS(   SELECT *
                FROM raa

              UNION ALL
                SELECT *
                FROM led
                WHERE Conversion NOT IN ('Bet', 'Registration') AND Dataset = 'GA' AND ChannelGrouping IN( 'Direct' , 'Display - Partners')
                  AND CustomerID IN (SELECT distinct CustomerID FROM raa)
              ),

     aaa  AS(   SELECT *,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, Minute
                                               ORDER BY VisitID, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC) AS rank
                FROM led
                WHERE Conversion NOT IN ('Bet', 'Registration')
                  AND (ChannelGrouping IN ('blue', 'Direct') OR Dataset <> 'GA')
                ),

     saa  AS(   SELECT CASE WHEN Prev = 0 OR aft = 0 THEN 0 ELSE NULL END AS lag, *,
                       CASE WHEN event_value IS NULL AND prev = 0 THEN LAG (event_value) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                            WHEN event_value IS NULL AND aft  = 0 THEN LEAD(event_value) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                       ELSE event_value END AS new_event_value

                FROM(
                       SELECT TIME_DIFF(LAG (event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, MINUTE) prev,
                              TIME_DIFF(LEAD(event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, MINUTE) aft,*
                       FROM maa)
               ),

     vie  AS(   SELECT * EXCEPT(newcode,dup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, newcode--lag, CAST(event_value AS STRING)
                                                  ORDER BY dup DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, VisitID, Event_value DESC) AS rank
                FROM( SELECT CASE WHEN prev = 0 THEN CONCAT(code, '_', LAG(rank) OVER (PARTITION BY CustomerID, Conversion, Date, code ORDER BY rank))
                             ELSE CONCAT(code, '_', rank)
                             END AS newcode, * EXCEPT (rank, code)
                      FROM(
                             SELECT ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, Conversion, hour ORDER BY Event_time) AS rank
                                    ,CONCAT(Brand, Customerid, date, Conversion, hour, new_event_value) as code
                                    ,* EXCEPT(new_event_value) REPLACE(new_event_value AS event_value)
                                    ,CASE WHEN channelgrouping = 'Direct' THEN 1
                                          WHEN dataset = 'TD' THEN 0
                                          ELSE 2 END AS dup
                             FROM saa
                             WHERE lag = 0))
               ),

    alle  AS(  SELECT *  REPLACE(CASE WHEN Conversion IN ('Bet', 'Registration') THEN Conversion ELSE 'Deposit' END AS Conversion)
                       , CASE WHEN ChannelGrouping != 'Direct' OR (REGEXP_CONTAINS(ChannelGrouping, 'artner') AND Dataset = 'GA') THEN 1 END AS non_dup
               FROM(
                       SELECT * EXCEPT(lag, aft, prev, rank)
                        FROM vie WHERE rank=1

                      UNION ALL
                       SELECT * EXCEPT(lag, new_event_value, aft, prev)
                       FROM saa WHERE lag <> 0 OR lag IS NULL

                      UNION ALL
                       SELECT * EXCEPT(rank)
                       FROM(
                             SELECT *, ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, Event_time
                                                               ORDER BY Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC) AS rank
                             FROM led
                             WHERE Conversion NOT IN ('Bet', 'Registration') AND ((Dataset = 'GA' AND ChannelGrouping != 'Direct')
                                OR (ChannelGrouping = 'Direct' AND CustomerID NOT IN (SELECT distinct CustomerID FROM raa)) )
                           ) WHERE rank =1
                  )),


     regs  AS(  SELECT * EXCEPT(ord) ,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID
                                                              ORDER BY ord DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC) AS rank
                FROM (SELECT *, IF(Channelgrouping = 'Direct',0,1) AS ord FROM led)
                WHERE Conversion = 'Registration'
              ),


     bets  AS(  SELECT * EXCEPT(non_dup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, transaction_id
                                                                  ORDER BY non_dup DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC) AS rank
                FROM (SELECT *, CASE WHEN ChannelGrouping != 'Direct' THEN 1 END AS non_dup
                      FROM led WHERE Conversion = 'Bet')
              ),


     kiko  AS(  SELECT * EXCEPT(rank3)
                           REPLACE(CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN 'FTD'       ELSE Conversion END AS Conversion,
                                   CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_0_type ELSE NULL       END AS pNGR_0_type,
                                   CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_0      ELSE NULL       END AS pNGR_0,
                                   CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_21     ELSE NULL       END AS pNGR_21)

                FROM(
                      SELECT *
                      FROM(
                            SELECT * EXCEPT(rank2, Minute)
                                   ,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY Hour, Minute, Click_conversion DESC, Lag_hours, Event_value DESC) AS rank3
                            FROM(
                                  SELECT * EXCEPT(non_dup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, Minute
                                                                                    ORDER BY non_dup DESC, Click_conversion DESC, Lag_hours, Event_value DESC, keyword DESC, Source DESC, visitid ASC) AS rank2
                                  FROM alle
                                  )
                            WHERE rank2 =1
                           ) r
                      UNION ALL
                      SELECT * EXCEPT(Minute, rank), 0 AS rank3 FROM Bets WHERE rank = 1
                      UNION ALL
                      SELECT * EXCEPT(Minute, rank), 0 AS rank3 FROM Regs WHERE rank = 1) s
               )


,    chan AS(   SELECT *
                FROM(
                      SELECT *, ROW_NUMBER() OVER (PARTITION BY Brand, campaign ORDER BY po ASC, ChannelGrouping) rank
                      FROM(
                           SELECT DISTINCT brand, campaign, ChannelGrouping,
                                  CASE WHEN REGEXP_CONTAINS(ChannelGrouping, 'Direct')   THEN 3
                                       WHEN REGEXP_CONTAINS(ChannelGrouping, 'Referral') THEN 2
                                       WHEN REGEXP_CONTAINS(ChannelGrouping, 'Display')  THEN 1
                                  ELSE 0 END AS po
                           FROM(
                                SELECT *, COUNT(distinct ChannelGrouping) OVER (PARTITION BY brand, campaign ) cnt
                                FROM {{ref('lc_conversions_ledger')}}
                                WHERE NOT REGEXP_CONTAINS(ChannelGrouping, '(?i)crm') AND NOT REGEXP_CONTAINS(campaign , 'not set|notset|null')
                          ) WHERE cnt > 1
                             )

                    )WHERE rank =1
                )

,    muu AS(   SELECT *, CASE WHEN Conversion = 'FTD'          AND ChannelGrouping  = 'Direct' AND brand IN ('Ladbrokes', 'Coral') AND CustomerID <> 'null' THEN 1
                              WHEN Conversion = 'Registration' AND ChannelGrouping != 'Direct' AND brand IN ('Ladbrokes', 'Coral') AND CustomerID <> 'null' THEN 2 END AS transf
                       ,DATETIME_ADD(DATETIME_ADD(DATETIME_ADD(CAST(date AS datetime), INTERVAL EXTRACT(HOUR FROM event_time) HOUR ), INTERVAL EXTRACT(MINUTE FROM event_time) MINUTE), INTERVAL EXTRACT(SECOND FROM event_time) SECOND) AS Datetime
               FROM(
                      SELECT kiko.* REPLACE(CASE WHEN chan.ChannelGrouping IS NOT NULL THEN chan.ChannelGrouping ELSE kiko.ChannelGrouping END AS ChannelGrouping,
                                            REGEXP_REPLACE(kiko.campaign, '  | ', '')  AS Campaign)
                      FROM kiko
                      LEFT JOIN chan
                      ON kiko.Brand = chan.Brand AND kiko.campaign = chan.campaign))

SELECT * REPLACE(INITCAP(REGEXP_REPLACE(Source, '(?i)appliftcustom', 'Applift')) AS source,
                 CASE WHEN dataset ='TD' THEN 0 ELSE lag_hours END AS lag_hours)
FROM(
    SELECT f.* EXCEPT(transf, datetime)
               REPLACE(CASE WHEN f.transf=1 AND t.ChannelGrouping  IS NOT NULL THEN t.ChannelGrouping  ELSE f.ChannelGrouping  END AS ChannelGrouping,
                       CASE WHEN f.transf=1 AND t.medium           IS NOT NULL THEN t.medium           ELSE f.medium           END AS medium,
                       CASE WHEN f.transf=1 AND t.source           IS NOT NULL THEN t.source           ELSE f.source           END AS source,
                       CASE WHEN f.transf=1 AND t.VisitId          IS NOT NULL THEN t.VisitId          ELSE f.VisitId          END AS VisitId,
                       CASE WHEN f.transf=1 AND t.Campaign         IS NOT NULL THEN t.Campaign         ELSE f.Campaign         END AS Campaign,
                       CASE WHEN f.transf=1 AND t.Lag_days         IS NOT NULL THEN t.Lag_days         ELSE f.Lag_days         END AS Lag_days,
                       CASE WHEN f.transf=1 AND t.Lag_hours        IS NOT NULL THEN t.Lag_hours        ELSE f.Lag_hours        END AS Lag_hours,
                       CASE WHEN f.transf=1 AND t.View_conversion  IS NOT NULL THEN t.View_conversion  ELSE f.View_conversion  END AS View_conversion,
                       CASE WHEN f.transf=1 AND t.Click_conversion IS NOT NULL THEN t.Click_conversion ELSE f.Click_conversion END AS Click_conversion,
                       CASE WHEN f.transf=1 AND t.Conv_medium      IS NOT NULL THEN t.Conv_medium      ELSE f.Conv_medium      END AS Conv_medium,
                       CASE WHEN f.transf=1 AND t.Dataset          IS NOT NULL THEN t.Dataset          ELSE f.Dataset          END AS Dataset,
                       CASE WHEN f.transf=1 AND t.adContent        IS NOT NULL THEN t.adContent        ELSE f.adContent        END AS adContent,
                       CASE WHEN f.transf=1 AND t.wm_tracking      IS NOT NULL THEN t.wm_tracking      ELSE f.wm_tracking      END AS wm_tracking,
                       CASE WHEN f.transf=1 AND t.campaignId       IS NOT NULL THEN t.campaignId       ELSE f.campaignId       END AS campaignId,
                       CASE WHEN f.transf=1 AND t.keyword          IS NOT NULL THEN t.keyword          ELSE f.keyword          END AS keyword,
                       CASE WHEN f.transf=1 AND t.campaign_id      IS NOT NULL THEN t.campaign_id      ELSE f.campaign_id      END AS campaign_id)

              ,CASE WHEN f.Conversion = 'FTD' THEN
                    CASE WHEN f.transf=1 AND t.ChannelGrouping IS NOT NULL THEN 'reg' ELSE 'ftd' END
               END AS FTD_attributed_channel

    FROM muu f
    LEFT JOIN (SELECT * FROM muu WHERE transf = 2) t
           ON f.customerID = t.customerID
          AND f.Brand= t.Brand
          AND DATE_DIFF(f.datetime, t.datetime, MINUTE) < 72*60
  )
