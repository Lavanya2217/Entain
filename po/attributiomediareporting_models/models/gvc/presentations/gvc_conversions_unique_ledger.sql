WITH
   ptnr2  AS(   SELECT STRING_AGG(LOWER(partner), '|') FROM (SELECT distinct publisher AS partner FROM {{ref('gvc_dim_campaigns')}}
                                                             WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND ChannelGrouping = 'Display - Partners')),

     med  AS(   SELECT r.* EXCEPT (dup_rank)
                           REPLACE (REGEXP_REPLACE(TRIM(country), '(?i)gb', 'UK')                          AS country,
                                    SPLIT(SPLIT(transaction_id, ':')[SAFE_OFFSET(0)], ',')[SAFE_OFFSET(0)] AS transaction_id,
                                    CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Partner') AND tracker_id IS NOT NULL THEN partner ELSE source          END AS source,
                                    CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Partner') AND wm_tracking IS NULL AND REGEXP_CONTAINS(SUBSTR(Campaign, 1,7), '[0-9]{7}')
                                         THEN SAFE_CAST(SUBSTR(Campaign, 1,7) AS INT64) ELSE wm_tracking END AS wm_tracking,
                                    CASE WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Partner') AND tracker_id IS NOT NULL
                                          AND( (NOT REGEXP_CONTAINS(campaign, '(?i)c:|cid|Wakeapp') AND LENGTH(campaign)<12) OR campaign IS NULL)
                                         THEN CONCAT(wm_tracking, '_',Partner, '_', IFNULL(campaign,'(notset)'))
                                         ELSE campaign END AS campaign,
                                    CASE WHEN dataset ='TD' THEN 100 ELSE lag_hours END AS lag_hours)

                FROM {{ref('gvc_conversions_ledger')}} r
                LEFT JOIN (SELECT distinct SAFE_CAST(campaign_id AS INT64) AS tracker_id, LOWER(publisher) AS partner
                           FROM {{ref('gvc_dim_campaigns')}}
                           WHERE REGEXP_CONTAINS(Brand, '(?i)gala|foxy|cheeky') AND ChannelGrouping = 'Display - Partners' AND NOT REGEXP_CONTAINS(Publisher, '(?i)dcmm') AND SAFE_CAST(campaign_id AS INT64) IS NOT NULL) p
                ON wm_tracking = tracker_id),

     led  AS(   SELECT distinct *
                         REPLACE ( CASE WHEN (REGEXP_CONTAINS(Campaign,'(?i)amp|google') OR REGEXP_CONTAINS(source,'(?i)amp|google'))
                                         AND  REGEXP_CONTAINS(ChannelGrouping,'(?i)ppc')                                               THEN 'Google_Ads'
                                        WHEN (REGEXP_CONTAINS(ChannelGrouping,'(?i)uac') AND REGEXP_CONTAINS(Campaign,'(?i)google'))
                                          OR REGEXP_CONTAINS(Campaign,'(?i)-guac-')                                                    THEN 'Google_UAC'
                                        WHEN REGEXP_CONTAINS(ChannelGrouping,'(?i)vod')
                                         AND REGEXP_CONTAINS(brand, '(?i)gala|foxy|cheek')                                             THEN 'VOD'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)app nexus|appnexus')
                                          OR REGEXP_CONTAINS(source,'(?i)app nexus|appnexus')                                          THEN 'AppNexus'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)forza')    OR REGEXP_CONTAINS(source,'(?i)forza')           THEN 'Forza App'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)ttd')      OR REGEXP_CONTAINS(source,'(?i)tradedesk')       THEN 'TradeDesk'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)dv360')    OR REGEXP_CONTAINS(source,'(?i)dv360')           THEN 'DV360'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)youtube')                                                   THEN 'YouTube'
                                        WHEN REGEXP_CONTAINS(Campaign,'(?i)facebook') OR REGEXP_CONTAINS(source,'(?i)facebook')        THEN 'Facebook'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)kicktipp') OR REGEXP_CONTAINS(source,'(?i)kicktipp')        THEN 'Kicktipp'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)aboola')   OR REGEXP_CONTAINS(source,'(?i)aboola')          THEN 'Taboola'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)snap')     OR REGEXP_CONTAINS(source,'(?i)snap')            THEN 'Snapchat'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)wakeapp')  OR REGEXP_CONTAINS(source,'(?i)wakeapp')         THEN 'Wakeapp'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)fluent')   OR REGEXP_CONTAINS(source,'(?i)fluent')          THEN 'Fluent'
                                        WHEN REGEXP_CONTAINS(source,'(?i)spiegel') OR REGEXP_CONTAINS(campaign,'(?i)spiegel|spiegal')  THEN 'Spiegel (de)'
                                        WHEN REGEXP_CONTAINS(source,'(?i)Uim-Media') OR REGEXP_CONTAINS(campaign,'(?i)Uim-Media')      THEN 'UIM'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)Bild-Media|Bild.de') OR REGEXP_CONTAINS(source,'(?i)Bild-Media|Bild.de')  THEN 'Bild.de'
                                        WHEN REGEXP_CONTAINS(campaign,'(?i)Sevenone|seven one') OR REGEXP_CONTAINS(source,'(?i)Sevenone|seven one')  THEN 'Seven One'
                                        WHEN REGEXP_CONTAINS(source,'(?i)sporttotal_media') OR REGEXP_CONTAINS(campaign,'(?i)sporttotal_media')      THEN 'Sporttotal'
                                        WHEN REGEXP_CONTAINS(source,'(?i)transfer_markt')   OR REGEXP_CONTAINS(campaign,'(?i)transfer_markt')        THEN 'Transfermarkt.de'
                                        WHEN REGEXP_CONTAINS(source,'(?i)Sport1_media')     OR REGEXP_CONTAINS(campaign,'(?i)Sport1_media')          THEN 'Sport1.de'
                                        WHEN REGEXP_CONTAINS(source,'(?i)e2_partnership|e2 online') OR REGEXP_CONTAINS(campaign,'(?i)e2_partnership')THEN 'E2network'
                                        WHEN REGEXP_CONTAINS(source,'(?i)erizon|veriz') OR REGEXP_CONTAINS(campaign,'(?i)erizon')      THEN 'Verizon'
                                        WHEN REGEXP_CONTAINS(source,'(?i)utbrai') OR REGEXP_CONTAINS(campaign,'(?i)utbrai')            THEN 'Outbrain'
                                        WHEN REGEXP_CONTAINS(source,'(?i)fyber.com')                                                   THEN 'Fyber'
                                        WHEN REGEXP_CONTAINS(source,'(?i)OneFootball')                                                 THEN 'Onefootball'
                                        WHEN REGEXP_CONTAINS(ChannelGrouping, '(?i)Organic - Yahoo')                                   THEN 'Yahoo'
                                        WHEN REGEXP_CONTAINS(source, '(?i)odds')                                                       THEN REGEXP_REPLACE(source, ' ', '')
                                        WHEN NOT REGEXP_CONTAINS(source,'(?i)google')   AND REGEXP_CONTAINS(source,'(?i)_ads')         THEN REGEXP_REPLACE(source, '(?i)_ads', '')
                                        WHEN REGEXP_CONTAINS(source, '(?i)display-')                                                   THEN SPLIT(LOWER(source), 'display-')[SAFE_OFFSET(1)]
                                        ELSE source END AS source,
                                   CASE WHEN REGEXP_CONTAINS(campaign, '(?i)dv360|appnexus') THEN TRIM(SPLIT(campaign,'ID_')[OFFSET(0)])
                                        WHEN REGEXP_CONTAINS(campaign, '(?i)cro_test')       THEN CONCAT(campaign,'_', SPLIT(source, '-')[OFFSET(0)])
                                        ELSE campaign
                                        END AS campaign)
                FROM med),

     raa  AS(   SELECT *     #Pick all the Deposits from DCM/Appsflyer
                FROM led
                WHERE Conversion NOT IN ('Bet', 'Registration') AND Dataset <> 'GA'
                ),

     maa  AS(   SELECT *     #Pick all the Deposits from DCM/Appsflyer
                FROM raa

              UNION ALL
                SELECT *     #Pick all the Deposits from GA being potential duplicates according to customerID
                FROM led
                WHERE Conversion NOT IN ('Bet', 'Registration') AND Dataset = 'GA' --AND ChannelGrouping IN( 'Direct' , 'Display - Partners')
                  AND CustomerID IN (SELECT distinct CustomerID FROM raa)
              ),


     tyy  AS(   SELECT *     #Pick all the Bets from DCM
                FROM led
                WHERE Conversion = 'Bet' AND Dataset = 'DCM'
                ),

     myy  AS(   SELECT *     #Pick all the Bets from DCM
                FROM tyy

              UNION ALL
                SELECT *     #Pick all the Bets from GA being potential duplicates according to customerID
                FROM led
                WHERE Conversion  IN ('Bet') AND Dataset = 'GA' --AND ChannelGrouping IN( 'Direct' , 'Display - Partners')
                  AND CustomerID IN (SELECT distinct CustomerID FROM tyy)
              ),

#Calculate lag (< 1 min) vs prev/next conversions. Replace GBP amount for null/DCM deposits
     saa  AS(   SELECT CASE WHEN Prev = 0 OR aft = 0 THEN 0 ELSE NULL END AS lag, *,
                       CASE WHEN (event_value_GBP IS NULL OR (Dataset = 'DCM' AND Currency ='USD')) AND prev = 0 THEN LAG (event_value_GBP) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                            WHEN (event_value_GBP IS NULL OR (Dataset = 'DCM' AND Currency ='USD')) AND aft  = 0 THEN LEAD(event_value_GBP) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                       ELSE event_value_GBP END AS new_event_value

                FROM(
                       SELECT TIME_DIFF(LAG (event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, MINUTE) prev,
                              TIME_DIFF(LEAD(event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, MINUTE) aft,*
                       FROM maa)
               ),

#Rank each pair of duplicate deposits according to the code assigned
     vie  AS(   SELECT * EXCEPT(newcode,dupdup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, newcode
                                                                         ORDER BY dupdup, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC, Visitid) AS rank

            #Assign a code to identify pairs of duplicates (from previous element, then from following for elements that do not have a previous one
                FROM( SELECT CASE WHEN prev = 0 THEN CONCAT(code, '_', LAG(rank) OVER (PARTITION BY Brand, CustomerID, Conversion, Date, code ORDER BY rank))
                                  ELSE CONCAT(code, '_', rank)
                                  END AS newcode,
                             * EXCEPT (rank, code), CASE WHEN ChannelGrouping IN ('Direct', 'Referral') THEN 1 END AS dupdup
                      FROM(
                             SELECT ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, Conversion, hour ORDER BY Event_time) AS rank
                                    ,CONCAT(Brand, Customerid, date, Conversion, hour, new_event_value) AS code
                                    ,* EXCEPT(new_event_value) REPLACE(new_event_value AS event_value_GBP)
                             FROM saa
                             WHERE lag = 0))
               ),

#Deposits: cleaned duplicates + SAA non-dup + non-processed deposits
    alle  AS(   SELECT *  REPLACE(CASE WHEN Conversion IN ('Bet', 'Registration') THEN Conversion ELSE 'Deposit' END AS Conversion)
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
                                                               ORDER BY Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value_GBP DESC) AS rank
                             FROM led
                             WHERE Conversion NOT IN ('Bet', 'Registration')
--                                AND ((Dataset = 'GA' AND ChannelGrouping != 'Direct')
--                                 OR (ChannelGrouping = 'Direct' AND CustomerID NOT IN (SELECT distinct CustomerID FROM raa)))
                               AND (Dataset = 'GA' AND CustomerID NOT IN (SELECT distinct CustomerID FROM raa))
                           ) WHERE rank =1
                  )),

     regs  AS(  SELECT * EXCEPT(ord) ,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID
                                                              ORDER BY ord DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC) AS rank
                FROM (SELECT *, IF(Channelgrouping = 'Direct',0,1) AS ord FROM led)
                WHERE Conversion = 'Registration'
              ),

#Calculate lag (< 10 sec) vs prev/next conversions. Replace GBP amount and transaction id for DCM Bets
      taa AS (  SELECT CASE WHEN ABS(Prev) <10 OR ABS(aft) < 10 THEN 0 ELSE NULL END AS lag, *,
                       CASE WHEN (event_value_GBP IS NULL OR (Dataset = 'DCM')) AND ABS(Prev) <10 THEN LAG (event_value_GBP) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                            WHEN (event_value_GBP IS NULL OR (Dataset = 'DCM')) AND ABS(Aft ) <10 THEN LEAD(event_value_GBP) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                       ELSE event_value_GBP END AS new_event_value,
                       CASE WHEN Dataset = 'DCM' AND ABS(Prev) <10 THEN LAG (transaction_id) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                            WHEN Dataset = 'DCM' AND ABS(Aft ) <10 THEN LEAD(transaction_id) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time)
                       ELSE transaction_id END AS new_transaction_id

                FROM(
                      SELECT TIME_DIFF(LAG (event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, SECOND) prev,
                             TIME_DIFF(LEAD(event_time) OVER (PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY event_time), event_time, SECOND) aft,*
                      FROM myy)
               ),

#Rank each pair of duplicate bets according to the code assigned
      rap AS(   SELECT * EXCEPT(newcode,dupdup,new_transaction_id)
                      , ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, newcode
                                            ORDER BY dupdup, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC, Visitid) AS rank

                          #Assign a code to identify pairs of duplicates (from previous element, then from following for elements that do not have a previous one
                              FROM( SELECT CASE WHEN prev = 0 THEN CONCAT(code, '_', LAG(rank) OVER (PARTITION BY Brand, CustomerID, Conversion, Date, code ORDER BY rank))
                                                ELSE CONCAT(code, '_', rank)
                                                END AS newcode,
                                           * EXCEPT (rank, code), CASE WHEN ChannelGrouping IN ('Direct', 'Referral') THEN 1 END AS dupdup
                                    FROM(
                                           SELECT ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, Conversion, hour ORDER BY Event_time) AS rank
                                                  ,CONCAT(Brand, Customerid, date, Conversion, hour, new_event_value) AS code
                                                  ,* EXCEPT(new_event_value)
                                                     REPLACE(new_event_value AS event_value_GBP,
                                                             CASE WHEN transaction_id IS NULL THEN new_transaction_id ELSE transaction_id END AS transaction_id)
                                           FROM taa
                                           WHERE lag = 0))
              ),

#Bets: cleaned duplicates + TAA non-dup + non-processed bets, then rank on transaction_id
     bets  AS(  SELECT * EXCEPT(non_dup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Date, transaction_id
                                                                  ORDER BY non_dup DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC, Event_value DESC) AS rank
                FROM (SELECT *, CASE WHEN ChannelGrouping != 'Direct' THEN 1 END AS non_dup
                      FROM(
                           SELECT * EXCEPT(lag, aft, prev, rank)
                           FROM rap
                          UNION ALL
                           SELECT * EXCEPT(lag, new_event_value, aft, prev,new_transaction_id)
                           FROM taa WHERE lag <> 0 OR lag IS NULL
                          UNION ALL
                           SELECT *
                           FROM led WHERE Conversion = 'Bet' AND (Dataset = 'GA' AND CustomerID NOT IN (SELECT distinct CustomerID FROM tyy))
                           )
              )),


#Deduplicate again deposits + bets + regs. Transform fist deposit into FTD
     kiko  AS(  SELECT * EXCEPT(rank3)
                         REPLACE(CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN 'FTD'       ELSE Conversion END AS Conversion,
                                 CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_0_type ELSE NULL       END AS pNGR_0_type,
                                 CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_0      ELSE NULL       END AS pNGR_0,
                                 CASE WHEN Conversion = 'Deposit' AND FTD_date =1 AND rank3 = 1 THEN pNGR_21     ELSE NULL       END AS pNGR_21)

                FROM(
                      SELECT *
                      FROM(
                            SELECT * EXCEPT(rank2, Minute)
                                   ,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date ORDER BY Hour, Minute, Click_conversion DESC, Lag_hours, Event_value_GBP DESC) AS rank3
                            FROM(
                                  SELECT * EXCEPT(non_dup), ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID, Conversion, Date, Hour, Minute
                                                                                    ORDER BY non_dup DESC, Click_conversion DESC, Lag_hours, Event_value_GBP DESC, keyword DESC, Source DESC, visitid ASC) AS rank2
                                  FROM alle
                                  )
                            WHERE rank2 =1
                           ) r
                     UNION ALL
                     SELECT * EXCEPT(Minute, rank), 0 AS rank3 FROM Bets WHERE rank = 1
                     UNION ALL
                     SELECT * EXCEPT(Minute, rank), 0 AS rank3 FROM Regs WHERE rank = 1
                    ) s
               ),

   muu AS(   SELECT *, CASE WHEN Conversion = 'FTD'          AND ChannelGrouping  = 'Direct' AND CustomerID <> 'null' THEN 1
                            WHEN Conversion = 'Registration' AND ChannelGrouping != 'Direct' AND CustomerID <> 'null' THEN 2 END AS transf
                       ,DATETIME_ADD(DATETIME_ADD(DATETIME_ADD(CAST(date AS datetime), INTERVAL EXTRACT(HOUR FROM event_time) HOUR ), INTERVAL EXTRACT(MINUTE FROM event_time) MINUTE), INTERVAL EXTRACT(SECOND FROM event_time) SECOND) AS Datetime
               FROM(
                      SELECT kiko.* REPLACE(REGEXP_REPLACE(kiko.campaign, '  | ', '')  AS Campaign)
                      FROM kiko)
                      )

SELECT * REPLACE(CASE WHEN REGEXP_CONTAINS(source, '(?i)trade desk|tradedesk')     THEN 'TradeDesk'
                      WHEN REGEXP_CONTAINS(source, '(?i)vod|UIM|dcm|crm|ppc|360') OR LOWER(source) IN('dsp', 'dpg') THEN UPPER(source)
                      WHEN (REGEXP_CONTAINS(source, '(?i)-odds') OR REGEXP_CONTAINS(campaign, '(?i)-odds|_odds') ) AND NOT REGEXP_CONTAINS(source, '(?i)forza|display')
                       AND REGEXP_CONTAINS(ChannelGrouping, '(?i)partner')         THEN CONCAT(INITCAP(SPLIT(REGEXP_REPLACE(LOWER(source), ' (De)',''), '-odds')[SAFE_OFFSET(0)]), '-odds')
                      WHEN REGEXP_CONTAINS(source, '(?i)onefoot')                  THEN 'Onefootball'
                      WHEN REGEXP_CONTAINS(source, '(?i)-media')                   THEN INITCAP(SPLIT(LOWER(source), '-media')[SAFE_OFFSET(0)])
                      WHEN REGEXP_CONTAINS(source, '(?i)uac') OR source LIKE '%.%' THEN source
                      ELSE INITCAP(source) END AS source,
                      CASE WHEN dataset ='TD' THEN 0 ELSE lag_hours END AS lag_hours
                      )
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
                       CASE WHEN f.transf=1 AND t.keyword          IS NOT NULL THEN t.keyword          ELSE f.keyword          END AS keyword)

              ,CASE WHEN f.Conversion = 'FTD' THEN
                    CASE WHEN f.transf=1 AND t.ChannelGrouping IS NOT NULL THEN 'reg' ELSE 'ftd' END
               END AS FTD_attributed_channel

    FROM muu f
    LEFT JOIN (SELECT * FROM muu WHERE transf = 2) t
           ON f.customerID = t.customerID
          AND f.Brand= t.Brand
          AND DATE_DIFF(f.datetime, t.datetime, MINUTE) < 72*60
    )
