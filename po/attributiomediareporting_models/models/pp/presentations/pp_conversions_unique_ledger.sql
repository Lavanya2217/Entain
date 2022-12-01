WITH 
     ledger AS( SELECT distinct * FROM {{ref('pp_conversions_ledger')}}),

                        #Pick all the Deposits from DCM/Appsflyer
     raa  AS(   SELECT * REPLACE(CASE WHEN dataset ='TD' THEN 100 ELSE lag_hours END AS lag_hours)    
                FROM ledger
                WHERE Conversion <> 'Registration' AND Dataset <> 'GA'
                ),

     maa  AS(   SELECT *     #Pick all the Deposits from DCM/Appsflyer
                FROM raa

              UNION ALL
                SELECT *     #Pick all the Deposits from GA being potential duplicates according to customerID
                FROM ledger
                WHERE Conversion <> 'Registration' AND Dataset = 'GA'
                  AND CustomerID IN (SELECT distinct CustomerID FROM raa)
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
    alle  AS(   SELECT *  REPLACE(CASE WHEN Conversion ='Registration' THEN Conversion ELSE 'Deposit' END AS Conversion)
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
                             FROM ledger
                             WHERE Conversion <>'Registration'
                               AND (Dataset = 'GA' AND CustomerID NOT IN (SELECT distinct CustomerID FROM raa))
                           ) WHERE rank =1
                  )),

     regs  AS(  SELECT * EXCEPT(ord) ,ROW_NUMBER() OVER ( PARTITION BY Brand, CustomerID
                                                              ORDER BY ord DESC, Click_conversion DESC, View_Conversion DESC, Lag_hours ASC) AS rank
                FROM (SELECT *, IF(Channelgrouping = 'Direct',0,1) AS ord FROM ledger)
                WHERE Conversion = 'Registration'
              ),

#Deduplicate again deposits + regs. Transform fist deposit into FTD
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
                     SELECT * EXCEPT(Minute, rank), 0 AS rank3 FROM Regs WHERE rank = 1
                    ) s
               ),

   muu AS(   SELECT *, CASE WHEN Conversion = 'FTD'          AND ChannelGrouping  = 'Direct' AND CustomerID <> 'null' THEN 1
                            WHEN Conversion = 'Registration' AND ChannelGrouping != 'Direct' AND CustomerID <> 'null' THEN 2 END AS transf
                       ,DATETIME_ADD(DATETIME_ADD(DATETIME_ADD(CAST(date AS datetime), INTERVAL EXTRACT(HOUR FROM event_time) HOUR ), INTERVAL EXTRACT(MINUTE FROM event_time) MINUTE), INTERVAL EXTRACT(SECOND FROM event_time) SECOND) AS Datetime
               FROM(
                      SELECT kiko.* REPLACE(REGEXP_REPLACE(kiko.campaign, '  | ', '')  AS Campaign)
                      FROM kiko)
                      ),
   transf AS(
            SELECT f.* EXCEPT(transf, datetime)
                       REPLACE(
                              /*
                               CASE WHEN f.transf=1 AND t.ChannelGrouping  IS NOT NULL THEN t.ChannelGrouping  ELSE f.ChannelGrouping  END AS ChannelGrouping,
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
                               CASE WHEN f.transf=1 AND t.keyword          IS NOT NULL THEN t.keyword          ELSE f.keyword          END AS keyword
                              */
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.ChannelGrouping  ELSE f.ChannelGrouping  END AS ChannelGrouping,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.medium           ELSE f.medium           END AS medium,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.source           ELSE f.source           END AS source,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.VisitId          ELSE f.VisitId          END AS VisitId,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Campaign         ELSE f.Campaign         END AS Campaign,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Lag_days         ELSE f.Lag_days         END AS Lag_days,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Lag_hours        ELSE f.Lag_hours        END AS Lag_hours,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.View_conversion  ELSE f.View_conversion  END AS View_conversion,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Click_conversion ELSE f.Click_conversion END AS Click_conversion,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Conv_medium      ELSE f.Conv_medium      END AS Conv_medium,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.Dataset          ELSE f.Dataset          END AS Dataset,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.adContent        ELSE f.adContent        END AS adContent,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.wm_tracking      ELSE f.wm_tracking      END AS wm_tracking,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.campaignId       ELSE f.campaignId       END AS campaignId,
                              CASE WHEN f.transf=1 AND t.customerID IS NOT NULL THEN t.keyword          ELSE f.keyword          END AS keyword

                             )

                      ,CASE WHEN f.Conversion = 'FTD' THEN
                            CASE WHEN f.transf=1 AND t.ChannelGrouping IS NOT NULL THEN 'reg' ELSE 'ftd' END
                       END AS FTD_attributed_channel

            FROM muu f
            LEFT JOIN (SELECT * FROM muu WHERE transf = 2) t
                   ON f.customerID = t.customerID
                  AND f.Brand= t.Brand
                  AND DATE_DIFF(f.datetime, t.datetime, MINUTE) < 72*60
        )


SELECT a.*
        REPLACE(CASE WHEN b.ChannelGrouping IS NOT NULL THEN b.ChannelGrouping ELSE a.ChannelGrouping END AS ChannelGrouping,
                CASE WHEN b.Campaign        IS NOT NULL THEN b.Campaign        ELSE a.Campaign        END AS Campaign,
		  CASE WHEN b.campaignId      IS NOT NULL THEN b.campaignId      ELSE a.campaignId      END AS campaignId,
                CASE WHEN b.medium          IS NOT NULL THEN b.medium          ELSE a.medium          END AS medium,
                CASE WHEN b.source          IS NOT NULL THEN b.source          ELSE a.source          END AS source,
                CASE WHEN b.keyword         IS NOT NULL THEN b.keyword         ELSE a.keyword         END AS keyword,
		  CASE WHEN b.adContent       IS NOT NULL THEN b.adContent       ELSE a.adContent       END AS adContent,
                CASE WHEN b.website 		    IS NOT NULL THEN b.website  	     ELSE a.website 	      END AS website,
                CASE WHEN b.gclid           IS NOT NULL THEN b.gclid           ELSE a.gclid 	        END AS gclid,
                CASE WHEN b.matching        IS NOT NULL THEN b.matching        ELSE a.matching        END AS matching
                )
FROM
    (SELECT * REPLACE(CASE WHEN dataset ='TD' THEN 0 ELSE lag_hours END AS lag_hours) FROM transf) a
LEFT JOIN
    (SELECT * FROM ledger
     WHERE conversion = 'Registration'
      AND customerid IN (SELECT distinct registration_customerid FROM {{ref('pp_pokervc_registrations')}} WHERE match_rule <> 0)
     ) b
ON a.customerid = b.customerid AND a.conversion = 'FTD'
