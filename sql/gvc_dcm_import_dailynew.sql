SELECT Activity
       ,a.Activity_ID
       ,a.Event_time
       ,EXTRACT(DATE FROM TIMESTAMP_MICROS(a.Event_time)) AS date
       ,a.Campaign_id
       ,event_sub_type
       ,Interaction_Time
       ,Other_Data
       ,Country_Code
       ,site_id_dcm
       ,Site_DCM

FROM {{ source('DCM_GVC', 'activity_8807') }} a
     JOIN (SELECT distinct * FROM {{ source('DCM_GVC', 'p_match_table_activity_cats_8807') }} )b USING(Activity_ID)
LEFT JOIN (SELECT distinct * FROM {{ source('DCM_GVC', 'p_match_table_sites_8807') }} )s         USING(Site_id_dcm)

WHERE a.Campaign_ID IS NOT NULL
  AND _DATA_DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND a.Activity_id  IN ("6019708","8710237","8693244","3932542","8688707","3709772","8686973","3501856","8687528","3504211","8693238","6732945","6766199","6764300","6906649","6766316","7010106","7010100",                  "9273211","9224777","1591088","6020347","1294141","7024995","1282288","6719296","7734412","7727914","7697111","8306420","8311634","8365297","7381380","7462030","7416335","10983796","10987297","10983793")
  
  AND Event_time BETWEEN  DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)   *86400000000
                     AND (DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)+1)*86400000000-1
