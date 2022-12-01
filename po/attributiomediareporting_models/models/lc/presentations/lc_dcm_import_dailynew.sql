SELECT Activity
       ,a.Activity_ID
       ,a.Event_time
       ,EXTRACT(DATE FROM TIMESTAMP_MICROS(a.Event_time)) AS date
       ,a.Campaign_id
       ,event_sub_type
       ,Interaction_Time
       ,Other_Data
       ,site_id_dcm
       ,Site_DCM

FROM {{ source('DCM_UK', 'activity_785192') }} a
     JOIN (SELECT distinct * FROM {{ source('DCM_UK', 'p_match_table_activity_cats_785192') }} )b USING(Activity_ID)
LEFT JOIN (SELECT distinct * FROM {{ source('DCM_UK', 'p_match_table_sites_785192') }} )s USING(Site_id_dcm)

WHERE a.Campaign_ID IS NOT NULL
  AND _DATA_DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND a.Activity_id NOT IN ('6974085', '6976437', '6976440', '7014892', '7017616', '7017619', '7305063', '7305069', '7305072', '7345049', '7355740', '7363408', '7372435', '7372441',
                            '7372444', '7535820', '7573565', '7588752', '7588887', '7598958', '7598970', '7617122', '7617131', '7630246', '7631095', '7641796', '7641811', '7792248',
                            '7792251', '7793463', '7809401', '7962966', '7963827', '7969178', '8012545', '8040264', '8082641', '8082650', '8083052', '8083070', '8083754', '8085800',
                            '8191586', '8232120', '8233224', '8234991', '8236467', '8239832', '8243264', '8250472', '8251683', '8255616', '8277781', '8278543', '8379701', '8379995',
                            '8393601', '8434987', '8571885', '8597884', '8597896', '8597899', '8598064', '8598067', '8780627', '8781182', '8939146', '8943238', '8945568', '8959024',
                            '8963737', '8964237', '8966037', '9030203', '9052504', '9148013', '9150986', '9183768', '9187590', '9188820', '9190021', '9190030', '9215838', '9219843',
                            '9276747', '9277672', '9379988', '9417120', '10314520', '10973669', '10996623')

  AND Event_time BETWEEN  DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)   *86400000000
                     AND (DATE_DIFF(Current_date, CAST('1970-1-2'AS DATE), DAY)+1)*86400000000-1
