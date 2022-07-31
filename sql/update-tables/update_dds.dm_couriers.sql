INSERT INTO dds.dm_couriers(object_id, first_name, last_name, effective_date, expiration_date)
    SELECT DISTINCT st.id, 
                    st.first_name,
                    st.last_name,
                    MAX(st.report_dt),
                    COALESCE(expiration_date, '2999-12-31 23:59:59') 
               FROM dds.dm_couriers tt 
         RIGHT JOIN (SELECT id,
                            (REGEXP_SPLIT_TO_ARRAY("name", ' '))[1] first_name,
                            (REGEXP_SPLIT_TO_ARRAY("name", ' '))[2] last_name,
                            report_dt 
                       FROM stg.api_couriers) st ON st.id =  tt.object_id  
              WHERE report_dt > COALESCE(effective_date, '1900-01-01 00:00:00'::TIMESTAMP)
                AND COALESCE(expiration_date, '2999-12-31 23:59:59') = '2999-12-31 23:59:59'
                AND ((tt.first_name != st.first_name OR tt.first_name IS NULL) OR (tt.last_name != st.last_name OR tt.last_name IS NULL))
           GROUP BY st.id, 
                    st.first_name,
                    st.last_name,
                    COALESCE(expiration_date, '2999-12-31 23:59:59');


UPDATE dds.dm_couriers
   SET expiration_date = subs_query.ld
  FROM (SELECT object_id oi,
               effective_date ed,
               COALESCE(LEAD(effective_date) OVER(PARTITION BY object_id ORDER BY effective_date), '2999-12-31 23:59:59') ld
          FROM dds.dm_couriers) subs_query
 WHERE object_id = subs_query.oi
   AND subs_query.ed = effective_date
   AND subs_query.ld != expiration_date;