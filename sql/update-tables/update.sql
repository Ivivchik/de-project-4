INSERT INTO {{ params.target_table }}(object_id, "name", effective_date, expiration_date)
    SELECT DISTINCT st.id, 
                    st."name",
                    max(st.report_dt),
                    COALESCE(expiration_date, '2999-12-31 23:59:59') 
               FROM {{ params.target_table }} tt 
         RIGHT JOIN {{ params.source_table }} st ON st.id =  tt.object_id  
              WHERE report_dt > COALESCE(effective_date, '1900-01-01 00:00:00'::TIMESTAMP)
                AND COALESCE(expiration_date, '2999-12-31 23:59:59') = '2999-12-31 23:59:59'
                AND (tt."name" != st."name" OR tt."name" IS NULL)
           GROUP BY st.id, 
                    st."name",
                    COALESCE(expiration_date, '2999-12-31 23:59:59');


UPDATE {{ params.target_table }}
   SET expiration_date = subs_query.ld
  FROM (SELECT object_id oi,
               effective_date ed,
               COALESCE(LEAD(effective_date) OVER(PARTITION BY object_id ORDER BY effective_date), '2999-12-31 23:59:59') ld
          FROM {{ params.target_table }}) subs_query
 WHERE object_id = subs_query.oi
   AND subs_query.ed = effective_date
   AND subs_query.ld != expiration_date;