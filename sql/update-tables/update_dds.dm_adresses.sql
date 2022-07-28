INSERT INTO dds.dm_addresses(street, house, flat)
    SELECT DISTINCT t[1], 
                    t[2]::INTEGER,
                    SUBSTRING(t[3], 5)::INTEGER
               FROM (SELECT REGEXP_SPLIT_TO_ARRAY("address", ',') t 
                       FROM stg.api_deliveries
                      WHERE order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                                         AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                    ) t1 
        ON CONFLICT (street, house, flat)
      DO UPDATE SET street = excluded.street,
                    house = excluded.house,
                    flat = excluded.flat;
