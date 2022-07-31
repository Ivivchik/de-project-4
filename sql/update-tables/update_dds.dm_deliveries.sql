INSERT INTO dds.dm_deliveries(delivery_key, courier_id, address_id, timestamp_id)
       SELECT DISTINCT delivery_id delivery_key,
                       dc.id courier_id,
                       da.id address_id,
                       dt.id timestamp_id
                  FROM stg.api_deliveries ad 
                  JOIN dds.dm_orders do2 ON do2.order_key = ad.order_id 
                  JOIN dds.dm_couriers dc ON dc.object_id = ad.courier_id 
                  JOIN dds.dm_timestamps dt ON dt.ts = ad.delivery_ts::TIMESTAMP
                  JOIN dds.dm_addresses da ON da.street = (REGEXP_SPLIT_TO_ARRAY("address", ','))[1]
                   AND da.house = (REGEXP_SPLIT_TO_ARRAY("address", ','))[2]::INTEGER
                   AND da.flat = SUBSTRING((REGEXP_SPLIT_TO_ARRAY("address", ','))[3], 5)::INTEGER
                 WHERE ad.order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                                       AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                   AND dc.expiration_date = '2999-12-31 23:59:59'
  ON CONFLICT (delivery_key) 
DO UPDATE SET courier_id = excluded.courier_id,
              address_id = excluded.address_id,
              timestamp_id = excluded.timestamp_id;