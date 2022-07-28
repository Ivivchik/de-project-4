INSERT INTO dds.dm_orders(order_key, restaurant_id, timestamp_id)
       SELECT DISTINCT order_id order_key,
                       dr.id restaurant_id,
                       dt.id timestamp_id
                  FROM stg.api_deliveries ad
                  JOIN dds.dm_restaurants dr ON dr.object_id = ad.restaurant_id 
                  JOIN dds.dm_timestamps dt ON dt.ts = ad.order_ts::TIMESTAMP
                 WHERE ad.order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                              AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
          AND dr.expiration_date = '2999-12-31 23:59:59'
  ON CONFLICT (order_key)
DO UPDATE SET order_key = excluded.order_key,
              restaurant_id = excluded.restaurant_id,
              timestamp_id = excluded.timestamp_id;