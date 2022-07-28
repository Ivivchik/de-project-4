INSERT INTO dds.fct_orders_delivery(delivery_id, order_id, rate, "sum", tip_sum)
       SELECT DISTINCT dd.id delivery_id,
                       do2.id order_id,
                       rate::INTEGER,
                       "sum"::NUMERIC(14,2),
                       tip_sum::NUMERIC(14,2)
                  FROM stg.api_deliveries ad
                  JOIN dds.dm_deliveries dd ON dd.delivery_key = ad.delivery_id 
                  JOIN dds.dm_orders do2 ON do2.order_key = ad.order_id 
                 WHERE ad.order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                              AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
  ON CONFLICT (delivery_id, order_id)
DO UPDATE SET rate = excluded.rate,
              "sum" = excluded."sum",
              tip_sum = excluded.tip_sum