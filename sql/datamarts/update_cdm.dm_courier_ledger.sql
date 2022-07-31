WITH t AS(
    SELECT dc.id,
           CONCAT_WS(' ', dc.first_name, dc.last_name) "name",
           order_id,
           EXTRACT('month' FROM '{{ prev_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'::TIMESTAMP) "month",
           EXTRACT('year' FROM '{{ prev_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'::TIMESTAMP) "year",
           rate,
           "sum",
           tip_sum, 
           AVG(rate) OVER (PARTITION BY dc.id, CONCAT_WS(' ', dc.first_name, dc.last_name)) av
      FROM dds.fct_orders_delivery fod 
      JOIN dds.dm_deliveries dd ON dd.id = fod.delivery_id 
      JOIN dds.dm_couriers dc ON dc.id = dd.courier_id
      JOIN dds.dm_orders do2 ON do2.id = fod.order_id
      JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id
     WHERE dt."month" = EXTRACT('month' FROM '{{ prev_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'::TIMESTAMP)
       AND dt."year" = EXTRACT('year' FROM '{{ prev_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'::TIMESTAMP)),

t1 AS(
    SELECT *, 
           CASE 
           WHEN av >= 4.9 THEN 
	           CASE WHEN 0.1 * "sum" >=200 THEN 0.1 * "sum" ELSE 200 END
           WHEN av >= 4.5 and av < 4.9 THEN
	           CASE WHEN 0.08 * "sum" >=175 THEN 0.08 * "sum" ELSE 175 END
           WHEN av >= 4.0 and av < 4.5 THEN 
	           CASE WHEN 0.07 * "sum" >=150 THEN 0.07 * "sum" ELSE 150 END
           WHEN av < 4.0 THEN 
	           CASE WHEN 0.05 * "sum" >=100 THEN 0.05 * "sum" ELSE 100 END
           END courier_order_sum 
      FROM t),

t3 AS(
    SELECT id,
           "name",
           COUNT(order_id) orders_count,
           "year",
           "month",
           av rate_avg,
           SUM("sum") orders_total_sum,
           SUM(tip_sum) courier_tips_sum,
           SUM(courier_order_sum ) courier_order_sum 
      FROM t1 
  GROUP BY id,
           "name",
           "year",
           "month",
           av)

INSERT INTO cdm.dm_courier_ledger(courier_id,
                                  courier_name,
                                  settlement_year,
                                  settlement_month,
                                  orders_count,
                                  orders_total_sum,
                                  rate_avg,
                                  order_processing_fee,
                                  courier_order_sum,
                                  courier_tips_sum,
                                  courier_reward_sum)
       SELECT id,
              "name",
              "year",
              "month",
              orders_count,
              orders_total_sum,
              rate_avg,
              (orders_total_sum * 0.25) order_processing_fee,
              courier_order_sum,
              courier_tips_sum,
              (courier_order_sum + courier_tips_sum * 0.95 ) courier_reward_sum 
         FROM t3
  ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET orders_count = excluded.orders_count,
              orders_total_sum = excluded.orders_total_sum,
              rate_avg = excluded.rate_avg,
              order_processing_fee = excluded.order_processing_fee,
              courier_order_sum = excluded.courier_order_sum,
              courier_tips_sum = excluded.courier_tips_sum,
              courier_reward_sum = excluded.courier_reward_sum