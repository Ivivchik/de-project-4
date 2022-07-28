WITH t AS (
      SELECT order_ts::TIMESTAMP ts,
             EXTRACT('year' FROM order_ts::TIMESTAMP) "year",
             EXTRACT('month' FROM order_ts::TIMESTAMP) "month",
             EXTRACT('day' FROM order_ts::TIMESTAMP) "day",
             order_ts::TIMESTAMP::TIME "time",
             order_ts::TIMESTAMP::DATE "date"
        FROM stg.api_deliveries 
       WHERE order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
         AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
       UNION
      SELECT delivery_ts::TIMESTAMP ts,
             EXTRACT('year' FROM delivery_ts::TIMESTAMP) "year",
             EXTRACT('month' FROM delivery_ts::TIMESTAMP) "month",
             EXTRACT('day' FROM delivery_ts::TIMESTAMP) "day",
             delivery_ts::TIMESTAMP::TIME "time",
             delivery_ts::TIMESTAMP::DATE "date"
        FROM stg.api_deliveries
       WHERE order_ts BETWEEN '{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
                          AND '{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}'
)

INSERT INTO dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
      SELECT DISTINCT ts,
                      "year",
                      "month",
                      "day",
                      "time",
                      "date"
                 FROM t
          ON CONFLICT (ts)
        DO UPDATE SET "year" = excluded."year",
                      "month" = excluded."month",
                      "day" = excluded."day",
                      "time" = excluded."time",
                      "date" = excluded."date";