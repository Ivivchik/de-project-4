CREATE TABLE IF NOT EXISTS stg.api_deliveries(
    order_id        VARCHAR,
    order_ts        VARCHAR,
    restaurant_id   VARCHAR,
    delivery_id     VARCHAR,
    courier_id      VARCHAR,
    "address"       VARCHAR,
    delivery_ts     VARCHAR,
    rate            VARCHAR,
    "sum"           VARCHAR,
    tip_sum         VARCHAR
);