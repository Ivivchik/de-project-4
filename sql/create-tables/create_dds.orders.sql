CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id SERIAL NOT NULL,
	order_key VARCHAR NOT NULL,
	restaurant_id INTEGER NOT NULL,
	timestamp_id INTEGER NOT NULL,
    CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_order_key_key UNIQUE (order_key)
);