CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id SERIAL NOT NULL,
	delivery_key VARCHAR NOT NULL,
	courier_id INTEGER NOT NULL,
	address_id INTEGER NOT NULL,
	timestamp_id INTEGER NOT NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_delivery_key_key UNIQUE (delivery_key)
);