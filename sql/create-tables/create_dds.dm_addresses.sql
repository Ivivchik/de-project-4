CREATE TABLE IF NOT EXISTS dds.dm_addresses (
	id SERIAL NOT NULL,
	street VARCHAR NOT NULL,
	house INTEGER NOT NULL,
	flat INTEGER NOT NULL,
	CONSTRAINT dm_addresses_pkey PRIMARY KEY (id),
	CONSTRAINT unique_dm_addresses UNIQUE (street, house, flat)
);