CREATE TABLE IF NOT EXISTS dds.fct_orders_delivery (
	id SERIAL NOT NULL,
	delivery_id INTEGER NOT NULL,
	order_id INTEGER NOT NULL,
	rate INTEGER NOT NULL,
	"sum" NUMERIC(14, 2) NOT NULL,
	tip_sum NUMERIC(14, 2) NOT NULL,
	CONSTRAINT fct_orders_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT fct_orders_delivery_rate_check CHECK (rate BETWEEN 1 AND 5),
	CONSTRAINT fct_orders_delivery_sum_check CHECK (sum BETWEEN 0 AND 999999999999.99),
	CONSTRAINT fct_orders_delivery_tip_sum_check CHECK (tip_sum BETWEEN 0 AND 999999999999.99),
	CONSTRAINT unique_fct_orders_delivery UNIQUE (delivery_id, order_id)
);