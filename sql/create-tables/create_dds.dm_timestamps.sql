CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id SERIAL NOT NULL,
	ts TIMESTAMP NOT NULL,
	"year" SMALLINT NOT NULL,
	"month" SMALLINT NOT NULL,
	"day" SMALLINT NOT NULL,
	"time" TIME NOT NULL,
	"date" DATE NOT NULL,
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_day_check CHECK (day BETWEEN 1 AND 31),
	CONSTRAINT dm_timestamps_month_check CHECK (month BETWEEN 1 AND 12),
	CONSTRAINT dm_timestamps_year_check CHECK (year BETWEEN 2022 AND 2999),
	CONSTRAINT unique_ts_dm_timestamps UNIQUE (ts)
);