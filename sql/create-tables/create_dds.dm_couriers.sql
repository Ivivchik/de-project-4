CREATE TABLE IF NOT EXISTS dds.dm_couriers(
    id              SERIAL      PRIMARY KEY,
    object_id       VARCHAR     NOT NULL,
    first_name      VARCHAR     NOT NULL,
    last_name       VARCHAR     NOT NULL,
    effective_date  TIMESTAMP   NOT NULL,
    expiration_date TIMESTAMP   NOT NULL
);