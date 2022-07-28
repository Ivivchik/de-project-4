CREATE TABLE IF NOT EXISTS {{ params.table_name }}(
    id              SERIAL      PRIMARY KEY,
    object_id       VARCHAR     NOT NULL,
    "name"          VARCHAR     NOT NULL,
    effective_date  TIMESTAMP   NOT NULL,
    expiration_date TIMESTAMP   NOT NULL
);