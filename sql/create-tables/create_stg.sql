CREATE TABLE IF NOT EXISTS {{ params.table_name }}(
    id          VARCHAR,
    "name"      VARCHAR,
    report_dt   TIMESTAMP
);