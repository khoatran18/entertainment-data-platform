CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS  silver_table.tv_series_crew
(
    tv_series_id UInt64,
    person_id UInt64,
    department LowCardinality(String),
    job String,
    known_for_department LowCardinality(String),

    batch_version UInt64,
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (tv_series_id, person_id);