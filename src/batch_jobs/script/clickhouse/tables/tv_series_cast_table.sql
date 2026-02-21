CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS silver_layer.tv_series_cast
(
    tv_series_id UInt64,
    cast_id UInt64,
    person_id UInt64,
    character String,
    credit_id String,
    known_for_department LowCardinality(String),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (tv_series_id, person_id);