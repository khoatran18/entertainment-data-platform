CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS silver_layer.movie_crew
(
    movie_id UInt64,
    person_id UInt64,
    department LowCardinality(String),
    job String,
    known_for_department LowCardinality(String),

    batch_version UInt64,
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (movie_id, person_id);