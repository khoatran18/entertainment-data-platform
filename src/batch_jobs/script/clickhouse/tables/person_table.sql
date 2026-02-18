CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS silver_layer.person
(
    person_id UInt64,
    name String,
    gender UInt8,
    also_known_as Array(String),
    biography String DEFAULT '',
    birthday Nullable(Date),
    deathday Nullable(Date),
    place_of_birth String DEFAULT '',
    known_for_department LowCardinality(String),
    popularity Float64,

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (person_id);