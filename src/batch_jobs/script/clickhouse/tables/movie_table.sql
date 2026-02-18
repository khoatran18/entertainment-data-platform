CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS silver_layer.movie
(
    movie_id UInt64,
    original_title String,
    overview String,
    popularity Float64,
    release_date Date,
    tagline String,
    vote_average Float64,
    vote_count UInt64,

    genres Nested
    (
        id UInt64,
        name LowCardinality(String),
    ),

    belongs_to_collection Nested
    (
        id UInt64,
        name LowCardinality(String),
    ),

    production_countries Nested
    (
        iso_3166_1 String,
        name LowCardinality(String),
    ),

    batch_version UInt64,
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY movie_id;