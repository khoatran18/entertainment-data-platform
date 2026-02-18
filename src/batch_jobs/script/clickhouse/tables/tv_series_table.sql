CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE silver_table.tv_series
(
    tv_series_id UInt64,
    overview String,
    popularity Float64,
    first_air_date Date,
    tagline String DEFAULT '',
    vote_average Float64,
    vote_count UInt64,
    status String,

    genres Nested
    (
        id UInt64,
        name String,
    ),

    production_countries
    (
        iso_3166_1 String,
        name String,
    ),

    number_of_seasons UInt64,

    batch_version UInt64,
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY tv_series_id;