CREATE DATABASE IF NOT EXISTS silver_layer;

CREATE TABLE IF NOT EXISTS silver_layer.tv_series
(
    tv_series_id UInt64,
    overview String,
    popularity Float64,
    first_air_date Date,
    tagline String DEFAULT '',
    vote_average Float64,
    vote_count UInt64,
    status String,

    genres Array(Tuple(id UInt64, name String)),
    production_countries Array(Tuple(iso_3166_1 String, name String)),

    number_of_seasons UInt64,

    vector_info_hash Int64,
    casts_total_hash Int64,
    crews_total_hash Int64,

    vector_info_hash_diff Bool,

    casts_diff String,
    crews_diff String,

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY tv_series_id;