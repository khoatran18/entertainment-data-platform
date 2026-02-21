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

    genres Array(Tuple(id UInt64, name String)),
    belongs_to_collection Array(Tuple(id UInt64, name String)),
    production_countries Array(Tuple(iso_3166_1 String, name String)),

    vector_info_hash Int64,
    casts_total_hash Int64,
    crews_total_hash Int64,

    vector_info_hash_diff Bool,

    casts_diff String,
    crews_diff String,

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY movie_id;