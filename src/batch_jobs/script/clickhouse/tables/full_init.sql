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
        name LowCardinality(String)
    ),

    belongs_to_collection Nested
    (
        id UInt64,
        name LowCardinality(String)
    ),

    production_countries Nested
    (
        iso_3166_1 String,
        name LowCardinality(String)
    ),

    vector_info_hash Int64,
    casts_total_hash Int64,
    crews_total_hash Int64,

    vector_info_hash_diff Bool,

    casts_diff JSON
    (
        added Array(UInt64),
        removed Array(UInt64)
    ),

    crews_diff JSON
    (
        added Array(UInt64),
        removed Array(UInt64)
    ),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY movie_id;

CREATE TABLE IF NOT EXISTS silver_layer.movie_cast
(
    movie_id UInt64,
    cast_id UInt64,
    person_id UInt64,
    character String,
    credit_id String,
    known_for_department LowCardinality(String),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (movie_id, cast_id);

CREATE TABLE IF NOT EXISTS silver_layer.movie_crew
(
    movie_id UInt64,
    person_id UInt64,
    department LowCardinality(String),
    job String,
    known_for_department LowCardinality(String),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (movie_id, person_id);

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

    genres Nested
    (
        id UInt64,
        name String
    ),

    production_countries Nested
    (
        iso_3166_1 String,
        name String
    ),

    number_of_seasons UInt64,

        vector_info_hash Int64,
    casts_total_hash Int64,
    crews_total_hash Int64,

    vector_info_hash_diff Bool,

    casts_diff JSON
    (
        added Array(UInt64),
        removed Array(UInt64)
    ),

    crews_diff JSON
    (
        added Array(UInt64),
        removed Array(UInt64)
    ),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY tv_series_id;

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
ORDER BY (tv_series_id, cast_id);

CREATE TABLE IF NOT EXISTS  silver_layer.tv_series_crew
(
    tv_series_id UInt64,
    person_id UInt64,
    department LowCardinality(String),
    job String,
    known_for_department LowCardinality(String),

    batch_version UInt64
)
ENGINE = ReplacingMergeTree(batch_version)
ORDER BY (tv_series_id, person_id);