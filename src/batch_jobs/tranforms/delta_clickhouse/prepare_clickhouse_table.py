from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, to_date, explode, to_json, pandas_udf, concat_ws
from pyspark.sql.types import DataType, LongType, StringType, DoubleType, IntegerType, BooleanType

def to_ch_json(column: Column):
    return to_json(column)
    # return regexp_replace(to_json(column), "'", '"')

def prepare_table_movie(
        df: DataFrame
):
    """
    Convert DataFrame movie to ClickHouse table movie format
    """
    table_df = df.select(
        col("parsed_raw_df.movie_id").cast(LongType()).alias("movie_id"),
        col("parsed_raw_df.movie_detail.original_title").cast(StringType()).alias("original_title"),
        col("parsed_raw_df.movie_detail.overview").cast(StringType()).alias("overview"),
        col("parsed_raw_df.movie_detail.popularity").cast(DoubleType()).alias("popularity"),
        to_date(col("parsed_raw_df.movie_detail.release_date").cast(StringType())).alias("release_date"),
        col("parsed_raw_df.movie_detail.vote_average").cast(DoubleType()).alias("vote_average"),
        col("parsed_raw_df.movie_detail.vote_count").cast(LongType()).alias("vote_count"),

        col("parsed_raw_df.movie_detail.genres").alias("genres"),
        col("parsed_raw_df.movie_detail.belongs_to_collection").alias("belongs_to_collection"),
        col("parsed_raw_df.movie_detail.production_countries").alias("production_countries"),

        col("vector_info_hash").cast(LongType()).alias("vector_info_hash"),
        col("casts_total_hash").cast(LongType()).alias("casts_total_hash"),
        col("crews_total_hash").cast(LongType()).alias("crews_total_hash"),

        col("vector_info_hash_diff").cast(BooleanType()).alias("vector_info_hash_diff"),

        to_ch_json(col("casts_diff")).alias("casts_diff"),
        to_ch_json(col("crews_diff")).alias("crews_diff"),

        col("batch_version").cast(LongType()).alias("batch_version"),
    )

    return table_df

def prepare_table_movie_cast(
        df: DataFrame
):
    """
    Convert DataFrame movie to ClickHouse table movie_cast format
    """
    exploded_df = df.withColumn("cast", explode(col("parsed_raw_df.casts_info")))

    table_df = exploded_df.select(
        col("parsed_raw_df.movie_id").cast(LongType()).alias("movie_id"),
        col("cast.cast_id").cast(LongType()).alias("cast_id"),
        col("cast.person_id").cast(LongType()).alias("person_id"),
        col("cast.character").cast(StringType()).alias("character"),
        col("cast.credit_id").cast(StringType()).alias("credit_id"),
        col("cast.known_for_department").cast(StringType()).alias("known_for_department"),
        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df


def prepare_table_movie_crew(
        df: DataFrame
):
    """
    Convert DataFrame movie to ClickHouse table movie_crew format
    """
    exploded_df = df.withColumn("crew", explode(col("parsed_raw_df.crews_info")))

    table_df = exploded_df.select(
        col("parsed_raw_df.movie_id").cast(LongType()).alias("movie_id"),
        col("crew.person_id").cast(LongType()).alias("person_id"),
        col("crew.department").cast(StringType()).alias("department"),
        col("crew.job").cast(StringType()).alias("job"),
        col("crew.known_for_department").cast(StringType()).alias("known_for_department"),
        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df

def prepare_table_person(
        df: DataFrame
):
    """
    Convert DataFrame person to ClickHouse table person format
    """
    table_df = df.select(
        col("parsed_raw_df.person_id").cast(LongType()).alias("person_id"),
        col("parsed_raw_df.person_detail.name").cast(StringType()).alias("name"),
        col("parsed_raw_df.person_detail.gender").cast(IntegerType()).alias("gender"),
        col("parsed_raw_df.person_detail.also_known_as").alias("also_known_as"),
        col("parsed_raw_df.person_detail.biography").cast(StringType()).alias("biography"),

        to_date(col("parsed_raw_df.person_detail.birthday").cast(StringType())).alias("birthday"),
        to_date(col("parsed_raw_df.person_detail.deathday").cast(StringType())).alias("deathday"),

        col("parsed_raw_df.person_detail.place_of_birth").cast(StringType()).alias("place_of_birth"),
        col("parsed_raw_df.person_detail.known_for_department").cast(StringType()).alias("known_for_department"),
        col("parsed_raw_df.person_detail.popularity").cast(DoubleType()).alias("popularity"),

        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df


def prepare_table_tv_series(
        df: DataFrame
):
    """
    Convert DataFrame tv_series to ClickHouse table tv_series format
    """
    table_df = df.select(
        col("parsed_raw_df.tv_series_id").cast(LongType()).alias("tv_series_id"),

        col("parsed_raw_df.tv_series_detail.overview").cast(StringType()).alias("overview"),
        col("parsed_raw_df.tv_series_detail.popularity").cast(DoubleType()).alias("popularity"),

        to_date(col("parsed_raw_df.tv_series_detail.first_air_date").cast(StringType())).alias("first_air_date"),

        col("parsed_raw_df.tv_series_detail.tagline").cast(StringType()).alias("tagline"),
        col("parsed_raw_df.tv_series_detail.vote_average").cast(DoubleType()).alias("vote_average"),
        col("parsed_raw_df.tv_series_detail.vote_count").cast(LongType()).alias("vote_count"),
        col("parsed_raw_df.tv_series_detail.status").cast(StringType()).alias("status"),

        col("parsed_raw_df.tv_series_detail.genres").alias("genres"),
        col("parsed_raw_df.tv_series_detail.production_countries").alias("production_countries"),

        col("parsed_raw_df.tv_series_detail.number_of_seasons").cast(LongType()).alias("number_of_seasons"),

        col("vector_info_hash").cast(LongType()).alias("vector_info_hash"),
        col("casts_total_hash").cast(LongType()).alias("casts_total_hash"),
        col("crews_total_hash").cast(LongType()).alias("crews_total_hash"),

        col("vector_info_hash_diff").cast(BooleanType()).alias("vector_info_hash_diff"),

        to_ch_json(col("casts_diff")).alias("casts_diff"),
        to_ch_json(col("crews_diff")).alias("crews_diff"),

        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df


def prepare_table_tv_series_cast(
        df: DataFrame
) -> DataFrame:
    """
    Convert DataFrame tv_series to ClickHouse table tv_series_cast format
    """

    exploded_df = df.withColumn("cast", explode(col("parsed_raw_df.casts_info")))
    table_df = exploded_df.select(
        col("parsed_raw_df.tv_series_id").cast(LongType()).alias("tv_series_id"),

        col("cast.cast_id").cast(LongType()).alias("cast_id"),
        col("cast.person_id").cast(LongType()).alias("person_id"),
        col("cast.character").cast(StringType()).alias("character"),
        col("cast.credit_id").cast(StringType()).alias("credit_id"),
        col("cast.known_for_department").cast(StringType()).alias("known_for_department"),

        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df


def prepare_table_tv_series_crew(
        df: DataFrame
) -> DataFrame:
    """
    Convert DataFrame tv_series to ClickHouse table tv_series_crew format
    """

    exploded_df = df.withColumn("crew", explode(col("parsed_raw_df.crews_info")))
    table_df = exploded_df.select(
        col("parsed_raw_df.tv_series_id").cast(LongType()).alias("tv_series_id"),

        col("crew.person_id").cast(LongType()).alias("person_id"),
        col("crew.department").cast(StringType()).alias("department"),
        col("crew.job").cast(StringType()).alias("job"),
        col("crew.known_for_department").cast(StringType()).alias("known_for_department"),

        col("batch_version").cast(LongType()).alias("batch_version")
    )

    return table_df