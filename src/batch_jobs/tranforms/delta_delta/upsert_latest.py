import logging

from delta.tables import *
from pyspark.sql.column import Column
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, row_number

from batch_jobs.tranforms.delta_delta.hash_column import full_hash_and_pre_diff_columns, get_full_diff_by_hash
from batch_jobs.tranforms.delta_delta.parse_schema import parse_schema

HASH_CONFIGS = {
        "movie": {
            "array": [
                {"array_col": "parsed_raw_df.casts_info", "prefix_target_column_name": "casts", "id_element_field": "cast_id"},
                {"array_col": "parsed_raw_df.crews_info", "prefix_target_column_name": "crews", "id_element_field": "person_id"},
            ],
            "vector_info_cols": ["parsed_raw_df.movie_detail.overview"],
            "array_prefix": ["casts", "crews"]
        },
        "tv_series": {
            "array": [
                {"array_col": "parsed_raw_df.casts_info", "prefix_target_column_name": "casts", "id_element_field": "cast_id"},
                {"array_col": "parsed_raw_df.crews_info", "prefix_target_column_name": "crews", "id_element_field": "person_id"},
            ],
            "vector_info_cols": ["parsed_raw_df.tv_series_detail.overview"],
            "array_prefix": ["casts", "crews"]
        },
        "person": {
            "array": [],
            "vector_info_cols": [],
            "array_prefix": []
        }
    }

logger = logging.getLogger(__name__)

def upsert_latest(
        spark: SparkSession,
        from_df: DataFrame,
        data_type: str,
        raw_column: str,
        data_schema,
        to_folder: str,
        key_columns: list[str],
        ts_column: str
):
    """
    Upsert latest batch to Delta Lake, from Bronze to Silver Layer
    """

    logger.info("Start dedup batch in snapshot")
    logger.info("Size before dedup: %s", from_df.count())
    clean_df = dedup_latest_batch(from_df, key_columns, ts_column)
    logger.info("Dedup batch in snapshot successfully completed")
    logger.info("Size after dedup: %s", clean_df.count())

    logger.info("Start parse raw data to schema")
    parsed_df = parse_schema(df=clean_df, col=raw_column, schema=data_schema)
    hashed_df = full_hash_and_pre_diff_columns(parsed_df, HASH_CONFIGS[data_type])

    # Init table
    if not DeltaTable.isDeltaTable(spark, to_folder):
        (
            hashed_df.limit(0)
                .write
                .format("delta")
                .mode("overwrite")
                .save(to_folder)
        )

    logger.info("Start upsert latest batch to delta table: %s", to_folder)
    target_delta_table = DeltaTable.forPath(spark, to_folder)
    final_source_df = get_full_diff_by_hash(
        source_df=hashed_df,
        target_df=target_delta_table.toDF(),
        key_columns=key_columns,
        prefixes=HASH_CONFIGS[data_type]["array_prefix"]
    )
    merge_condition = " AND ".join(
        [f"t.{col} = s.{col}" for col in key_columns]
    )
    update_condition = f"s.{ts_column} > t.{ts_column}"

    (
        target_delta_table.alias("t") \
            .merge(
                source=final_source_df.alias("s"),
                condition=merge_condition,
            ) \
            .whenMatchedUpdateAll(condition=update_condition) \
            .whenNotMatchedInsertAll() \
            .execute()
    )
    logger.info("Upsert latest batch successfully completed")


def dedup_latest_batch(
        df: DataFrame,
        key_columns: list[str],
        ts_column: str
):
    """
    Deduplicate DataFrame by latest timestamp
    """
    window = Window.partitionBy(*key_columns).orderBy(col(ts_column).desc())
    target_df = df.withColumn("row_number", row_number().over(window)) \
                    .filter(col("row_number") == 1) \
                    .drop("row_number")
    return target_df
