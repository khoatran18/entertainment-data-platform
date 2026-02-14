import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, coalesce

from stream_proccessor.schema.event_schema import PARTIAL_EVENT_SCHEMA, ID_SCHEMA

logger = logging.getLogger(__name__)

def process_event(df: DataFrame) -> DataFrame:
    raw_df = cast_event(df)
    validated_df = valid_full_schema(raw_df)

    return validated_df

def cast_event(df: DataFrame) -> DataFrame:
    raw_df = df.select(
        col("value").cast("string").alias("raw_df"),
        from_json(col("value").cast("string"), PARTIAL_EVENT_SCHEMA).alias("data")
    ).select("data.*", "raw_df")
    return raw_df

def valid_full_schema(df: DataFrame):
    id_info = from_json(col("raw_df"), ID_SCHEMA)
    return df.withColumn("id_info", id_info) \
                .withColumn(
                "valid_schema",
                col("data_type").isNotNull() &
                col("data_label").isNotNull() &
                col("timestamp").isNotNull() &
                coalesce(
                    col("id_info.person_id"),
                    col("id_info.movie_id"),
                    col("id_info.tv_series_id")
                ).isNotNull()
            ).drop("id_info")