import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, coalesce, current_timestamp

from stream_proccessor.schema.event_schema import PARTIAL_EVENT_SCHEMA, ID_SCHEMA

logger = logging.getLogger(__name__)

def process_event(df: DataFrame) -> DataFrame:
    """
    Full logic processing event
    """
    raw_df = cast_event(df)
    enriched_df = enrich_event(raw_df)
    validated_df = valid_full_schema(enriched_df)

    return validated_df

def cast_event(df: DataFrame) -> DataFrame:
    """
    Cast event to string and parse json.
    """
    raw_df = df.select(
        col("value").cast("string").alias("raw_df"),
        from_json(col("value").cast("string"), PARTIAL_EVENT_SCHEMA).alias("data")
    ).select("data.*", "raw_df")
    return raw_df

def valid_full_schema(df: DataFrame):
    """
    Check valid schema by adding boolean value to Column "valid_schema"
    """
    id_info = from_json(col("raw_df"), ID_SCHEMA)
    df.withColumn("id_info", id_info)
    return df.withColumn("id_info", id_info) \
                .withColumn(
                    "id_of_data_type",
                    coalesce(col("id_info.person_id"), col("id_info.movie_id"), col("id_info.tv_series_id"))
                ) \
                .withColumn(
                "valid_schema",
                col("data_type").isNotNull() &
                col("data_label").isNotNull() &
                col("timestamp").isNotNull() &
                col("process_timestamp").isNotNull() &
                col("id_of_data_type").isNotNull()
            ) \
            .drop("id_info")

def enrich_event(df: DataFrame):
    """
    Enrich event with more information: process_timestamp, ...
    """
    enriched_df =  df.withColumn("process_timestamp", current_timestamp())
    return enriched_df.select(
        "process_timestamp",
        *[c for c in enriched_df.columns if c != "process_timestamp"]
    )