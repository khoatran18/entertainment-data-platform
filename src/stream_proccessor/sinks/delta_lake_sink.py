import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

def delta_lake_sink(
    df: DataFrame,
    table: str,
    checkpoint: str
):
    """
    Create a single write stream to Delta Lake.
    """
    def log_batch(batch_df, batch_id):
        logger.info("Batch %s count=%s", batch_id, batch_df.count())
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .save(table)
        batch_df.show(truncate=False)

    return (
        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint) \
            .foreachBatch(log_batch)
            .start(table)
    )

def split_valid_invalid_stream(df: DataFrame):
    """
    Split to 2 DataFrames by "valid_schema" column.
    """
    valid_df = df.filter(col("valid_schema") == True).drop("valid_schema")
    invalid_df = df.filter(col("valid_schema") == False)
    return valid_df, invalid_df

def delta_lake_sink_dql(
        df: DataFrame,
        valid_table: str,
        invalid_table: str,
        valid_checkpoint: str,
        invalid_checkpoint: str
):
    """
    Create 2 write streams to Delta Lake, one for valid events and another for invalid events.
    """
    valid_df, invalid_df = split_valid_invalid_stream(df)

    valid_query = delta_lake_sink(valid_df, valid_table, valid_checkpoint)
    invalid_query = delta_lake_sink(invalid_df, invalid_table, invalid_checkpoint)

    return valid_query, invalid_query