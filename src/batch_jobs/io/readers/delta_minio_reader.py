from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from batch_jobs.config.settings import Settings, load_settings

class DeltaMinioReader:
    """
    Create Delta table reader from Minio by Spark
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.settings = load_settings()

    def read_table(self, target_path: str):
        return self.spark.read.format("delta").load(target_path)

    def  read_table_with_filters(self, target_path: str, filters: dict):
        df = self.read_table(target_path)
        if not filters:
            return df

        target_df = reduce(
            lambda temp_df, item: temp_df.filter(col(item[0]) == item[1]),
            filters.items(),
            df
        )

        return target_df

    def read_table_cdf(
            self,
            target_path: str,
            start_timestamp: str | None = None,
            end_timestamp: str | None = None,
            start_version: int | None = None,
            end_version: int | None = None
    ):
        reader = self.spark.read.format("delta") \
                        .option("readChangeFeed", "true")

        if start_timestamp is not None:
            reader = reader.option("startingTimestamp", start_timestamp)
        if end_timestamp is not None:
            reader = reader.option("endingTimestamp", end_timestamp)
        if start_version is not None:
            reader = reader.option("startingVersion", start_version)
        if end_version is not None:
            reader = reader.option("endingVersion", end_version)

        return reader.load(target_path)