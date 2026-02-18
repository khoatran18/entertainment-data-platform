from pyspark.sql import SparkSession

from batch_jobs.config.settings import Settings


class DeltaMinioReader:
    """
    Create Delta table reader from Minio by Spark
    """

    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings

    def read_table(self, target_path: str):
        return self.spark.read.format("delta").load(target_path)

    def read_table_cdf(
            self,
            target_path: str,
            start_timestamp: str | None = None,
            end_timestamp: str | None = None
    ):
        reader = self.spark.read.format("delta") \
                        .option("readChangeFeed", "true")

        if start_timestamp is not None:
            reader = reader.option("startingTimestamp", start_timestamp)
        if end_timestamp is not None:
            reader = reader.option("endingTimestamp", end_timestamp)

        return reader.load(target_path)