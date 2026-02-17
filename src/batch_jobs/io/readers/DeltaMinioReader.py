from pyspark.sql import SparkSession

from batch_jobs.config.settings import Settings


class DeltaMinioReader:

    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings

    def read_table(self, target_path: str):
        return self.spark.read.format("delta").load(target_path)
