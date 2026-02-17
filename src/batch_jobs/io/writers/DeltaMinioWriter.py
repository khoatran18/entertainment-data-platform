from pyspark.sql import SparkSession, DataFrame

from batch_jobs.config.settings import Settings


class DeltaMinioWriter:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def overwrite(self, df: DataFrame, target_path: str):
        return df.write.format("delta").mode("overwrite").save(target_path)

    def append(self, df: DataFrame, target_path: str):
        return df.write.format("delta").mode("append").save(target_path)
