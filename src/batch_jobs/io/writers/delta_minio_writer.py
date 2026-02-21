from pyspark.sql import SparkSession, DataFrame

from batch_jobs.config.settings import Settings


class DeltaMinioWriter:
    """
    reate Delta table writer from Minio by Spark
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def overwrite(self, df: DataFrame, target_path: str):
        """
        Write table to Delta with overwrite mode
        """
        return df.write.format("delta").mode("overwrite").save(target_path)

    def append(self, df: DataFrame, target_path: str):
        """
        Write table to Delta with append mode
        """
        return df.write.format("delta").mode("append").save(target_path)

    def write_first_with_cdf(self, df: DataFrame, target_path: str):
        """
        Write table to Delta with overwrite mode and enable Change Data Feed
        """
        return df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").save(target_path)
