from pyspark.sql import SparkSession, DataFrame

from batch_jobs.config.settings import load_settings


class  PineconeWriter:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.settings = load_settings()

    def write_index(
            self,
            df: DataFrame
    ):
        df.write \
            .option("pinecone.apiKey", self.settings.storage.pinecone.api_key) \
            .option("pinecone.indexName", self.settings.storage.pinecone.index_name) \
            .format("io.pinecone.spark.pinecone.Pinecone") \
            .mode("append") \
            .save()