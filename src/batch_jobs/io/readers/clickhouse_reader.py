from airflow.models import DagBag
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from batch_jobs.config.settings import load_settings, Settings


class ClickHouseReader:
    """
    Create Delta table reader from Minio by Spark
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.settings: Settings = load_settings()

        self.driver = self.settings.storage.clickhouse.jdbc_driver
        self.host = self.settings.storage.clickhouse.host
        self.port = self.settings.storage.clickhouse.port
        self.database = self.settings.storage.clickhouse.database
        self.username = self.settings.storage.clickhouse.username
        self.password = self.settings.storage.clickhouse.password
        self.url = f"jdbc:ch://{self.host}:{self.port}/{self.database}"

    def read_table(self, query: str):
        df = self.spark.sql(query)
        return df

    def read_table_with_filters(self, table_name: str, filters: list[dict]):
        df = self.spark.table(f"clickhouse.{self.database}.{table_name}")
        for filter_dict in filters:
            for column_name, value in filter_dict.items():
                df = df.filter(col(column_name) == value)

        return df

