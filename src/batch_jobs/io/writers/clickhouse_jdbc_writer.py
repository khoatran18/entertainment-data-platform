import logging

from pyspark.sql import SparkSession, DataFrame

from batch_jobs.config.settings import load_settings, Settings
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

class ClickHouseJdbcWriter:

    def __init__(self, spark: SparkSession):
        setup_logging()
        self.spark = spark
        self.settings: Settings = load_settings()

        self.driver = self.settings.storage.clickhouse.jdbc_driver
        self.host = self.settings.storage.clickhouse.host
        self.port = self.settings.storage.clickhouse.port
        self.database = self.settings.storage.clickhouse.database
        self.username = self.settings.storage.clickhouse.username
        self.password = self.settings.storage.clickhouse.password
        self.url = f"jdbc:ch://{self.host}:{self.port}/{self.database}"

    def write_table(self, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Write table to Clickhouse
        """
        logger.info("Start writing to Clickhouse table: %s.%s", self.database, table_name)
        try:
            df.write \
                .format("jdbc") \
                .option("driver", self.driver) \
                .option("url", self.url) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("isolationLevel", "NONE") \
                .option("dbtable", table_name) \
                .mode(mode) \
                .save()
            logger.info("Finish writing to Clickhouse table: %s.%s", self.database, table_name)
        except Exception as e:
            logger.error("Error when write to Clickhouse table: %s.%s", self.database, table_name)


