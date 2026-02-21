from pyspark.sql import SparkSession

from batch_jobs.config.settings import load_settings


def prepare_clickhouse_native(
        spark: SparkSession
):
    """
    Prepare Spark to interact with Clickhouse Native Driver
    """
    settings = load_settings()
    spark.conf.set("spark.sql.catalog.clickhouse", settings.storage.clickhouse.native_driver)
    spark.conf.set("spark.sql.catalog.clickhouse.host", settings.storage.clickhouse.host)
    spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
    spark.conf.set("spark.sql.catalog.clickhouse.http_port", settings.storage.clickhouse.port)
    spark.conf.set("spark.sql.catalog.clickhouse.user", settings.storage.clickhouse.username)
    spark.conf.set("spark.sql.catalog.clickhouse.password", settings.storage.clickhouse.password)
    spark.conf.set("spark.sql.catalog.clickhouse.database", settings.storage.clickhouse.password)
    spark.conf.set("spark.clickhouse.write.format", "json")

    return spark