from batch_jobs.config.settings import Settings
from batch_jobs.run_time.spark.builder.spark_base_builder import spark_base_builder


def create_spark_minio_clickhouse(
        app_name: str,
        settings: Settings
):
    """
    Spark builder with all config to interact with Delta Lake and Clickhouse
    """
    base_builder = spark_base_builder(app_name)

    builder = base_builder.master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", settings.storage.delta_lake.minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", settings.storage.delta_lake.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", settings.storage.delta_lake.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \

    return builder