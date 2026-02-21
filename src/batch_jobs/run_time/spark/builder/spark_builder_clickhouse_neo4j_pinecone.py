from batch_jobs.config.settings import Settings, load_settings
from batch_jobs.run_time.spark.builder.spark_base_builder import spark_base_builder


def create_spark_clickhouse_neo4j_pinecone(
        app_name: str,
        settings: Settings
, base_builder=None):
    """
    Spark builder with all config to interact with Clickhouse, Neo4j and Pinecone
    """
    base_builder = spark_base_builder(app_name)

    packages = [
        "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.9.0",
        "com.clickhouse:clickhouse-jdbc-all:0.9.6",
        "org.jspecify:jspecify:1.0.0",
        "org.neo4j:neo4j-connector-apache-spark_2.12:5.4.0_for_spark_3",
        "io.pinecone:spark-pinecone_2.12:1.2.0"
    ]

    builder = base_builder.master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.jars.packages",",".join(packages)) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.storage.delta_lake.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", settings.storage.delta_lake.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.storage.delta_lake.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("neo4j.url", settings.storage.neo4j.url) \
        .config("neo4j.authentication.basic.username", settings.storage.neo4j.username) \
        .config("neo4j.authentication.basic.password", settings.storage.neo4j.password) \
        .config("neo4j.database", settings.storage.neo4j.database)

    return builder


if __name__ == "__main__":
    spark = create_spark_clickhouse_neo4j_pinecone("test", load_settings()).getOrCreate()
