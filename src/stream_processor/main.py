import logging

from pyspark.sql import SparkSession
import logging
from delta import *

from common.logging_config import setup_logging
from common.load_path_config import get_valid_invalid_path
from stream_processor.config.settings import load_settings
from stream_processor.processor.event_processor import process_event
from stream_processor.runtime.minio_client import MinioClient
from stream_processor.sinks.delta_lake_sink import delta_lake_sink, delta_lake_sink_dql
from stream_processor.sources.kafka_source import read_kafka_stream

logger = logging.getLogger(__name__)

def log_batch(df, batch_id):
    logger.info("Batch %s count=%s", batch_id, df.count())
    df.show(truncate=False)


def run_stream():
    # Initial setup
    setup_logging()
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    # Create bucket
    minio_client = MinioClient()
    bucket_name = settings.sinks.delta_lake.core_bucket
    minio_client.make_bucket_if_not_exists(bucket_name)
    logger.info("Streaming Processor service starting...")

    queries = []

    # Create Spark session
    logger.info("Starting Spark session...")
    builder = (
        SparkSession.builder \
            .appName("KafkaStreamToDelta") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", settings.sinks.delta_lake.minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", settings.sinks.delta_lake.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", settings.sinks.delta_lake.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \

    )
    spark = builder.getOrCreate()

    # Start create Kafka streams logic
    logger.info("Spark session started")
    logger.info("Start creating Kafka streams logic...")
    for name, topic in settings.sources.kafka.topics:

        # Process DataFrame
        logger.info("Creating stream logic for topic: %s", topic)
        raw_df = read_kafka_stream(spark, settings, topic)
        processed_df = process_event(raw_df)

        # Get Delta Lake paths
        logger.info("Get all folder names")
        target_folder = settings.sinks.delta_lake.target_name_folder.model_dump()[name]

        # Get Path
        valid_table_path, invalid_table_path = get_valid_invalid_path(settings.sinks.delta_lake.tables, target_folder)
        valid_checkpoint_path, invalid_checkpoint_path = get_valid_invalid_path(settings.sinks.delta_lake.checkpoints, target_folder)
        logger.info("All path for topic %s: \nvalid_table_path: %s \ninvalid_table_path: %s \nvalid_checkpoint_path: %s \ninvalid_checkpoint_path: %s", name, valid_table_path, invalid_table_path, valid_checkpoint_path, invalid_checkpoint_path)

        # Create and append queries
        valid_query, invalid_query = delta_lake_sink_dql(processed_df, valid_table_path, invalid_table_path, valid_checkpoint_path, invalid_checkpoint_path)
        queries.append(valid_query)
        queries.append(invalid_query)

    for query in queries:
        query.awaitTermination()

if __name__== "__main__":
    run_stream()

