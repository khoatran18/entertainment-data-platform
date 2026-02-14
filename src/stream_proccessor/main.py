import logging

from pyspark.sql import SparkSession
import logging
from delta import *

from common.logging_config import setup_logging
from stream_proccessor.config.settings import load_settings
from stream_proccessor.processor.event_processor import process_event
from stream_proccessor.sinks.delta_lake_sink import delta_lake_sink, delta_lake_sink_dql
from stream_proccessor.sources.kafka_source import read_kafka_stream

logger = logging.getLogger(__name__)

def log_batch(df, batch_id):
    logger.info("Batch %s count=%s", batch_id, df.count())
    df.show(truncate=False)


def run_stream():
    setup_logging()

    logger.info("Streaming Processor service starting...")
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    queries = []

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
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minio") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \

    )
    spark = builder.getOrCreate()

    for name, topic in settings.sources.kafka.topics:
        raw_df = read_kafka_stream(spark, settings, topic)
        processed_df = process_event(raw_df)

        target_folder = settings.sinks.delta_lake.target_name_folder.model_dump()[name]

        valid_table_path = f"{settings.sinks.delta_lake.tables.valid_base_path}/{target_folder}"
        invalid_table_path = f"{settings.sinks.delta_lake.tables.invalid_base_path}/{target_folder}"
        valid_checkpoint_path = f"{settings.sinks.delta_lake.checkpoints.valid_base_path}/{target_folder}"
        invalid_checkpoint_path = f"{settings.sinks.delta_lake.checkpoints.invalid_base_path}/{target_folder}"

        valid_query, invalid_query = delta_lake_sink_dql(processed_df, valid_table_path, invalid_table_path, valid_checkpoint_path, invalid_checkpoint_path)
        queries.append(valid_query)
        queries.append(invalid_query)

        # query = delta_lake_sink(processed_df, valid_table_path, valid_checkpoint_path)
        # queries.append(query)

    for query in queries:
        query.awaitTermination()

if __name__== "__main__":
    run_stream()

