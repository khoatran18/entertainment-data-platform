import logging

from pyspark.sql import SparkSession, DataFrame

from stream_processor.config.settings import Settings

logger = logging.getLogger(__name__)

def read_kafka_stream(
        spark: SparkSession,
        settings: Settings,
        topic: str
) -> DataFrame:
    """
    Create Spark Kafka stream
    """

    return (spark.readStream
                .format("kafka") \
                .option("kafka.bootstrap.servers", settings.sources.kafka.server.bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("kafka.isolation.level", "read_committed") \
                .load()
            )