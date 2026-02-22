import logging

from batch_jobs.config.settings import load_settings, Settings
from batch_jobs.io.readers.clickhouse_reader import ClickHouseReader
from batch_jobs.io.writers.pinecone_writer import PineconeWriter
from batch_jobs.run_time.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from batch_jobs.run_time.redis.redis_client import RedisClient
from batch_jobs.run_time.spark.builder.spark_builder_clickhouse_neo4j_pinecone import \
    create_spark_clickhouse_neo4j_pinecone
from batch_jobs.tranforms.clickhouse_pinecone.prepare_vector_df import prepare_vector_schema
from common.logging_config import setup_logging

TRANSFORM_MAP = {
    "movie": {
        "id_col": "movie_id",
        "vector_prepare_cols": ["overview", "tagline"],
        "vector_col_name": "document" ,
        "metadata": "{}"
    },
    "tv_series": {
        "id_col": "tv_series_id",
        "vector_prepare_cols": ["overview", "tagline"],
        "vector_col_name": "document" ,
        "metadata": "{}"
    },
}

logger = logging.getLogger(__name__)

def write_clickhouse_to_pinecone(transform_map=TRANSFORM_MAP):
    """
    Pipeline to write to Neo4j from ClickHouse
    """
    # Config
    setup_logging()
    logger.info("Batch jobs from ClickHouse to write to Pinecone: Starting...")
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    redis_client = RedisClient()

    # Create Spark session
    builder = create_spark_clickhouse_neo4j_pinecone(app_name=settings.spark.app_name_3, settings=settings)
    spark = builder.getOrCreate()
    spark = prepare_clickhouse_native(spark)
    clickhouse_reader = ClickHouseReader(spark)
    neo4j_writer = PineconeWriter(spark)

    logger.info("Starting processing...")
    for table_name, config in transform_map.items():
        logger.info(f"Processing table: {table_name}")
        # Get batch version
        version_key = f"{settings.storage.redis.keys.dedup_batch_version}_{table_name}"
        last_version = redis_client.get(version_key)
        logger.info(f"Last version for {version_key} found in Redis: {last_version}")

        filters = [
            {"batch_version": int(last_version)}
        ]
        table_reader = clickhouse_reader.read_table_with_filters(table_name=table_name, filters=filters)
        vector_df = prepare_vector_schema(
            df=table_reader,
            id_col=config["id_col"],
            namespace=settings.storage.pinecone.namespace.model_dump()[table_name],
            model_name="intfloat/e5-large-v2",
            vector_prepare_cols=config["vector_prepare_cols"],
            vector_col_name=config["vector_col_name"],
            metadata=config["metadata"]
        )
        neo4j_writer.write_index(vector_df)
        logger.info(f"Finish processing table: {table_name}")



    logger.info("Processing completed")

if __name__ == "__main__":
    write_clickhouse_to_pinecone()

