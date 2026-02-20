import logging
import sys

from batch_jobs.config.settings import load_settings
from batch_jobs.io.readers.delta_minio_reader import DeltaMinioReader
from batch_jobs.io.writers.clickhouse_jdbc_writer import ClickHouseJdbcWriter
from batch_jobs.io.writers.clickhouse_native_writer import ClickHouseNativeWriter
from batch_jobs.run_time.clickhouse.clickhouse_init import init_clickhouse
from batch_jobs.run_time.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from batch_jobs.run_time.redis.redis_client import RedisClient
from batch_jobs.run_time.spark.builder.spark_builder_minio_clickhouse import create_spark_minio_clickhouse
from batch_jobs.tranforms.delta_clickhouse.prepare_clickhouse_table import prepare_table_movie, prepare_table_person, \
    prepare_table_movie_cast, prepare_table_movie_crew, prepare_table_tv_series, prepare_table_tv_series_cast, \
    prepare_table_tv_series_crew
from common.load_path_config import get_valid_invalid_path
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

def run_write_to_clickhouse():
    """
    Pipeline to write to Clickhouse from Delta Lake Minio (After dedup timestamp)
    """
    setup_logging()
    logger.info("Batch jobs from Delta Minio to Minio to write to Clickhouse: Starting...")
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    logger.info("Init Clickhouse Table...")
    init_clickhouse()
    logger.info("Finish init Clickhouse Table")

    redis_client = RedisClient()

    builder = create_spark_minio_clickhouse(app_name=settings.spark.app_name_2, settings=settings)
    spark = builder.getOrCreate()
    spark = prepare_clickhouse_native(spark)
    delta_minio_reader = DeltaMinioReader(spark)
    # clickhouse_writer = ClickHouseJdbcWriter(spark)
    clickhouse_writer = ClickHouseNativeWriter(spark)

    transform_map = {
        "movie": [
            {"table_name": "movie", "transform_func": prepare_table_movie},
            {"table_name": "movie_cast", "transform_func": prepare_table_movie_cast},
            {"table_name": "movie_crew", "transform_func": prepare_table_movie_crew},
        ],
        "person": [
            {"table_name": "person", "transform_func": prepare_table_person}
        ],
        "tv_series": [
            {"table_name": "tv_series", "transform_func": prepare_table_tv_series},
            {"table_name": "tv_series_cast", "transform_func": prepare_table_tv_series_cast},
            {"table_name": "tv_series_crew", "transform_func": prepare_table_tv_series_crew},
        ]
    }

    logger.info("Starting processing...")
    for data_type, target_folder in settings.storage.delta_lake.target_name_folder:
        logger.info(f"Processing data type: {data_type}")
        # Get input path and table list
        from_path, _ = get_valid_invalid_path(settings.storage.delta_lake.tables.silver_layer, target_folder)
        if data_type not in transform_map:
            logger.warning(f"Data type {data_type} not found in transform_map. Skipping...")
            continue
        table_list = transform_map[data_type]

        # Get batch version
        version_key = f"{settings.storage.redis.keys.dedup_batch_version}_{data_type}"
        last_version = redis_client.get(version_key)
        if not last_version:
            logger.error(f"CRITICAL: Last version for {version_key} NOT found in Redis. Aborting pipeline.")
            sys.exit(1)
        else:
            logger.info(f"Last version for {version_key} found in Redis: {last_version}")
        filters = {
            "batch_version": int(last_version)
        }

        # Input DataFrame
        from_df = delta_minio_reader.read_table_with_filters(target_path=from_path, filters=filters)

        for table in table_list:
            table_name = table["table_name"]
            transform_func = table["transform_func"]

            table_df = transform_func(from_df)

            logger.info(f"Processing transform for Clickhouse table: {table_name}")
            clickhouse_writer.write_table(df=table_df, table_name=table_name)
            logger.info(f"Finish processing transform for Clickhouse table: {table_name}")
        logger.info(f"Finish processing data type: {data_type}")

    logger.info("Processing completed")

if __name__ == "__main__":
    run_write_to_clickhouse()

