import logging

from batch_jobs.config.settings import load_settings
from batch_jobs.io.readers.DeltaMinioReader import DeltaMinioReader
from batch_jobs.run_time.spark.builder.spark_builder_minio_clickhouse import create_spark_minio_clickhouse
from batch_jobs.tranforms.delta_delta.upsert_latest import upsert_latest
from common.load_path_config import get_valid_invalid_path
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

def run_dedup_timestamp():
    """
    Pipeline to dedup timestamp, from bronze to silver layer in Delta Lake Minio
    """
    setup_logging()
    logger.info("Batch jobs from Delta Minio to Minio to dedup timestamp: Starting...")
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    builder = create_spark_minio_clickhouse(app_name=settings.spark.app_name_1, settings=settings)
    spark = builder.getOrCreate()

    for data_type, target_folder in settings.storage.delta_lake.target_name_folder:
        # Get Path
        from_path, _ = get_valid_invalid_path(settings.storage.delta_lake.tables.bronze_layer, target_folder)
        to_path, _ = get_valid_invalid_path(settings.storage.delta_lake.tables.silver_layer, target_folder)

        delta_minio_reader = DeltaMinioReader(spark, settings)
        from_df = delta_minio_reader.read_table(from_path)
        key_columns = ["data_type", "id_of_data_type"]
        ts_column = "timestamp"

        logger.info("Upserting data from %s to %s", from_path, to_path)
        upsert_latest(spark=spark, from_df=from_df, to_folder=to_path, key_columns=key_columns, ts_column=ts_column)

if __name__ == "__main__":
    run_dedup_timestamp()