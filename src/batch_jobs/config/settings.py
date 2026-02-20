import os
from pathlib import Path
import yaml
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent

##### DeltaLake setting
class DeltaLakeTargetNameFolderSettings(BaseModel):
    movie: str
    person: str
    tv_series: str

# Delta Lake tables
class DeltaLakeBronzeTablesSettings(BaseModel):
    valid_base_path: str
    invalid_base_path: str

class DeltaLakeSilverTablesSettings(BaseModel):
    valid_base_path: str
    invalid_base_path: str

class DeltaLakeTablesSettings(BaseModel):
    bronze_layer: DeltaLakeBronzeTablesSettings
    silver_layer: DeltaLakeSilverTablesSettings

# Delta Lake checkpoints
class DeltaLakeCheckpointToDeltaSettings(BaseModel):
    valid_base_path: str
    invalid_base_path: str

class DeltaLakeCheckpointToClickhouseSettings(BaseModel):
    valid_base_path: str
    invalid_base_path: str

class DeltaLakeCheckpointsSettings(BaseModel):
    write_to_delta: DeltaLakeCheckpointToDeltaSettings
    write_to_clickhouse: DeltaLakeCheckpointToClickhouseSettings

# Full Delta Lake settings
class DeltaLakeSettings(BaseModel):
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    target_name_folder: DeltaLakeTargetNameFolderSettings
    tables: DeltaLakeTablesSettings

##### Clickhouse setting
class ClickhouseTablesSettings(BaseModel):
    movie: str
    person: str
    tv_series: str

class ClickhouseSettings(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str
    jdbc_driver: str
    native_driver: str
    tables: ClickhouseTablesSettings

##### Redis Setting
class RedisKeySettings(BaseModel):
    dedup_batch_version: str

class RedisSettings(BaseModel):
    host: str
    port: int
    keys: RedisKeySettings

##### Storage setting
class StorageSettings(BaseModel):
    delta_lake: DeltaLakeSettings
    clickhouse: ClickhouseSettings
    redis: RedisSettings

##### Spark setting
class SparkSettings(BaseModel):
    app_name_1: str
    app_name_2: str

##### Main settings
class Settings(BaseModel):
    storage: StorageSettings
    spark: SparkSettings

def load_settings() -> Settings:
    """
    Get APP_ENV to get config file and load
    """

    env = os.getenv("APP_ENV", "dev")
    config_path = BASE_DIR / f"config.{env}.yml"

    logger.info("Loading config with env=%s", env)
    logger.info("Config path: %s", config_path)

    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        raise  FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    logger.info("Load config successfully!")

    return Settings(**cfg)


if __name__ == "__main__":
    from common.logging_config import setup_logging
    setup_logging()

    print(load_settings())




