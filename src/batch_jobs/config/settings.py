import os
from pathlib import Path
import yaml
import logging
from pydantic import BaseModel
from dotenv import load_dotenv

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

##### Neo4j Setting
class Neo4jSettings(BaseModel):
    url: str
    username: str
    password: str
    database: str
    instance_id: str
    instance_name: str
    batch_size: int

##### Storage setting
class StorageSettings(BaseModel):
    delta_lake: DeltaLakeSettings
    clickhouse: ClickhouseSettings
    redis: RedisSettings
    neo4j: Neo4jSettings

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

    ENV_PATH = Path(__file__).parent / ".env"
    if ENV_PATH.exists():
        logger.info("Load env file from %s", ENV_PATH)
        load_dotenv(dotenv_path=ENV_PATH)
    else:
        logger.warning("No env file found at %s", ENV_PATH)

    env = os.getenv("APP_ENV", "dev")
    config_path = BASE_DIR / f"config.{env}.yml"

    logger.info("Loading config with env=%s", env)
    logger.info("Config path: %s", config_path)

    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        raise  FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    cfg = load_env(cfg)

    logger.info("Load config successfully!")

    return Settings(**cfg)

def load_env(cfg):
    """
    Load .env to config
    """
    if os.getenv("NEO4J_URI"):
        cfg["storage"]["neo4j"]["url"] = os.getenv("NEO4J_URI")
        logger.info("Set NEO4J_URI to config")
    else:
        logger.warning("NEO4J_URI not found in env, use default value")

    if os.getenv("NEO4J_USERNAME"):
        cfg["storage"]["neo4j"]["username"] = os.getenv("NEO4J_USERNAME")
        logger.info("Set NEO4J_USERNAME to config")
    else:
        logger.warning("NEO4J_USERNAME not found in env, use default value")

    if os.getenv("NEO4J_PASSWORD"):
        cfg["storage"]["neo4j"]["password"] = os.getenv("NEO4J_PASSWORD")
        logger.info("Set NEO4J_PASSWORD to config")
    else:
        logger.warning("NEO4J_PASSWORD not found in env, use default value")

    if os.getenv("NEO4J_DATABASE"):
        cfg["storage"]["neo4j"]["database"] = os.getenv("NEO4J_DATABASE")
        logger.info("Set NEO4J_DATABASE to config")
    else:
        logger.warning("NEO4J_DATABASE not found in env, use default value")

    if os.getenv("AURA_INSTANCEID"):
        cfg["storage"]["neo4j"]["instance_id"] = os.getenv("AURA_INSTANCEID")
        logger.info("Set AURA_INSTANCEID to config")
    else:
        logger.warning("AURA_INSTANCEID not found in env, use default value")

    if os.getenv("AURA_INSTANCENAME"):
        cfg["storage"]["neo4j"]["instance_name"] = os.getenv("AURA_INSTANCENAME")
        logger.info("Set AURA_INSTANCENAME to config")
    else:
        logger.warning("AURA_INSTANCENAME not found in env, use default value")

    return cfg


if __name__ == "__main__":
    from common.logging_config import setup_logging
    setup_logging()

    print(load_settings())




