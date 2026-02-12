import os
from pathlib import Path
import yaml
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent

class TopicSettings(BaseModel):
    movie: str
    tv_series: str
    person: str

class KafkaServerSettings(BaseModel):
    bootstrap_servers: str

class ProducerSettings(BaseModel):
    retries: int
    max_buffer: int

class KafkaSettings(BaseModel):
    topics: TopicSettings
    server: KafkaServerSettings
    producer: ProducerSettings

class Settings(BaseModel):
    kafka: KafkaSettings

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




