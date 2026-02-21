import logging

import redis

from batch_jobs.config.settings import Settings, load_settings
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

class RedisClient:

    def __init__(self):
        logger.info("Init Redis client...")
        setup_logging()
        self.settings: Settings = load_settings()
        self.client = redis.Redis(
            host=self.settings.storage.redis.host,
            port=self.settings.storage.redis.port,
            decode_responses=True
        )
        logger.info("Redis client initialized")

    def set(self, key, value):
        """
        Set key-value pair to Redis
        """
        logger.info(f"Set key {key} with value {value}")
        self.client.set(name=key, value=value)

    def get(self, key):
        """
        Get value by key from Redis
        """
        value = self.client.get(key)
        logger.info(f"Get key {key} with value {value}")
        return value