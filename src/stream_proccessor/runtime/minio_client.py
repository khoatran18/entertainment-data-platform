import logging

from minio import Minio

from common.logging_config import setup_logging
from stream_proccessor.config.settings import load_settings

logger = logging.getLogger(__name__)

class MinioClient:
    """
    Minio client to interact with Minio
    """

    def __init__(self):
        setup_logging()
        self.settings = load_settings()
        self.client = Minio(
            endpoint=self.settings.sinks.delta_lake.minio_endpoint_sdk,
            access_key=self.settings.sinks.delta_lake.minio_access_key,
            secret_key=self.settings.sinks.delta_lake.minio_secret_key,
            secure=False
        )

    def bucket_exists(self, bucket_name: str):
        """
        Check if bucket exists
        """
        ans = self.client.bucket_exists(bucket_name)
        logger.info(f"Bucket {bucket_name} exists: {ans}")
        return ans

    def make_bucket(self, bucket_name: str):
        """
        Create bucket
        """
        logger.info(f"Creating bucket {bucket_name}...")
        self.client.make_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} created successfully.")

    def make_bucket_if_not_exists(self, bucket_name: str):
        """
        Create bucket if it does not exist
        """
        if not self.bucket_exists(bucket_name):
            logger.info(f"Bucket {bucket_name} does not exist. Creating bucket...")
            self.make_bucket(bucket_name)
        else:
            logger.info(f"Bucket {bucket_name} already exists.")