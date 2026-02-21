import logging
from pathlib import Path

import clickhouse_connect

from batch_jobs.config.settings import load_settings, Settings
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

def init_clickhouse():
    """
    Init Clickhouse with creating required tables
    """

    setup_logging()
    logger.info("Start init Clickhouse Table...")

    ddl_path = Path(__file__).parent.parent.parent / "script" / "clickhouse" / "tables" / "full_init.sql"
    settings: Settings = load_settings()
    client = clickhouse_connect.get_client(
        host=settings.storage.clickhouse.host,
        port=settings.storage.clickhouse.port,
        username=settings.storage.clickhouse.username,
        password=settings.storage.clickhouse.password
    )

    full_command = ddl_path.read_text()
    commands = [s.strip() for s in full_command.split(";")]
    for sql_command in commands:
        if sql_command:
            logger.info("Execute SQL: %s", sql_command)
            client.command(sql_command)
        else:
            logger.info("Skip empty SQL command")

    logger.info("Finish init Clickhouse Table")

if __name__ == "__main__":
    init_clickhouse()