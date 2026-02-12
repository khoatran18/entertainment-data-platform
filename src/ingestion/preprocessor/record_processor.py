import logging
import random
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def get_random_timestamp(start: datetime, end: datetime) -> datetime:
    """
    Get random timestamp between start and end
    """

    delta_time = int((end-start).total_seconds())
    random_second = random.randint(0, delta_time)
    target = start + timedelta(seconds=random_second)
    target = target.astimezone(timezone.utc)

    return target.strftime("%Y-%m-%dT%H:%M:%SZ")


def enrich_record(
        record: dict,
        data_type: str,
        data_label: str
) -> dict:
    """
    Add metadata to record
    """

    enriched_record = record.copy()
    enriched_record["data_type"] = data_type
    enriched_record["data_label"] = data_label

    if data_label == "old":
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    elif data_label in ["new", "change"]:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 2, 20, 23, 59, 59, tzinfo=timezone.utc)
    else:
        logger.error("Invalid data label: %s", data_label)
        return enriched_record

    enriched_record["timestamp"] = get_random_timestamp(start, end)

    return enriched_record