from pathlib import Path
import json
import logging
from collections import deque

logger = logging.getLogger(__name__)

DATA_TYPES = ["movie", "tv_series", "person"]
DATA_LABELS = ["old", "new", "change"]

def iter_records_from_file(file_path: Path):
    """
    Load and yield records from file
    """

    with open(file_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception as e:
                    logger.error("Error when load record %d in file %s", line_number, file_path)

def iter_records_from_folder(data_root_path: Path, data_type: str, data_label: str):
    """
    Load and yield records from all data files in folder
    """

    target_folder_path = data_root_path / data_type / data_label

    if not target_folder_path.exists():
        logger.error("Data folder not found: %s", target_folder_path)
        return

    for file_path in sorted(target_folder_path.glob("*.jsonl")):
        yield from iter_records_from_file(file_path)

def iter_full(data_root_path: Path):
    """
    Round robin iterator to load mix data from all data folders
    """

    streams = []
    for _, data_type in enumerate(DATA_TYPES):
        for _, data_label in enumerate(DATA_LABELS):
            stream = iter_records_from_folder(data_root_path, data_type, data_label)
            streams.append((stream, data_type, data_label))

    q = deque([(iter(stream), data_type, data_label) for stream, data_type, data_label in streams])

    while q:
        try:
            stream, data_type, data_label = q.popleft()
            value = next(stream)
            yield value, data_type, data_label
            q.append((stream, data_type, data_label))
        except StopIteration:
            logger.info("Finished loading data")
            pass


