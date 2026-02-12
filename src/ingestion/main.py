import logging
from pathlib import Path

from common.logging_config import setup_logging
from ingestion.config.settings import load_settings
from ingestion.loader.file_loader import iter_full
from ingestion.preprocessor.record_processor import enrich_record
from ingestion.producer.kafka_producer import KafkaProducerService


def main():

    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Ingestion service starting...")

    try:
        # Load config
        logger.info("Loading configuration...")
        settings = load_settings()
        setup_logging()
        logger.info("Configuration loaded")

        # Init Kafka Producer
        producer = KafkaProducerService(settings)
        data_root_path = Path(__file__).resolve().parent / "data"
        topics = {
            "movie": settings.kafka.topics.movie,
            "tv_series": settings.kafka.topics.tv_series,
            "person": settings.kafka.topics.person
        }

        # Main logic
        logger.info("Start loading data...")
        record_flush_buffer = 0
        total_record_count = 0
        for record, data_type, data_label in iter_full(data_root_path):
            # Enrich record
            enriched_record = enrich_record(record, data_type, data_label)

            # Send record to kafka
            producer.send(topics[data_type], enriched_record)
            record_flush_buffer += 1
            total_record_count += 1

            # Flush
            if record_flush_buffer >= settings.kafka.producer.max_buffer:
                producer.flush()
                record_flush_buffer = 0
            logger.info("Processed %d records", total_record_count)

    except Exception as e:
        logger.exception("Fatal error in ingestion service")


if __name__ == "__main__":
    main()