import logging
import sys

from batch_jobs.config.settings import load_settings
from batch_jobs.io.readers.clickhouse_reader import ClickHouseReader
from batch_jobs.io.writers.neo4j_writer import Neo4jWriter
from batch_jobs.run_time.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from batch_jobs.run_time.redis.redis_client import RedisClient
from batch_jobs.run_time.spark.builder.spark_builder_clickhouse_neo4j_pinecone import \
    create_spark_clickhouse_neo4j_pinecone
from batch_jobs.tranforms.clickhouse_neo4j.join_relationship import join_to_get_relationship
from common.load_path_config import get_valid_invalid_path
from common.logging_config import setup_logging

logger = logging.getLogger(__name__)

def write_clickhouse_to_neo4j():
    """
    Pipeline to write to Neo4j from ClickHouse
    """
    setup_logging()
    logger.info("Batch jobs from ClickHouse to write to Neo4j: Starting...")
    logger.info("Loading configuration...")
    settings = load_settings()
    logger.info("Configuration loaded")

    redis_client = RedisClient()

    builder = create_spark_clickhouse_neo4j_pinecone(app_name=settings.spark.app_name_3, settings=settings)
    spark = builder.getOrCreate()
    spark = prepare_clickhouse_native(spark)
    clickhouse_reader = ClickHouseReader(spark)
    neo4j_writer = Neo4jWriter(spark)

    transform_map = {
        "nodes": { # Key is table name in clickhouse
            "movie": {
                "label": "Movie", "keys": ["movie_id"],
                "select_cols": ["movie_id", "original_title", "overview", "popularity", "release_date", "tagline", "vote_average", "vote_count", "batch_version"],
            },
            "person": {
                "label": "Person", "keys": ["person_id"],
                "select_cols": ["person_id", "name", "gender", "also_known_as", "biography", "birthday", "deathday", "place_of_birth", "known_for_department", "known_for_department", "batch_version"],
            },
            "tv_series": {
                "label": "TV_Series", "keys": ["tv_series_id"], "table_get_batch_version": "tv_series",
                "select_cols": ["tv_series_id", "overview", "popularity", "first_air_date", "tagline", "vote_average", "vote_count", "status", "number_of_seasons", "batch_version"],
            },
        },
        "relationships": [ # Key is number of table to join in ClickHouse
            {
                "ACTED_IN": {
                    "tables": [["movie", "movie_cast"], ["tv_series", "tv_series_cast"]],
                    "diff_col": ["casts_diff", "casts_diff"],
                    "action_col": [["added", "removed"], ["added", "removed"]],
                    "id_col_in_diff": ["person_id", "person_id"],
                    "repartition_cols": [["movie_id"], ["tv_series_id"]],
                    "source_label": ["Person", "Person"],
                    "source_keys": [["person_id"], ["person_id"]],
                    "source_properties": [[], []],
                    "target_label": ["Movie", "TV_Series"],
                    "target_keys": [["movie_id"], ["tv_series_id"]],
                    "target_properties": [[], []],
                    "relationship_properties": [
                        ["cast_id", "character", "credit_id", "known_for_department", "batch_version"],
                        ["cast_id", "character", "credit_id", "known_for_department", "batch_version"]
                    ],
                    "partition_num": 1,
                }
            },
            {
                "WORKS_ON": {
                    "tables": [["movie", "movie_crew"], ["tv_series", "tv_series_crew"]],
                    "diff_col": ["crews_diff", "crews_diff"],
                    "action_col": [["added", "removed"], ["added", "removed"]],
                    "id_col_in_diff": ["person_id", "person_id"],
                    "repartition_cols": [["movie_id"], ["tv_series_id"]],
                    "source_label": ["Person", "Person"],
                    "source_keys": [["person_id"], ["person_id"]],
                    "source_properties": [[], []],
                    "target_label": ["Movie", "TV_Series"],
                    "target_keys": [["movie_id"], ["tv_series_id"]],
                    "target_properties": [[], []],
                    "relationship_properties": [
                        ["department", "job", "known_for_department", "batch_version"],
                        ["department", "job", "known_for_department", "batch_version"]
                    ],
                    "partition_num": 1,
                }
            },
        ]
    }

    logger.info("Starting processing...")
    # Nodes Process
    logger.info("Start processing node data...")
    for table_name, config in transform_map["nodes"].items():
        # Get table config
        label = config["label"]
        keys = config["keys"]
        select_cols = config["select_cols"]
        logger.info(f"Processing table: {table_name}")

        # Get batch version
        version_key = f"{settings.storage.redis.keys.dedup_batch_version}_{table_name}"
        last_version = redis_client.get(version_key)
        logger.info(f"Last version for {version_key} found in Redis: {last_version}")

        # Read from ClickHouse
        filters = [
            {"batch_version": int(last_version)}
        ]
        table_reader = clickhouse_reader.read_table_with_filters(table_name=table_name, filters=filters).select(*select_cols)

        # Start write node
        neo4j_writer.write_node_constraint(df=table_reader, label=label, keys=keys)
        logger.info(f"Finish processing table: {table_name}")

    # Relationship Process
    logger.info("Start processing relationship data...")
    for rel in transform_map["relationships"]:
        # The under dict has only one key
        for relationship, config in rel.items():
            work_lens = len(config["tables"])
            for i in range(work_lens):
                # Get table config
                tables = config["tables"][i]
                repartition_cols = config["repartition_cols"][i]
                action_col = config["action_col"][i]
                source_label = config["source_label"][i]
                source_keys = config["source_keys"][i]
                source_properties = config["source_properties"][i]
                target_label = config["target_label"][i]
                target_keys = config["target_keys"][i]
                target_properties = config["target_properties"][i]
                relationship_properties = config["relationship_properties"][i]
                partition_num = config["partition_num"]
                id_col_in_diff = config["id_col_in_diff"][i]
                diff_col = config["diff_col"][i]
                logger.info(f"id_col_in_diff: {id_col_in_diff}, diff_col: {diff_col}")
                logger.info(f"Processing relationship: {relationship}")

                # Get batch version
                version_key = f"{settings.storage.redis.keys.dedup_batch_version}_{tables[0]}"
                last_version = redis_client.get(version_key)
                logger.info(f"Last version for {version_key} found in Redis: {last_version}")

                # Read from ClickHouse
                filters = [
                    {"batch_version": int(last_version)}
                ]
                left_df = clickhouse_reader.read_table_with_filters(table_name=tables[0], filters=filters)
                right_df = clickhouse_reader.read_table_with_filters(table_name=tables[1], filters=filters)

                action_numbers = len(action_col)
                for j in range(action_numbers):
                    join_df = join_to_get_relationship(
                        left_df=left_df,
                        right_df=right_df,
                        key_cols=target_keys,
                        diff_col=diff_col,
                        action_col=action_col[j],
                        id_col_in_diff=id_col_in_diff,
                        relationship_properties=relationship_properties
                    )

                    # Start write relationship
                    neo4j_writer.write_relationship(
                        df=join_df,
                        repartition_cols=repartition_cols,
                        relationship=relationship,
                        source_label=source_label,
                        source_keys=source_keys,
                        source_properties=source_properties,
                        target_label=target_label,
                        target_keys=target_keys,
                        target_properties=target_properties,
                        relationship_properties=relationship_properties,
                        partition_num=partition_num
                    )
                    logger.info(f"Finish processing relationship: {relationship}")

    logger.info("Processing completed")

if __name__ == "__main__":
    write_clickhouse_to_neo4j()

