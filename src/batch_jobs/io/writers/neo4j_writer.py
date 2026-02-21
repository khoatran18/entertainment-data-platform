import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from batch_jobs.config.settings import load_settings
from common.logging_config import setup_logging

DELETED_COL_FLAG = "is_deleted"

logger = logging.getLogger(__name__)

class Neo4jWriter:

    def __init__(self, spark: SparkSession):
        setup_logging()
        self.spark = spark
        self.settings = load_settings()

    def write_node_constraint(
            self,
            df: DataFrame,
            label: str,
            keys: list[str]
    ):
        """
        Write Node to Neo4j with strict constraint
        :param df: DataFrame to write
        :param label: Label of Node
        :param keys: Keys of Node
        """
        logger.info("Start writing Node (strict mode) to Neo4j with label: %s", label)
        try:
            format_label = f":{label}"
            format_keys = ",".join(keys)
            df.write.format("org.neo4j.spark.DataSource") \
                .mode("Overwrite") \
                .option("labels", format_label) \
                .option("node.keys", format_keys) \
                .option("schema.optimization.node.keys", "KEY") \
                .option("schema.optimization", "TYPE") \
                .option("batch.size", self.settings.storage.neo4j.batch_size) \
                .save()
            logger.info("Finish writing Node to Neo4j with label: %s", label)
        except Exception as e:
            logger.error("Error when writing Node to Neo4j with label: %s", label)
            raise e

    def write_node_normal(
            self,
            df: DataFrame,
            label: str,
            keys: list[str],
    ):
        """
        Write Node to Neo4j with normal constraint
        :param df: DataFrame to write
        :param label: Label of Node
        :param keys: Keys of Node
        """
        logger.info("Start writing Node (normal mode) to Neo4j with label: %s", label)
        try:
            format_label = f":{label}"
            format_keys = ",".join(keys)
            df.write.format("org.neo4j.spark.DataSource") \
                .mode("Overwrite") \
                .option("labels", format_label) \
                .option("node.keys", format_keys) \
                .option("schema.optimization.node.keys", "UNIQUE") \
                .option("node.keys.skip.nulls", "true") \
                .option("schema.optimization", "TYPE") \
                .option("batch.size", self.settings.storage.neo4j.batch_size) \
                .save()
            logger.info("Finish writing Node to Neo4j with label: %s", label)
        except Exception as e:
            logger.error("Error when writing Node to Neo4j with label: %s", label)
            raise e

    def write_relationship(
            self,
            df: DataFrame,
            repartition_cols: list[str],
            relationship: str,
            source_label: str,
            source_keys: list[str],
            source_properties: list[str],
            target_label: str,
            target_keys: list[str],
            target_properties: list[str],
            relationship_properties: list[str],
            partition_num: int = 1,
            action_col: str = "added"
    ):
        """
        Write Relationship to Neo4j
        :param df: DataFrame to write
        :param repartition_cols: Repartition columns
        :param relationship: Relationship name
        :param source_label: Label of source node
        :param source_keys: Keys of source node
        :param source_properties: Properties of source node
        :param target_label: Label of target node
        :param target_keys: Keys of target node
        :param target_properties: Properties of target node
        :param relationship_properties: Relationship properties
        :param partition_num: Partition number
        :param action_col: Action column name in diff column (added/removed)
        :return:
        """
        is_deleted_action = True if action_col == "removed" else False
        logger.info("Start writing Relationship (%s action) to Neo4j with relationship: %s", action_col, relationship)
        try:
            enriched_df = df.withColumn(DELETED_COL_FLAG, lit(is_deleted_action))
            repartition_df = enriched_df.repartition(partition_num, *repartition_cols)
            logger.info("--------------------------------------------source_keys: %s, target_keys: %s", ",".join(source_keys), ",".join(target_keys))
            repartition_df.write.format("org.neo4j.spark.DataSource") \
                .mode("Overwrite") \
                .option("relationship", relationship) \
                .option("relationship.save.strategy", "keys") \
                .option("relationship.source.save.mode", "Overwrite") \
                .option("relationship.source.labels", f":{source_label}") \
                .option("relationship.source.node.keys", ",".join(source_keys)) \
                .option("relationship.source.node.properties", ",".join(source_properties)) \
                .option("relationship.target.save.mode", "Overwrite") \
                .option("relationship.target.labels", f":{target_label}") \
                .option("relationship.target.node.keys", ",".join(target_keys)) \
                .option("relationship.target.node.properties", ",".join(target_properties)) \
                .option("relationship.properties", ",".join(relationship_properties + [DELETED_COL_FLAG])) \
                .option("batch.size", self.settings.storage.neo4j.batch_size) \
                .save()

            logger.info("Finish writing Relationship (%s action) to Neo4j with relationship: %s", action_col, relationship)
        except Exception as e:
            logger.error("Error when writing Relationship (%s action) to Neo4j with relationship: %s", action_col, relationship)
            raise e