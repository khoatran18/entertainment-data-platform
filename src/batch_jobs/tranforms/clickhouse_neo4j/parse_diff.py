from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, explode_outer, col, from_json

from batch_jobs.schema.diff_schema import DIFF_SCHEMA


def parse_diff(
        df: DataFrame,
        key_cols: list[str],
        diff_col: str,
        action_diff: str
):
    target_df = df.select(*key_cols, diff_col) \
                    .withColumn("parsed_diff", from_json(diff_col, DIFF_SCHEMA)) \
                    .withColumn("person_id", explode_outer(f"parsed_diff.{action_diff}")) \
                    .filter(col("person_id").isNotNull()) \
                    .select(*key_cols, col("person_id"))

    return target_df