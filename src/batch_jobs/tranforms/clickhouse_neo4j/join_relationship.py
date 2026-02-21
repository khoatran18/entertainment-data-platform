from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, explode_outer, col, from_json

from batch_jobs.schema.diff_schema import DIFF_SCHEMA


def parse_diff(
        df: DataFrame,
        key_cols: list[str],
        diff_col: str,
        action_col: str,
        id_col_in_diff: str
):
    target_df = df.select(*key_cols, diff_col) \
                    .withColumn("parsed_diff", from_json(diff_col, DIFF_SCHEMA)) \
                    .withColumn(id_col_in_diff, explode_outer(f"parsed_diff.{action_col}")) \
                    .filter(col(id_col_in_diff).isNotNull()) \
                    .select(*key_cols, col(id_col_in_diff))

    return target_df


def join_to_get_relationship(
        left_df: DataFrame,
        right_df: DataFrame,
        key_cols: list[str],
        diff_col: str,
        action_col: str,
        id_col_in_diff: str,
        relationship_properties: list[str]
):
    parse_diff_df = parse_diff(left_df, key_cols, diff_col, action_col, id_col_in_diff)

    join_df = parse_diff_df.join(
        right_df,
        on=[*key_cols, id_col_in_diff],
        how="inner"
    ).select(*key_cols, id_col_in_diff, *relationship_properties)

    return join_df
