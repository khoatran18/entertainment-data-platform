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
    """
    Parse diff column to get list of added/removed ids
    :param df: DataFrame to parse
    :param key_cols: Columns to join on, to have after parsed
    :param diff_col: Column with diff
    :param action_col: Column with action (add/remove) in diff
    :param id_col_in_diff: Column id name in action column of diff
    :return: DataFrame with parsed diff column
    """
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
    """
    Join left DataFrame to right DataFrame by key columns and diff column to get list of added/removed ids, to final relationship DataFrame
    :param left_df: Main DataFrame to join (have diff column)
    :param right_df: DataFrame to join with (have ids in left_df diff column)
    :param key_cols: Columns to join on, to have after parsed
    :param diff_col: Column with diff
    :param action_col: Column with action (add/remove) in diff
    :param id_col_in_diff: Id column name in diff column
    :param relationship_properties: Columns to select to create relationship
    :return: DataFrame with relationship
    """
    parse_diff_df = parse_diff(left_df, key_cols, diff_col, action_col, id_col_in_diff)

    join_df = parse_diff_df.join(
        right_df,
        on=[*key_cols, id_col_in_diff],
        how="inner"
    ).select(*key_cols, id_col_in_diff, *relationship_properties)

    return join_df
