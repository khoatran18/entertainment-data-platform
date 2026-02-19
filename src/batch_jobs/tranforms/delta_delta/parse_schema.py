from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json


def parse_schema(
        df: DataFrame,
        col: str,
        schema
):
    """
    Parse schema from DataFrame
    :param df: DataFrame to parse
    :param col: column need to parse
    :param schema: data schema
    :return: Dataframe with parsed column (name = parsed_<col>)
    """
    target_col_name = f"parsed_{col}"
    target_df = df.withColumn(target_col_name, from_json(col, schema)).drop(col)
    return target_df
