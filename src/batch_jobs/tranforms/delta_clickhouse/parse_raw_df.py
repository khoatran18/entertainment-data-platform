from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json


def parse_raw_detail(
        df: DataFrame,
        struct_type
):
    target_df = df.withColumn(
        "details",
        from_json("raw_df", struct_type)
    ).drop("raw_df")

    return target_df
