import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import transform, col, struct, xxhash64, array_sort, when, array_except, array, lit, coalesce
from pyspark.sql.types import ArrayType, LongType, StructType

from common.logging_config import setup_logging

ELEMENTS_HASH_SUFFIX = "_elements_hash"
TOTAL_HASH_SUFFIX = "_total_hash"
DIFF_SUFFIX = "_diff"
VECTOR_HASH_COL = "vector_info_hash"
VECTOR_HASH_DIFF_COL = "vector_info_hash_diff"

logger = logging.getLogger(__name__)
setup_logging()

def hash_cols_to_vector_check(
        df: DataFrame,
        cols: list[str]
):
    if cols is None or len(cols) == 0:
        logger.warning("No columns to hash, skipping")
        return df

    logger.info("Hashing columns: %s", cols)
    df = df.withColumn(f"{VECTOR_HASH_COL}", xxhash64(*cols))
    return df


def hash_array(
        df: DataFrame,
        configs: list
):
    """
    configs: list of dicts [{"array_col": "casts", "prefix_target_column_name": "cast", "id_element_field": "person_id"}, ...]
    """
    if configs is None or len(configs) == 0:
        return df

    for config in configs:
        array_col = config["array_col"]
        prefix_target_column_name = config["prefix_target_column_name"]
        id_element_field = config["id_element_field"]

        # Hash elements
        df = df.withColumn(
            f"{prefix_target_column_name}{ELEMENTS_HASH_SUFFIX}",
            transform(
                col=col(array_col),
                f=lambda x: struct(
                    x[id_element_field].alias("id"),
                    xxhash64(x).alias("hash")
                )
            )
        )

        # Hash full array
        df = df.withColumn(
            f"{prefix_target_column_name}{TOTAL_HASH_SUFFIX}",
            xxhash64(array_sort(transform(col=col(f"{prefix_target_column_name}{ELEMENTS_HASH_SUFFIX}"), f=lambda x: x["hash"])))
        )

    return df

def add_pre_diff_columns(df: DataFrame, configs: list):
    if configs is None or len(configs) == 0:
        return df

    select_cols = [col("*"), lit(False).alias(f"{VECTOR_HASH_DIFF_COL}")]

    for config in configs:
        prefix_target_column_name = config["prefix_target_column_name"]
        empty_struct = get_empty_ids_array_struct()
        select_cols.append(empty_struct.alias(f"{prefix_target_column_name}{DIFF_SUFFIX}"))

    return df.select(*select_cols)

def get_empty_ids_array_struct():
    empty_ids_array = array().cast(ArrayType(LongType()))
    return struct(
                empty_ids_array.alias("added"),
                empty_ids_array.alias("removed")
            )

def full_hash_and_pre_diff_columns(df: DataFrame, configs: dict[str, list]):
    if configs is None or len(configs) == 0:
        return df

    array_conf = configs["array"]
    vector_info_cols_conf = configs["vector_info_cols"]
    df = hash_cols_to_vector_check(df, vector_info_cols_conf)
    df = hash_array(df, array_conf)
    df = add_pre_diff_columns(df, array_conf)

    return df

def get_full_diff_by_hash(
    source_df: DataFrame,
    target_df: DataFrame,
    key_columns: list[str],
    prefixes: list[str]
):
    if prefixes is None or len(prefixes) == 0:
        return source_df

    # Optimize: get only required columns from target table
    required_cols = key_columns + [f"{prefix}{ELEMENTS_HASH_SUFFIX}" for prefix in prefixes] + [f"{prefix}{TOTAL_HASH_SUFFIX}" for prefix in prefixes] + [VECTOR_HASH_COL]
    join_df = source_df.alias("s").join(
        other=target_df.select(*required_cols).alias("t"),
        on=key_columns,
        how="left"
    )
    select_cols = [col(f"s.{c}") for c in source_df.columns if not c.endswith(DIFF_SUFFIX)]

    # For vector hash
    vector_diff_col = when(
        col(f"t.{VECTOR_HASH_COL}").isNull() |
        (col(f"s.{VECTOR_HASH_COL}") != col(f"t.{VECTOR_HASH_COL}")),
        lit(True)
    ).otherwise(lit(False))
    select_cols.append(vector_diff_col.alias(VECTOR_HASH_DIFF_COL))

    # For array hash
    for prefix in prefixes:
        # Get hash column names
        s_elements_hash = f"s.{prefix}{ELEMENTS_HASH_SUFFIX}"
        s_total_hash = f"s.{prefix}{TOTAL_HASH_SUFFIX}"
        t_elements_hash = f"t.{prefix}{ELEMENTS_HASH_SUFFIX}"
        t_total_hash = f"t.{prefix}{TOTAL_HASH_SUFFIX}"
        # When it is a new record, also need added to process more efficiently in building GraphDB
        safe_t_elements_hash = coalesce(t_elements_hash, array().cast("array<struct<id:bigint,hash:bigint>>"))

        # Get diff columns
        elements_add = array_except(s_elements_hash, safe_t_elements_hash)
        elements_remove = array_except(safe_t_elements_hash, s_elements_hash)

        # Get only ids in diff columns
        ids_add = transform(elements_add, lambda x: x["id"])
        ids_remove = transform(elements_remove, lambda x: x["id"])

        diff_col = when(
            (col(s_total_hash) != col(t_total_hash)) | col(t_total_hash).isNull(),
            struct(
                ids_add.alias("added"),
                ids_remove.alias("removed")
            )
        ).otherwise(get_empty_ids_array_struct())

        select_cols.append(diff_col.alias(f"{prefix}{DIFF_SUFFIX}"))

    return join_df.select(*select_cols)

