from delta.tables import *

def upsert_latest(
        spark: SparkSession,
        from_df: DataFrame,
        to_folder: str,
        key_columns: list[str],
        ts_column: str
):
    if not DeltaTable.isDeltaTable(spark, to_folder):
        (
            from_df.limit(0)
                .write
                .format("delta")
                .mode("overwrite")
                .save(to_folder)
        )

    target_delta_table = DeltaTable.forPath(spark, to_folder)
    merge_condition = " AND ".join(
        [f"t.{col} = s.{col}" for col in key_columns]
    )
    update_condition = f"s.{ts_column} > t.{ts_column}"

    (
        target_delta_table.alias("t") \
            .merge(
                source=from_df.alias("s"),
                condition=merge_condition,
            ) \
            .whenMatchedUpdateAll(condition=update_condition) \
            .whenNotMatchedInsertAll() \
            .execute()
    )