from pyspark.sql import DataFrame


def console_sink(
        df: DataFrame
):
    df.writeStream.format("console").outputMode("append").start()