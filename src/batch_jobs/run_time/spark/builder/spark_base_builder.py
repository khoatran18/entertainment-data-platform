from pyspark.sql import SparkSession


def spark_base_builder(app_name: str):
    return (
        SparkSession.builder.appName(app_name)
    )