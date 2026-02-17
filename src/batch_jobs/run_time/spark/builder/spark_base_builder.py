from pyspark.sql import SparkSession


def spark_base_builder(app_name: str):
    """
    Base Spark Builder
    """
    return (
        SparkSession.builder.appName(app_name)
    )