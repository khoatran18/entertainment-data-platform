from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

PARTIAL_EVENT_SCHEMA = StructType([
    StructField("data_type", StringType(), True),
    StructField("data_label", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

ID_SCHEMA = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("tv_series_id", IntegerType(), True),
])