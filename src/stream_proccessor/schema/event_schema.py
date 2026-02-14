from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

PARTIAL_EVENT_SCHEMA = StructType([
    StructField("data_type", StringType(), False),
    StructField("data_label", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

ID_SCHEMA = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("tv_series_id", IntegerType(), True),
])