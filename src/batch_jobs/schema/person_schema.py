from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, LongType, IntegerType

PERSON_FULL_SCHEMA = StructType([
    StructField("person_id", LongType(), True),
    StructField("person_detail", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("also_known_as", ArrayType(StringType()), True),
        StructField("biography", StringType(), True),
        StructField("birthday", StringType(), True),
        StructField("deathday", StringType(), True),
        StructField("place_of_birth", StringType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("popularity", DoubleType(), True),
    ]), True)
])