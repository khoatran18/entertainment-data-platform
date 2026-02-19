from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, LongType, IntegerType

TV_SERIES_FULL_SCHEMA = StructType([
    StructField("tv_series_id", LongType(), True),
    StructField("casts_info", ArrayType(
        StructType([
            StructField("cast_id", LongType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("person_id", LongType(), True)
        ])
    )),
    StructField("crews_info", ArrayType(
        StructType([
            StructField("department", StringType(), True),
            StructField("job", StringType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("person_id", LongType(), True)
        ])
    )),
    StructField("tv_series_detail", StructType([
        StructField("id", LongType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("first_air_date", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", LongType(), True),
        StructField("status", StringType(), True),
        StructField("genres", ArrayType(
            StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
        StructField("production_countries", ArrayType(
            StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
        StructField("number_of_seasons", IntegerType(), True),
    ]), True)
])