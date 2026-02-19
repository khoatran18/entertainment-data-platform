from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, LongType

MOVIE_FULL_SCHEMA = StructType([
    StructField("movie_id", LongType(), True),
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
    StructField("movie_detail", StructType([
        StructField("id", LongType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("release_date", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", LongType(), True),
        StructField("genres", ArrayType(
            StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
        StructField("belongs_to_collection", ArrayType(
            StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
            ])
        ), True),
        StructField("production_countries", ArrayType(
            StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
    ]))
])