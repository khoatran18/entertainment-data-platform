from pyspark.sql.types import StructType, StructField, ArrayType, LongType

DIFF_SCHEMA = StructType([
    StructField("added", ArrayType(LongType()), True),
    StructField("removed", ArrayType(LongType()), True)
])