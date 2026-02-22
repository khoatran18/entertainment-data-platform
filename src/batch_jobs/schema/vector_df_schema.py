from pyspark.sql.types import StructType, ArrayType, LongType, StructField, FloatType

SPARSE_SCHEMA = StructType([
    StructField("indices", ArrayType(LongType()), False),
    StructField("values", ArrayType(FloatType()), False)
])
