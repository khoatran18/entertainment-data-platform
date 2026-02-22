import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col, lit, concat_ws
from pyspark.sql.pandas.functions import PandasUDFType
from pyspark.sql.types import FloatType, ArrayType, StringType
from sentence_transformers import SentenceTransformer

from batch_jobs.schema.vector_df_schema import SPARSE_SCHEMA

# model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

def get_embed_udf(model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
    """
    Create UDF to embed text into vector
    """

    model_holder = {"model": None}

    @pandas_udf(ArrayType(FloatType()))
    def embed_udf(texts: pd.Series) -> pd.Series:
        """
        Embed text column into vector
        """
        if model_holder["model"] is None:
            from sentence_transformers import SentenceTransformer
            model_holder["model"] = SentenceTransformer(model_name)

        embeddings = model_holder["model"].encode(
            sentences=texts,
            batch_size=64,
            normalize_embeddings=True
        )
        return pd.Series([e.tolist() for e in embeddings])

    return embed_udf

def prepare_vector_col(
        df: DataFrame,
        vector_prepare_cols: list[str],
        vector_col_name: str = "document"
):
    """
    Concatenate columns to create a full vector column
    """
    target_df = df.withColumn(vector_col_name, concat_ws(
        " | ",
        *[col(c) for c in vector_prepare_cols]
    ))
    return target_df

def prepare_vector_schema(
        df: DataFrame,
        id_col: str,
        namespace: str,
        vector_prepare_cols: list[str],
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        vector_col_name: str = "document",
        metadata: str = "None"
):
    """
    Prepare DataFrame with the required schema to write to Pinecone
    """
    embed_udf = get_embed_udf(model_name)
    vector_df = prepare_vector_col(df, vector_prepare_cols, vector_col_name)
    target_df = vector_df.withColumn("id", col(id_col).cast(StringType())) \
                    .withColumn("namespace", lit(namespace)) \
                    .withColumn("values", embed_udf(col(vector_col_name))) \
                    .withColumn("metadata", lit(metadata)) \
                    .withColumn("sparse_values",lit(None).cast(SPARSE_SCHEMA)) \
                    .select("id", "namespace", "values", "metadata", "sparse_values")
    return target_df