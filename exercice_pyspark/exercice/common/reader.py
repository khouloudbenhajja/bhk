
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from exercice.context.context import spark

def read_from_parquet(parquet_file_path: str) -> DataFrame:
    return spark.read.parquet(parquet_file_path)


def create_df_with_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Create a dataframe and enforce the given schema."""
    return spark.createDataFrame(
        schema=schema, data=spark.sparkContext.emptyRDD()
    ).union(df)
