from enum import Enum

from pyspark.sql import DataFrame


class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


def write_to_parquet(df: DataFrame, output_file_path: str) -> str:
    df.write.mode(WriteMode.OVERWRITE.value).parquet(output_file_path)
    return output_file_path
