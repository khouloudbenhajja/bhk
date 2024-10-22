import os
import sys

# Get the directory of the current script
os
current_dir = os.path.dirname(os.path.realpath(__file__))
# Add the parent directory of the project to sys.path
parent_dir = os.path.abspath(os.path.join(current_dir, "../../.."))
sys.path.insert(0, parent_dir)

from exercice.common.reader import create_df_with_schema, read_from_parquet
from exercice.config.config import app_config
from exercice.context.context import logger
from exercice.data.maag_master_agrem.maag_master_agrem_schema import (
    MAAG_MASTER_AGREM_SCHEMA,
)
from pyspark.sql import DataFrame


class MAAGMASTERAGREMReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        maag_master_agrem_df: DataFrame = read_from_parquet(self.path).select(
            *MAAG_MASTER_AGREM_SCHEMA.fieldNames()
        )

        return create_df_with_schema(
            maag_master_agrem_df,
            MAAG_MASTER_AGREM_SCHEMA,
        )


if __name__ == "__main__":
    reader = MAAGMASTERAGREMReader(app_config.get("maag_master_agrem_input_path"))

    df = reader.read()

    df.show()