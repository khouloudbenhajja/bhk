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
from exercice.data.rtpa_ref_third_party.rtpa_ref_third_party_schema import (
    RTPA_REF_THIRD_PARTY_SCHEMA,
)
from pyspark.sql import DataFrame


class RtpaRefThirdPartyReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        rtpa_ref_third_party_df: DataFrame = read_from_parquet(self.path).select(
            *RTPA_REF_THIRD_PARTY_SCHEMA.fieldNames()
        )

        return create_df_with_schema(
            rtpa_ref_third_party_df,
            RTPA_REF_THIRD_PARTY_SCHEMA,
        )


if __name__ == "__main__":
    reader = RtpaRefThirdPartyReader(app_config.get("rtpa_ref_third_party_input_path"))

    df = reader.read()

    df.show()