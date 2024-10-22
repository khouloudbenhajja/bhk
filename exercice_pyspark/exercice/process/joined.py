from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

import sys
import os

os
current_dir = os.path.dirname(os.path.realpath(__file__))
# Add the parent directory of the project to sys.path
parent_dir = os.path.abspath(os.path.join(current_dir, '../..'))
sys.path.insert(0, parent_dir)

from typing import Any
from exercice.config.config import app_config
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from exercice.common.writer import write_to_parquet
from exercice.data.joined.joined_schema import JOINED_SCHEMA
from exercice.data.maag_master_agrem.maag_master_agrem_reader import (
    MAAGMASTERAGREMReader,
)
from exercice.data.maag_raty_linked.maag_raty_linked_reader import (
    MAAGRATYLINKEDReader,
)
from exercice.data.maag_repa_rrol_linked.maag_repa_rrol_linked_reader import (
    MaagRepaRrolLinkedReader,
)
from exercice.data.rtpa_ref_third_party.rtpa_ref_third_party_reader import (
    RtpaRefThirdPartyReader,
)
from exercice.data.reac_ref_act_type.reac_ref_act_type_reader import REACREFACTTYPEReader





class JoinedJob:
    def __init__(
        self,
        maag_master_agrem_input_path: str,
        maag_raty_linked_input_path: str,
        maag_repa_rrol_linked_input_path: str,
        reac_ref_act_type_input_path: str,
        rtpa_ref_third_party_input_path: str,
        joined_output_path: str,
    ) -> None:

        self.maag_master_agrem_input_path: str = maag_master_agrem_input_path
        self.maag_raty_linked_input_path: str = maag_raty_linked_input_path
        self.maag_repa_rrol_linked_input_path: str = maag_repa_rrol_linked_input_path
        self.rtpa_ref_third_party_input_path: str = rtpa_ref_third_party_input_path
        self.reac_ref_act_type_input_path: str = reac_ref_act_type_input_path
        self.joined_output_path: str = joined_output_path

    def run(self) -> None:
        maag_master_agrem_df: DataFrame = self._get_data_from_maag_master_agrem(
            self.maag_master_agrem_input_path
        )
        maag_raty_linked_df: DataFrame = self._get_data_from_maag_raty_linked(
            self.maag_raty_linked_input_path
        )

        rtpa_ref_third_party_df: DataFrame = self._get_data_from_rtpa_ref_third_party(
            self.rtpa_ref_third_party_input_path
        )
        maag_repa_rrol_linked_df: DataFrame = self._get_data_from_maag_repa_rrol_linked(
            self.maag_repa_rrol_linked_input_path
        )
        reac_ref_act_type_df: DataFrame = self._get_data_from_reac_ref_act_type(
            self.reac_ref_act_type_input_path
        )
        dataset_joined: DataFrame = self._create_dataset_joined(
            maag_master_agrem_df,
            maag_raty_linked_df,
            rtpa_ref_third_party_df,
            maag_repa_rrol_linked_df,
            reac_ref_act_type_df
        )

        dataset_joined.show()

        self._write_dataset_joined(dataset_joined, self.joined_output_path)

    def _get_data_from_maag_master_agrem(self, path: str) -> DataFrame:
        maag_master_agrem_reader: MAAGMASTERAGREMReader = MAAGMASTERAGREMReader(
            path
        )
        maag_master_agrem = maag_master_agrem_reader.read()
        # maag_master_agrem.show()
        return maag_master_agrem

    def _get_data_from_maag_raty_linked(self, path: str) -> DataFrame:
        maag_raty_linked_reader: MAAGRATYLINKEDReader = MAAGRATYLINKEDReader(path)
        maag_raty_linked = maag_raty_linked_reader.read()
        # maag_raty_linked.show()
        return maag_raty_linked

    def _get_data_from_rtpa_ref_third_party(self, path: str) -> DataFrame:
        rtpa_ref_third_party_reader: RtpaRefThirdPartyReader = RtpaRefThirdPartyReader(path)
        rtpa_ref_third_party = rtpa_ref_third_party_reader.read()
        # rtpa_ref_third_party.show()
        return rtpa_ref_third_party

    def _get_data_from_maag_repa_rrol_linked(self, path: str) -> DataFrame:
        maag_repa_rrol_linked_reader: MaagRepaRrolLinkedReader = MaagRepaRrolLinkedReader(path)
        maag_repa_rrol_linked = maag_repa_rrol_linked_reader.read()
        # maag_repa_rrol_linked.show()
        return maag_repa_rrol_linked
    def _get_data_from_reac_ref_act_type(self, path: str) -> DataFrame:
        reac_ref_act_type_reader: REACREFACTTYPEReader = REACREFACTTYPEReader(path)
        reac_ref_act_type = reac_ref_act_type_reader.read()
        # reac_ref_act_type.show()
        return reac_ref_act_type

    def _create_dataset_joined(
        self,
        maag_master_agrem_df: DataFrame,
        maag_raty_linked_df: DataFrame,
        rtpa_ref_third_party_df: DataFrame,
        maag_repa_rrol_linked_df: DataFrame,
        reac_ref_act_type_df: DataFrame

    ) -> DataFrame:
        maag_master_agrem_df = maag_master_agrem_df.withColumnRenamed('eventdate', 'eventdate_maag_master_agrem')
        maag_raty_linked_df = maag_raty_linked_df.withColumnRenamed('eventdate', 'eventdate_maag_raty')
        maag_repa_rrol_linked_df = maag_repa_rrol_linked_df.withColumnRenamed('evendate', 'eventdate_maag_repa')
        reac_ref_act_type_df = reac_ref_act_type_df.withColumnRenamed('eventdate', 'eventdate_reac_ref_act_type')
        rtpa_ref_third_party_df = rtpa_ref_third_party_df.withColumnRenamed('eventdate', 'eventdate_rtpa_ref_third_party')

        rtpa_ref_third_party_df = rtpa_ref_third_party_df.withColumnRenamed('c_thir_part_refer', "c_part_refer")
        maag_raty_linked_df = maag_raty_linked_df.withColumnRenamed('n_applic_infq', "n_applic_infq_maag")
        joined_df = maag_master_agrem_df.join(reac_ref_act_type_df, ['c_act_type', 'n_applic_infq'], 'left')
        joined_df = joined_df.join(maag_repa_rrol_linked_df, ['N_APPLIC_INFQ', 'C_MAST_AGREM_REFER'], 'left')
        joined_df = joined_df.join(maag_raty_linked_df, ['c_mast_agrem_refer'], 'left')
        joined_df = joined_df.join(rtpa_ref_third_party_df, ['n_applic_infq', 'c_part_refer'], 'left')

        joined_df = (
            joined_df
            .distinct()
            .select(*JOINED_SCHEMA.fieldNames())
        )
        return joined_df

    def _write_dataset_joined(self, df: DataFrame, output_path: str) -> None:
        write_to_parquet(df, output_path)


def run_job(**kwargs: Any) -> None:
    print(f"Running Job with arguments[{kwargs}]")

    file_path_maag_master_agrem = app_config.get("maag_master_agrem_input_path")
    file_path_maag_raty_linked = app_config.get("maag_raty_linked_input_path")
    file_path_maag_repa_rrol_linked = app_config.get("maag_repa_rrol_linked_input_path")
    file_path_rtpa_ref_third_party = app_config.get("rtpa_ref_third_party_input_path")
    file_path_reac_ref_act_type = app_config.get("reac_ref_act_type_input_path")

    output_file_path_joined = app_config.get("joined_output_path")

    maag_master_agrem_path: str =file_path_maag_master_agrem
    maag_raty_linked_path: str = file_path_maag_raty_linked
    reac_ref_act_type_path: str = file_path_reac_ref_act_type
    maag_repa_rrol_linked_path: str = file_path_maag_repa_rrol_linked
    rtpa_ref_third_party_path: str =file_path_rtpa_ref_third_party

    joined_output_path: str = output_file_path_joined

    job: JoinedJob = JoinedJob(
        maag_master_agrem_path,
        maag_raty_linked_path,
        maag_repa_rrol_linked_path,
        reac_ref_act_type_path,
        rtpa_ref_third_party_path,
        joined_output_path,
    )
    job.run()


"""
job = JoinedJob(
    "maag_master_agrem_input_path",
    "maag_raty_linked_input_path",
    "maag_repa_rrol_linked_input_path",
    "reac_ref_act_type_input_path",
    "rtpa_ref_third_party_input_path",
    "joined_output_path",
)
"""

run_job()