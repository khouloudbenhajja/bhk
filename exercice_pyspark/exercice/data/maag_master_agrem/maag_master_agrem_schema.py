from datetime import datetime
from typing import Tuple

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# fiag_fipl_attached fields

N_APPLIC_INFQ: str = "n_applic_infq"
C_ACT_TYPE: str = "c_act_type"
C_MAST_AGREM_REFER: str = "c_mast_agrem_refer"
C_ACT_MNG_STG: str = "c_act_mng_stg"
D_ACT_MNG_STG: str = "d_act_mng_stg"
C_ACT_MNG_STEP: str = "c_act_mng_step"
D_ACT_MNG_STEP: str = "d_act_mng_step"
D_PLAN_INIT_END: str = "d_plan_init_end"
D_PLAN_ACTUA_END: str = "d_plan_actua_end"
C_IDENT_LOCA: str = "c_ident_loca"
C_PRNT_MAST_AGREM_REFER: str = "c_prnt_mast_agrem_refer"
L_MAST_AGREM_NAME: str = "l_mast_agrem_name"
N_MAST_AGREM_VALI_PER: str = "n_mast_agrem_vali_per"
D_CRDT_COMMITTEE_APRV: str = "d_crdt_committee_aprv"
D_NOTIF: str = "d_notif"
C_CRCY: str = "c_crcy"
C_FUND_CHANL: str = "c_fund_chanl"
B_POOL: str = "b_pool"
CENMES: str = "cenmes"
CNUPRJ: str = "cnuprj"
NSSSDC: str = "nsssdc"
EVENTDATE: str = "eventdate"


MAAG_MASTER_AGREM_SCHEMA: StructType = StructType(
    [
        StructField(N_APPLIC_INFQ, StringType()),
        StructField(C_ACT_TYPE, StringType()),
        StructField(C_MAST_AGREM_REFER, StringType()),
        StructField(C_ACT_MNG_STG, StringType()),
        StructField(D_ACT_MNG_STG, DateType()),
        StructField(C_ACT_MNG_STEP, StringType()),
        StructField(D_ACT_MNG_STEP, DateType()),
        StructField(D_PLAN_INIT_END, DateType()),
        StructField(D_PLAN_ACTUA_END, DateType()),
        StructField(C_IDENT_LOCA, StringType()),
        StructField(C_PRNT_MAST_AGREM_REFER, StringType()),
        StructField(L_MAST_AGREM_NAME, StringType()),
        StructField(N_MAST_AGREM_VALI_PER, IntegerType()),
        StructField(D_CRDT_COMMITTEE_APRV, DateType()),
        StructField(D_NOTIF, DateType()),
        StructField(C_CRCY, StringType()),
        StructField(C_FUND_CHANL, StringType()),
        StructField(B_POOL, BooleanType()),
        StructField(CENMES, IntegerType()),
        StructField(CNUPRJ, StringType()),
        StructField(NSSSDC, IntegerType()),
        StructField(EVENTDATE, DateType()),
    ]
)