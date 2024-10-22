from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


N_APPLIC_INFQ: str = "n_applic_infq"
C_PART_REFER: str = "c_part_refer"
C_MAST_AGREM_REFER: str = "c_mast_agrem_refer"
C_ACT_TYPE: str = "c_act_type"
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
EVENTDATE_MAAG_MASTER_AGREM: str = "eventdate_maag_master_agrem"
L_ACT_TYPE: str = "l_act_type"
EVENTDATE_REAC_REF_ACT_TYPE: str = "eventdate_reac_ref_act_type"
C_ROLE: str = "c_role"
D_STR_ACTR_AGREM: str = "d_str_actr_agrem"
D_END_ACTR_AGREM: str = "d_end_actr_agrem"
EVENTDATE: str = "eventdate"
N_APPLIC_INFQ_MAAG: str = "n_applic_infq_maag"
C_M: str = "c_m"
M_ORIG: str = "m_orig"
M_CONVT_EURO: str = "m_convt_euro"
M_ORIG_SHAR: str = "m_orig_shar"
M_CONVT_EURO_SHAR: str = "m_convt_euro_shar"
EVENTDATE_MAAG_RATY: str = "eventdate_maag_raty"
L_THIR_PART_NAME: str = "l_thir_part_name"
N_IDENT_RET: str = "n_ident_ret"
NUMERA: str = "numera"
NSIREN: str = "nsiren"
EVENTDATE_RTPA_REF_THIRD_PARTY: str = "eventdate_rtpa_ref_third_party"
SOURCE: str = "source"

JOINED_SCHEMA: StructType = StructType(
    [
        StructField(N_APPLIC_INFQ, IntegerType(), nullable=True),
        StructField(C_PART_REFER, StringType(), nullable=True),
        StructField(C_MAST_AGREM_REFER, StringType(), nullable=True),
        StructField(C_ACT_TYPE, StringType(), nullable=True),
        StructField(C_ACT_MNG_STG, StringType(), nullable=True),
        StructField(D_ACT_MNG_STG, DateType(), nullable=True),
        StructField(C_ACT_MNG_STEP, StringType(), nullable=True),
        StructField(D_ACT_MNG_STEP, DateType(), nullable=True),
        StructField(D_PLAN_INIT_END, DateType(), nullable=True),
        StructField(D_PLAN_ACTUA_END, DateType(), nullable=True),
        StructField(C_IDENT_LOCA, StringType(), nullable=True),
        StructField(C_PRNT_MAST_AGREM_REFER, IntegerType(), nullable=True),
        StructField(L_MAST_AGREM_NAME, StringType(), nullable=True),
        StructField(N_MAST_AGREM_VALI_PER, IntegerType(), nullable=True),
        StructField(D_CRDT_COMMITTEE_APRV, DateType(), nullable=True),
        StructField(D_NOTIF, DateType(), nullable=True),
        StructField(C_CRCY, StringType(), nullable=True),
        StructField(C_FUND_CHANL, StringType(), nullable=True),
        StructField(B_POOL, BooleanType(), nullable=True),
        StructField(CENMES, IntegerType(), nullable=True),
        StructField(CNUPRJ, StringType(), nullable=True),
        StructField(NSSSDC, IntegerType(), nullable=True),
        StructField(EVENTDATE_MAAG_MASTER_AGREM, DateType(), nullable=True),
        StructField(L_ACT_TYPE, StringType(), nullable=True),
        StructField(EVENTDATE_REAC_REF_ACT_TYPE, DateType(), nullable=True),
        StructField(C_ROLE, StringType(), nullable=True),
        StructField(D_STR_ACTR_AGREM, DateType(), nullable=True),
        StructField(D_END_ACTR_AGREM, DateType(), nullable=True),
        StructField(EVENTDATE, DateType(), nullable=True),
        StructField(N_APPLIC_INFQ_MAAG, IntegerType(), nullable=True),
        StructField(C_M, StringType(), nullable=True),
        StructField(M_ORIG, DecimalType(15, 2), nullable=True),
        StructField(M_CONVT_EURO, DecimalType(15, 2), nullable=True),
        StructField(M_ORIG_SHAR, DecimalType(15, 2), nullable=True),
        StructField(M_CONVT_EURO_SHAR, DecimalType(15, 2), nullable=True),
        StructField(EVENTDATE_MAAG_RATY, DateType(), nullable=True),
        StructField(L_THIR_PART_NAME, StringType(), nullable=True),
        StructField(N_IDENT_RET, LongType(), nullable=True),
        StructField(NUMERA, IntegerType(), nullable=True),
        StructField(NSIREN, StringType(), nullable=True),
        StructField(EVENTDATE_RTPA_REF_THIRD_PARTY, DateType(), nullable=True),
        StructField(SOURCE, StringType(), nullable=True),
    ]
)