from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

N_APPLIC_INFQ: str = "n_applic_infq"
C_MAST_AGREM_REFER: str = "c_mast_agrem_refer"
C_PART_REFER: str = "c_part_refer"
C_ROLE: str = "c_role"
D_STR_ACTR_AGREM: str = "d_str_actr_agrem"
D_END_ACTR_AGREM: str = "d_end_actr_agrem"
EVENTDATE: str = "eventdate"


MAAG_REPA_RROL_LINKED_SCHEMA: StructType = StructType(
    [
        StructField("N_APPLIC_INFQ", IntegerType()),
        StructField("C_MAST_AGREM_REFER", StringType()),
        StructField("C_PART_REFER", StringType()),
        StructField("C_ROLE", StringType()),
        StructField("D_STR_ACTR_AGREM", DateType()),
        StructField("D_END_ACTR_AGREM", DateType()),
        StructField("EVENTDATE", DateType()),
    ]
)