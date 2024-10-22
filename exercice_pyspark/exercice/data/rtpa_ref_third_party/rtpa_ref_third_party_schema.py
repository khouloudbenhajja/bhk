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
C_THIR_PART_REFER: str = "c_thir_part_refer"
L_THIR_PART_NAME: str = "l_thir_part_name"
N_IDENT_RET: str = "n_ident_ret"
NUMERA: str = "numera"
NSIREN: str = "nsiren"
EVENTDATE: str = "eventdate"
SOURCE: str = "source"

RTPA_REF_THIRD_PARTY_SCHEMA: StructType = StructType(
    [
        StructField("N_APPLIC_INFQ", IntegerType()),
        StructField("C_THIR_PART_REFER", StringType()),
        StructField("L_THIR_PART_NAME", StringType()),
        StructField("N_IDENT_RET", LongType()),
        StructField("NUMERA", IntegerType()),
        StructField("NSIREN", StringType()),
        StructField("EVENTDATE", DateType()),
        StructField("SOURCE", StringType()),
    ]
)