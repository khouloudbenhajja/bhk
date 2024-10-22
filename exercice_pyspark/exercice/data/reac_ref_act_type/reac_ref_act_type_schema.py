from typing import Tuple

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

N_APPLIC_INFQ: str = "n_applic_infq"
L_ACT_TYPE: str = "l_act_type"
C_ACT_TYPE: str = "c_act_type"
EVENTDATE: str = "eventdate"


REAC_REF_ACT_TYPE_SCHEMA: StructType = StructType(
    [
        StructField(
            N_APPLIC_INFQ, IntegerType(), nullable=True
        ),  # Nombre d'applications inférieures (nullable = true)
        StructField(
            L_ACT_TYPE, StringType(), nullable=True
        ),  # Type d'activité longue (nullable = true)
        StructField(
            C_ACT_TYPE, StringType(), nullable=True
        ),  # Type d'activité courte (nullable = true)
        StructField(
            EVENTDATE, DateType(), nullable=True
        ),  # Date de l'événement (nullable = true)
    ]
)