from datetime import datetime
from typing import Tuple

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# fipl_financial_plan fields

N_APPLIC_INFQ: str = "n_applic_infq"
C_MAST_AGREM_REFER: str = "c_mast_agrem_refer"
C_M: str = "c_m"
M_ORIG: str = "m_orig"
M_CONVT_EURO: str = "m_convt_euro"
M_ORIG_SHAR: str = "m_orig_shar"
M_CONVT_EURO_SHAR: str = "m_convt_euro_shar"
EVENTDATE: str = "eventdate"


MAAG_RATY_LINKED_SCHEMA: StructType = StructType(
    [
        StructField(
            N_APPLIC_INFQ, IntegerType(), nullable=True
        ),  # Nombre d'applications inférieures
        StructField(
            C_MAST_AGREM_REFER, StringType(), nullable=True
        ),  # Référence de l'accord principal
        StructField(C_M, StringType(), nullable=True),  # Champ C_M
        StructField(
            M_ORIG, DecimalType(15, 2), nullable=True
        ),  # Valeur originale (décimal 15,2)
        StructField(
            M_CONVT_EURO, DecimalType(15, 2), nullable=True
        ),  # Conversion en euros (décimal 15,2)
        StructField(
            M_ORIG_SHAR, DecimalType(15, 2), nullable=True
        ),  # Part originale (décimal 15,2)
        StructField(
            M_CONVT_EURO_SHAR, DecimalType(15, 2), nullable=True
        ),  # Part convertie en euros (décimal 15,2)
        StructField(EVENTDATE, DateType(), nullable=True),  # Date de l'événement
    ]
)